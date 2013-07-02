package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.salesforce.hbase.index.builder.ColumnFamilyIndexer;
import com.salesforce.hbase.index.builder.covered.CoveredColumnIndexer;

/**
 * most of the underlying work (creating/splitting the WAL, etc) is from
 * org.apache.hadoop.hhbase.regionserver.wal.TestWALReplay, copied here for completeness and ease of
 * use
 */
public class TestWALReplayWithIndexWrites {

  public static final Log LOG = LogFactory.getLog(TestWALReplay.class);
  static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final String INDEX_TABLE_NAME = "IndexTable";
  private Path hbaseRootDir = null;
  private Path oldLogDir;
  private Path logDir;
  private FileSystem fs;
  private Configuration conf;

  protected static void configureCluster() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    // make sure writers fail quickly
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    conf.setInt(HConstants.HBASE_CLIENT_PAUSE, 1000);
    conf.setInt("zookeeper.recovery.retry", 3);
    conf.setInt("zookeeper.recovery.retry.intervalmill", 100);
    conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 30000);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 5000);

    // enable appends
    conf.setBoolean("dfs.support.append", true);
  }

  protected static void startCluster() throws Exception {
    UTIL.startMiniDFSCluster(3);
    UTIL.startMiniZKCluster();
    UTIL.startMiniHBaseCluster(1, 1);

    Path hbaseRootDir = UTIL.getDFSCluster().getFileSystem().makeQualified(new Path("/hbase"));
    LOG.info("hbase.rootdir=" + hbaseRootDir);
    UTIL.getConfiguration().set(HConstants.HBASE_DIR, hbaseRootDir.toString());
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    configureCluster();
    // use our custom WAL Reader
    UTIL.getConfiguration().set("hbase.regionserver.hlog.reader.impl", IndexedHLogReader.class.getName());
    startCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniHBaseCluster();
    UTIL.shutdownMiniDFSCluster();
    UTIL.shutdownMiniZKCluster();
  }

  @Before
  public void setUp() throws Exception {
    this.conf = HBaseConfiguration.create(UTIL.getConfiguration());
    this.fs = UTIL.getDFSCluster().getFileSystem();
    this.hbaseRootDir = new Path(this.conf.get(HConstants.HBASE_DIR));
    this.oldLogDir = new Path(this.hbaseRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    this.logDir = new Path(this.hbaseRootDir, HConstants.HREGION_LOGDIR_NAME);
  }

  @After
  public void tearDown() throws Exception {
  }

  private void deleteDir(final Path p) throws IOException {
    if (this.fs.exists(p)) {
      if (!this.fs.delete(p, true)) {
        throw new IOException("Failed remove of " + p);
      }
    }
  }

  /**
   * Test writing edits into an HRegion, closing it, splitting logs, opening Region again. Verify
   * seqids.
   * @throws Exception on failure
   */
  @Test
  public void testReplayEditsWrittenViaHRegion() throws Exception {
    final String tableNameStr = "testReplayEditsWrittenViaHRegion";
    final HRegionInfo hri = new HRegionInfo(Bytes.toBytes(tableNameStr), null, null, false);
    final Path basedir = new Path(this.hbaseRootDir, tableNameStr);
    deleteDir(basedir);
    final HTableDescriptor htd = createBasic3FamilyHTD(tableNameStr);
    
    //setup basic indexing for the table
    Map<byte[], String> familyMap = new HashMap<byte[], String>();
    byte[] indexedFamily = new byte[] {'a'};
    familyMap.put(indexedFamily, INDEX_TABLE_NAME);
    ColumnFamilyIndexer.enableIndexing(htd, familyMap);

    // create the region + its WAL
    HRegion region0 = HRegion.createHRegion(hri, hbaseRootDir, this.conf, htd);
    region0.close();
    region0.getLog().closeAndDelete();
    HLog wal = createWAL(this.conf);
    RegionServerServices mockRS = Mockito.mock(RegionServerServices.class);
    // mock out some of the internals of the RSS, so we can run CPs
    Mockito.when(mockRS.getWAL()).thenReturn(wal);
    RegionServerAccounting rsa = Mockito.mock(RegionServerAccounting.class);
    Mockito.when(mockRS.getRegionServerAccounting()).thenReturn(rsa);
    HRegion region = new HRegion(basedir, wal, this.fs, this.conf, hri, htd, mockRS);
    long seqid = region.initialize();
    // HRegionServer usually does this. It knows the largest seqid across all regions.
    wal.setSequenceNumber(seqid);
    
    //make an attempted write to the primary that should also be indexed
    byte[] rowkey = Bytes.toBytes("indexed_row_key");
    Put p = new Put(rowkey);
    p.add(indexedFamily, Bytes.toBytes("qual"), Bytes.toBytes("value"));
    region.put(p);

    // we should then see the server go down
    Mockito.verify(mockRS, Mockito.times(1)).abort(Mockito.anyString(),
      Mockito.any(Exception.class));
    region.close(true);
    wal.close();

    // then create the index table so we are successful on WAL replay
    CoveredColumnIndexer.createIndexTable(UTIL.getHBaseAdmin(), INDEX_TABLE_NAME);

    // run the WAL split and setup the region
    runWALSplit(this.conf);
    HLog wal2 = createWAL(this.conf);
    HRegion region1 = new HRegion(basedir, wal2, this.fs, this.conf, hri, htd, mockRS);

    // initialize the region - this should replay the WALEdits from the WAL
    region1.initialize();

    // now check to ensure that we wrote to the index table
    HTable index = new HTable(UTIL.getConfiguration(), INDEX_TABLE_NAME);
    int indexSize = getKeyValueCount(index);
    assertEquals("Index wasn't propertly updated from WAL replay!", 1, indexSize);
    Get g = new Get(rowkey);
    final Result result = region1.get(g);
    assertEquals("Primary region wasn't updated from WAL replay!", 1, result.size());

    // cleanup the index table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.disableTable(INDEX_TABLE_NAME);
    admin.deleteTable(INDEX_TABLE_NAME);
    admin.close();
  }

  /**
   * Create simple HTD with three families: 'a', 'b', and 'c'
   * @param tableName name of the table descriptor
   * @return
   */
  private HTableDescriptor createBasic3FamilyHTD(final String tableName) {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor a = new HColumnDescriptor(Bytes.toBytes("a"));
    htd.addFamily(a);
    HColumnDescriptor b = new HColumnDescriptor(Bytes.toBytes("b"));
    htd.addFamily(b);
    HColumnDescriptor c = new HColumnDescriptor(Bytes.toBytes("c"));
    htd.addFamily(c);
    return htd;
  }

  /*
   * @param c
   * @return WAL with retries set down from 5 to 1 only.
   * @throws IOException
   */
  private HLog createWAL(final Configuration c) throws IOException {
    HLog wal = new HLog(FileSystem.get(c), logDir, oldLogDir, c);
    // Set down maximum recovery so we dfsclient doesn't linger retrying something
    // long gone.
    HBaseTestingUtility.setMaxRecoveryErrorCount(wal.getOutputStream(), 1);
    return wal;
  }

  /*
   * Run the split. Verify only single split file made.
   * @param c
   * @return The single split file made
   * @throws IOException
   */
  private Path runWALSplit(final Configuration c) throws IOException {
    FileSystem fs = FileSystem.get(c);
    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(c, this.hbaseRootDir, this.logDir,
      this.oldLogDir, fs);
    List<Path> splits = logSplitter.splitLog();
    // Split should generate only 1 file since there's only 1 region
    assertEquals("splits=" + splits, 1, splits.size());
    // Make sure the file exists
    assertTrue(fs.exists(splits.get(0)));
    LOG.info("Split file=" + splits.get(0));
    return splits.get(0);
  }

  private int getKeyValueCount(HTable table) throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions(Integer.MAX_VALUE - 1);

    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      count += res.list().size();
      System.out.println(count + ") " + res);
    }
    results.close();

    return count;
  }
}