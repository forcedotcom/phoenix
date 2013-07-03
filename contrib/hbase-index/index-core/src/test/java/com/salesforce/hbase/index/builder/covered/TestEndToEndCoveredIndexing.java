package com.salesforce.hbase.index.builder.covered;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test Covered Column indexing in an 'end-to-end' manner on a minicluster. This covers cases where
 * we manage custom timestamped updates that arrive in and out of order as well as just using the
 * generically timestamped updates.
 */
public class TestEndToEndCoveredIndexing {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final String FAM_STRING = "FAMILY";
  private static final byte[] FAM = Bytes.toBytes(FAM_STRING);
  private static final String FAM2_STRING = "FAMILY2";
  private static final byte[] FAM2 = Bytes.toBytes(FAM2_STRING);
  private static final String INDEX_TABLE = "INDEX_TABLE";
  private static final String INDEX_TABLE2 = "INDEX_TABLE2";
  private static final byte[] EMPTY_BYTES = new byte[0];
  private static final byte[] indexed_qualifer = Bytes.toBytes("indexed_qual");
  private static final byte[] regular_qualifer = Bytes.toBytes("reg_qual");
  private static final byte[] row1 = Bytes.toBytes("row1");
  private static final byte[] value1 = Bytes.toBytes("val1");
  private static final byte[] value2 = Bytes.toBytes("val2");
  private static final byte[] value3 = Bytes.toBytes("val3");

  // setup a couple of index columns
  private static final ColumnGroup fam1 = new ColumnGroup(INDEX_TABLE);
  private static final ColumnGroup fam2 = new ColumnGroup(INDEX_TABLE2);
  // match a single family:qualifier pair
  private static final CoveredColumn col1 = new CoveredColumn(FAM_STRING, indexed_qualifer);
  // matches the family2:* columns
  private static final CoveredColumn col2 = new CoveredColumn(FAM2_STRING, null);
  private static final CoveredColumn col3 = new CoveredColumn(FAM2_STRING, indexed_qualifer);
  static {
    // values are [col1][col2_1]...[col2_n]
    fam1.add(col1);
    fam1.add(col2);
    // value is [col2]
    fam2.add(col3);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  /**
   * Test that a bunch of puts with a single timestamp across all the puts builds and inserts index
   * entries as expected
   * @throws Exception on failure
   */
  @Test
  public void testSimpleTimestampedUpdates() throws Exception {
    //setup the index 
    CoveredColumnIndexSpecifierBuilder builder = new CoveredColumnIndexSpecifierBuilder();
    builder.addIndexGroup(fam1);

    // setup the primary table
    String indexedTableName = "testSimpleTimestampedUpdates";
    HTableDescriptor pTable = new HTableDescriptor(indexedTableName);
    pTable.addFamily(new HColumnDescriptor(FAM));
    pTable.addFamily(new HColumnDescriptor(FAM2));
    builder.build(pTable);

    // create the primary table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.createTable(pTable);
    HTable primary = new HTable(UTIL.getConfiguration(), indexedTableName);

    // create the index tables
    CoveredColumnIndexer.createIndexTable(admin, INDEX_TABLE);

    // do a put to the primary table
    Put p = new Put(row1);
    long ts = 10;
    p.add(FAM, indexed_qualifer, ts, value1);
    p.add(FAM, regular_qualifer, ts, value2);
    primary.put(p);
    primary.flushCommits();

    // read the index for the expected values
    HTable index1 = new HTable(UTIL.getConfiguration(), INDEX_TABLE);

    // build the expected kvs
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));
    List<KeyValue> expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts, pairs);

    // verify that the index matches
    verifyIndexTableAtTimestamp(index1, expected, ts, value1);

    // cleanup
    closeAndCleanupTables(primary, index1);
  }

  /**
   * Test that the multiple timestamps in a single put build the correct index updates.
   * @throws Exception on failure
   */
  @Test
  public void testMultipleTimestampsInSinglePut() throws Exception {
    // setup the index
    CoveredColumnIndexSpecifierBuilder builder = new CoveredColumnIndexSpecifierBuilder();
    builder.addIndexGroup(fam1);

    // setup the primary table
    String indexedTableName = "testMultipleTimestampsInSinglePut";
    HTableDescriptor pTable = new HTableDescriptor(indexedTableName);
    pTable.addFamily(new HColumnDescriptor(FAM));
    pTable.addFamily(new HColumnDescriptor(FAM2));
    builder.build(pTable);

    // create the primary table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.createTable(pTable);
    HTable primary = new HTable(UTIL.getConfiguration(), indexedTableName);

    // create the index tables
    CoveredColumnIndexer.createIndexTable(admin, INDEX_TABLE);

    // do a put to the primary table
    Put p = new Put(row1);
    long ts1 = 10;
    long ts2 = 11;
    p.add(FAM, indexed_qualifer, ts1, value1);
    p.add(FAM, regular_qualifer, ts1, value2);
    // our group indexes all columns in the this family, so any qualifier here is ok
    p.add(FAM2, regular_qualifer, ts2, value3);
    primary.put(p);
    primary.flushCommits();

    // read the index for the expected values
    HTable index1 = new HTable(UTIL.getConfiguration(), INDEX_TABLE);

    // build the expected kvs
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));

    // check the first entry at ts1
    List<KeyValue> expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts1, pairs);
    verifyIndexTableAtTimestamp(index1, expected, ts1, value1);

    // check the second entry at ts2
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col2));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts2, pairs);
    verifyIndexTableAtTimestamp(index1, expected, ts2, value1);

    // cleanup
    closeAndCleanupTables(primary, index1);
  }

  /**
   * Test that we make updates to multiple {@link ColumnGroup}s across a single put/delete 
   * @throws Exception on failure
   */
  @Test
  public void testMultipleConcurrentGroupsUpdated() throws Exception {
    // setup the index
    CoveredColumnIndexSpecifierBuilder builder = new CoveredColumnIndexSpecifierBuilder();
    builder.addIndexGroup(fam1);
    builder.addIndexGroup(fam2);

    // setup the primary table
    String indexedTableName = "testMultipleConcurrentGroupsUpdated";
    HTableDescriptor pTable = new HTableDescriptor(indexedTableName);
    pTable.addFamily(new HColumnDescriptor(FAM));
    pTable.addFamily(new HColumnDescriptor(FAM2));
    builder.build(pTable);

    // create the primary table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.createTable(pTable);
    HTable primary = new HTable(UTIL.getConfiguration(), indexedTableName);

    // create the index tables
    CoveredColumnIndexer.createIndexTable(admin, INDEX_TABLE);
    CoveredColumnIndexer.createIndexTable(admin, INDEX_TABLE2);

    // do a put to the primary table
    Put p = new Put(row1);
    long ts = 10;
    p.add(FAM, indexed_qualifer, ts, value1);
    p.add(FAM, regular_qualifer, ts, value2);
    p.add(FAM2, indexed_qualifer, ts, value3);
    primary.put(p);
    primary.flushCommits();

    // read the index for the expected values
    HTable index1 = new HTable(UTIL.getConfiguration(), INDEX_TABLE);
    HTable index2 = new HTable(UTIL.getConfiguration(), INDEX_TABLE2);

    // build the expected kvs
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col2));
    List<KeyValue> expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts, pairs);
    verifyIndexTableAtTimestamp(index1, expected, ts, value1);

    // and check the second index as well
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col3));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts, pairs);
    verifyIndexTableAtTimestamp(index2, expected, ts, value3);

    // cleanup
    closeAndCleanupTables(primary, index1, index2);
  }

  /**
   * HBase has a 'fun' property wherein you can completely clobber an existing row if you make a
   * {@link Put} at the exact same dimension (row, cf, cq, ts) as an existing row. The old row
   * disappears and the new value (since the rest of the row is the same) completely subsumes it.
   * This test ensures that we remove the old entry and put a new entry in its place.
   * @throws Exception on failure
   */
  @Test
  public void testOverwritingPutsCorrectlyGetIndexed() throws Exception {
    // setup the index
    CoveredColumnIndexSpecifierBuilder builder = new CoveredColumnIndexSpecifierBuilder();
    builder.addIndexGroup(fam1);

    // setup the primary table
    String indexedTableName = "testMultipleTimestampsInSinglePut";
    HTableDescriptor pTable = new HTableDescriptor(indexedTableName);
    pTable.addFamily(new HColumnDescriptor(FAM));
    pTable.addFamily(new HColumnDescriptor(FAM2));
    builder.build(pTable);

    // create the primary table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.createTable(pTable);
    HTable primary = new HTable(UTIL.getConfiguration(), indexedTableName);

    // create the index tables
    CoveredColumnIndexer.createIndexTable(admin, INDEX_TABLE);

    // do a put to the primary table
    Put p = new Put(row1);
    long ts = 10;
    p.add(FAM, indexed_qualifer, ts, value1);
    p.add(FAM, regular_qualifer, ts, value2);
    primary.put(p);
    primary.flushCommits();

    // read the index for the expected values
    HTable index1 = new HTable(UTIL.getConfiguration(), INDEX_TABLE);

    // build the expected kvs
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));

    // check the first entry at ts
    List<KeyValue> expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts, pairs);
    verifyIndexTableAtTimestamp(index1, expected, ts, value1);

    // now overwrite the put in the primary table with a new value
    p = new Put(row1);
    p.add(FAM, indexed_qualifer, ts, value3);
    primary.put(p);
    primary.flushCommits();

    pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));

    // check the first entry at ts
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts, pairs);
    verifyIndexTableAtTimestamp(index1, expected, ts, value3);
    // and verify that a scan at the first entry returns nothing (ignore the updated row)
    verifyIndexTableAtTimestamp(index1, Collections.<KeyValue> emptyList(), ts, value1, value2);

    // cleanup
    closeAndCleanupTables(primary, index1);
  }
  
  @Test
  public void testSimpleDeletes() throws Exception {

    // setup the index
    CoveredColumnIndexSpecifierBuilder builder = new CoveredColumnIndexSpecifierBuilder();
    builder.addIndexGroup(fam1);

    // setup the primary table
    String indexedTableName = "testSimpleDelete";
    HTableDescriptor pTable = new HTableDescriptor(indexedTableName);
    pTable.addFamily(new HColumnDescriptor(FAM));
    pTable.addFamily(new HColumnDescriptor(FAM2));
    builder.build(pTable);

    // create the primary table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.createTable(pTable);
    HTable primary = new HTable(UTIL.getConfiguration(), indexedTableName);

    // create the index tables
    CoveredColumnIndexer.createIndexTable(admin, INDEX_TABLE);

    // do a simple Put
    long ts = 10;
    Put p = new Put(row1);
    p.add(FAM, indexed_qualifer, ts, value1);
    p.add(FAM, regular_qualifer, ts, value2);
    primary.put(p);
    primary.flushCommits();

    Delete d = new Delete(row1);
    primary.delete(d);

    HTable index = new HTable(UTIL.getConfiguration(), INDEX_TABLE);
    List<KeyValue> expected = Collections.<KeyValue> emptyList();
    // scan over all time should cause the delete to be covered
    verifyIndexTableAtTimestamp(index, expected, 0, Long.MAX_VALUE, value1,
      HConstants.EMPTY_END_ROW);

    // scan at the older timestamp should still show the older value
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts, pairs);
    verifyIndexTableAtTimestamp(index, expected, ts, value1);

    // cleanup
    closeAndCleanupTables(index, primary);
  }

  /**
   * If we don't have any updates to make to the index, we don't take a lock on the WAL. However, we
   * need to make sure that we don't try to unlock the WAL on write time when we don't write
   * anything, since that will cause an java.lang.IllegalMonitorStateException
   * @throws Exception on failure
   */
  @Test
  public void testDeletesWithoutPreviousState() throws Exception {
    // setup the index
    CoveredColumnIndexSpecifierBuilder builder = new CoveredColumnIndexSpecifierBuilder();
    builder.addIndexGroup(fam1);

    // setup the primary table
    String indexedTableName = "testSimpleDelete";
    HTableDescriptor pTable = new HTableDescriptor(indexedTableName);
    pTable.addFamily(new HColumnDescriptor(FAM));
    pTable.addFamily(new HColumnDescriptor(FAM2));
    builder.build(pTable);

    // create the primary table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.createTable(pTable);
    HTable primary = new HTable(UTIL.getConfiguration(), indexedTableName);

    // create the index tables
    CoveredColumnIndexer.createIndexTable(admin, INDEX_TABLE);

    // do a delete on the primary table (no data, so no index updates...hopefully).
    long ts = 10;
    Delete d = new Delete(row1);
    primary.delete(d);

    HTable index1 = new HTable(UTIL.getConfiguration(), INDEX_TABLE);
    List<KeyValue> expected = Collections.<KeyValue> emptyList();
    verifyIndexTableAtTimestamp(index1, expected, ts, value1);

    // a delete of a specific family/column should also not show any index updates
    d = new Delete(row1);
    d.deleteColumn(FAM, indexed_qualifer);
    primary.delete(d);
    verifyIndexTableAtTimestamp(index1, expected, ts, value1);

    // also just a family marker should have the same effect
    d = new Delete(row1);
    d.deleteFamily(FAM);
    primary.delete(d);
    verifyIndexTableAtTimestamp(index1, expected, ts, value1);

    // also just a family marker should have the same effect
    d = new Delete(row1);
    d.deleteColumns(FAM, indexed_qualifer);
    primary.delete(d);
    primary.flushCommits();
    verifyIndexTableAtTimestamp(index1, expected, ts, value1);

    // cleanup
    closeAndCleanupTables(primary, index1);
  }

  /**
   * Similar to the {@link #testMultipleTimestampsInSinglePut()}, this check the same with deletes
   * @throws Exception on failure
   */
  @Test
  public void testMultipleTimestampsInSingleDelete() throws Exception {
    // setup the index
    CoveredColumnIndexSpecifierBuilder builder = new CoveredColumnIndexSpecifierBuilder();
    builder.addIndexGroup(fam1);

    // setup the primary table
    String indexedTableName = "testMultipleTimestampsInSinglePut";
    HTableDescriptor pTable = new HTableDescriptor(indexedTableName);
    pTable.addFamily(new HColumnDescriptor(FAM));
    pTable.addFamily(new HColumnDescriptor(FAM2));
    builder.build(pTable);

    // create the primary table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.createTable(pTable);
    HTable primary = new HTable(UTIL.getConfiguration(), indexedTableName);

    // create the index tables
    CoveredColumnIndexer.createIndexTable(admin, INDEX_TABLE);

    // do a put to the primary table
    Put p = new Put(row1);
    long ts1 = 10, ts2 = 11, ts3 = 12;
    p.add(FAM, indexed_qualifer, ts1, value1);
    p.add(FAM, regular_qualifer, ts1, value2);
    // our group indexes all columns in the this family, so any qualifier here is ok
    p.add(FAM2, regular_qualifer, ts2, value3);
    primary.put(p);
    primary.flushCommits();

    // now build up a delete with a couple different timestamps
    Delete d = new Delete(row1);
    d.deleteColumn(FAM, indexed_qualifer, ts2);
    d.deleteColumn(FAM2, regular_qualifer, ts3);
    primary.delete(d);

    // read the index for the expected values
    HTable index1 = new HTable(UTIL.getConfiguration(), INDEX_TABLE);

    // build the expected kvs
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));

    // check the first entry at ts1
    List<KeyValue> expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts1, pairs);
    verifyIndexTableAtTimestamp(index1, expected, ts1, value1);

    // delete at ts2 changes what the put would insert
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col2));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts2, pairs);
    verifyIndexTableAtTimestamp(index1, expected, ts2, value1);

    // final delete clears out everything
    expected = Collections.emptyList();
    verifyIndexTableAtTimestamp(index1, expected, ts3, value1);

    // cleanup
    closeAndCleanupTables(primary, index1);
  }

  private void verifyIndexTableAtTimestamp(HTable index1, List<KeyValue> expected, long ts,
      byte[] startKey) throws IOException {
    verifyIndexTableAtTimestamp(index1, expected, ts, startKey, HConstants.EMPTY_END_ROW);
  }

  private void verifyIndexTableAtTimestamp(HTable index1, List<KeyValue> expected, long start,
      byte[] startKey, byte[] endKey) throws IOException {
    verifyIndexTableAtTimestamp(index1, expected, start, start + 1, startKey, endKey);
  }

  private void verifyIndexTableAtTimestamp(HTable index1, List<KeyValue> expected, long start,
      long end,
      byte[] startKey, byte[] endKey) throws IOException {
    Scan s = new Scan(startKey, endKey);
    s.setTimeRange(start, end);
    // s.setRaw(true);
    List<KeyValue> received = new ArrayList<KeyValue>();
    ResultScanner scanner = index1.getScanner(s);
    for (Result r : scanner) {
      received.addAll(r.list());
    }
    scanner.close();
    assertEquals("Didn't get the expected kvs from the index table!", expected, received);
  }

  private void closeAndCleanupTables(HTable... tables) throws IOException {
    if (tables == null) {
      return;
    }

    for (HTable table : tables) {
      table.close();
      UTIL.deleteTable(table.getTableName());
    }
  }
}
