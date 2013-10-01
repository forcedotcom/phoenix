package org.apache.hadoop.hbase.regionserver.wal;

import org.junit.BeforeClass;

import com.salesforce.hbase.index.util.IndexManagementUtil;

/**
 * Do the WAL Replay test but with the WALEditCodec, rather than an {@link IndexedHLogReader}, but
 * still with compression
 */
public class TestWALReplayWithIndexWritesAndUncompressedWALInHBase_094_9 extends TestWALReplayWithIndexWritesAndCompressedWAL {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    configureCluster();
    // use our custom WAL Reader
    UTIL.getConfiguration().set(IndexManagementUtil.HLOG_READER_IMPL_KEY,
      IndexedHLogReader.class.getName());
    startCluster();
  }
}