package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.junit.BeforeClass;

/**
 * Do the WAL Replay test but with the WALEditCodec, rather than an {@link IndexedHLogReader}, but
 * still without compression
 */
public class TestWALReplayWithCompressedIndexWrites extends TestWALReplayWithIndexWrites {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    configureCluster();
    // use our custom Codec to handle the custom WALEdits
    Configuration conf = UTIL.getConfiguration();
    conf.set(WALEditCodec.WAL_EDIT_CODEC_CLASS_KEY, IndexedWALEditCodec.class.getName());

    // enable WAL compression
    conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);

    startCluster();
  }
}