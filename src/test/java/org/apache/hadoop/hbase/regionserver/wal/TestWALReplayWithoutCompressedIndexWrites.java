package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.junit.BeforeClass;

/**
 * Do the WAL Replay test but with the our custom {@link WALEditCodec} - {@link IndexedWALEditCodec}
 * - and enabling compression - the main use case for having a custom {@link WALEditCodec}.
 */
public class TestWALReplayWithoutCompressedIndexWrites extends TestWALReplayWithIndexWrites {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    configureCluster();

    // use our custom Codec to handle the custom WALEdits
    Configuration conf = UTIL.getConfiguration();
    conf.set(WALEditCodec.WAL_EDIT_CODEC_CLASS_KEY, IndexedWALEditCodec.class.getName());

    // disable WAL compression
    conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, false);

    startCluster();
  }
}