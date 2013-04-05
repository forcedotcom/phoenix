package com.salesforce.hbase.stats;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.hbase.protobuf.generated.StatisticProtos.Histogram;
import com.salesforce.hbase.stats.impl.EqualByteDepthHistogramStatisticTracker;

/**
 * Simple unit test of equal width histograms. Doesn't test against a full cluster, but rather is
 * just simple interface testing.
 */
public class TestEqualWidthHistogramStat {

  // number of keys in each column
  private final int columnWidth = 676;
  // depth is the width (count of keys) times the number of bytes of each key, which in this case is
  // fixed to 3 bytes, so we know the depth in all cases
  private final int columnDepth = columnWidth * 3;

  @Test
  public void testSimpleStat() throws IOException {
    EqualByteDepthHistogramStatisticTracker tracker = new EqualByteDepthHistogramStatisticTracker();
    // unfortunately, need to mock a lot here
    RegionCoprocessorEnvironment env = Mockito.mock(RegionCoprocessorEnvironment.class);
    HRegion mockRegion = Mockito.mock(HRegion.class);
    String tableName = "testSimpleStatPrimary";
    HTableDescriptor primary = new HTableDescriptor(tableName);
    Mockito.when(env.getRegion()).thenReturn(mockRegion);
    Mockito.when(mockRegion.getTableDesc()).thenReturn(primary);
    HTableInterface mockTable = Mockito.mock(HTableInterface.class);
    Mockito.when(env.getTable((byte[]) Mockito.any())).thenReturn(mockTable);

    // setup the actual configuration that we care about
    Configuration conf = new Configuration(false);
    // setup our byte width == to [letter]zz
    conf.setLong(EqualByteDepthHistogramStatisticTracker.BYTE_DEPTH_CONF_KEY, columnDepth);
    Mockito.when(env.getConfiguration()).thenReturn(conf);

    // setup the tracker
    tracker.start(env);

    // put some data in the tracker and check the histograms that come out
    loadAndVerifyTracker(tracker);

    // should be able to clear it and get the exact same results
    tracker.clear();
    loadAndVerifyTracker(tracker);
  }

  /**
   * @param tracker tracker to load with data and then validate
   * @throws InvalidProtocolBufferException if protobufs are broken - should not be thrown since we
   *           are not serializing information
   */
  private void loadAndVerifyTracker(EqualByteDepthHistogramStatisticTracker tracker)
      throws InvalidProtocolBufferException {
    // now feed the tracker a bunch of bytes
    KeyValue kv = new KeyValue();
    byte[] k = new byte[3];
    for (byte b1 = 'a'; b1 <= 'z'; b1++) {
      for (byte b2 = 'a'; b2 <= 'z'; b2++) {
        for (byte b3 = 'a'; b3 <= 'z'; b3++) {
          k[0] = b1;
          k[1] = b2;
          k[2] = b3;
          kv = new KeyValue(k, 0, 3);
          tracker.updateStatistic(kv);
        }
      }
    }

    List<StatisticValue> stats = tracker.getCurrentStats();
    assertEquals("Got more than one histogram!", 1, stats.size());
    HistogramStatisticValue stat = (HistogramStatisticValue) stats.get(0);
    Histogram histogram = stat.getHistogram();
    assertEquals("Got an incorrect number of guideposts!", 26, histogram.getValueList().size());

    // make sure we got the correct guideposts
    byte counter = 'a';
    for (ByteString column : histogram.getValueList()) {
      byte[] guidepost = new byte[] { counter, 'z', 'z' };
      byte[] actual = column.toByteArray();
      assertArrayEquals(
        "Guidepost should be:" + Bytes.toString(guidepost) + " , but was: "
            + Bytes.toString(actual), guidepost, actual);
      counter++;
    }
  }
}