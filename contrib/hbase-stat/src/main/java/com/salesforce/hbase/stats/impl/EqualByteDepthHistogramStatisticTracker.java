package com.salesforce.hbase.stats.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.salesforce.hbase.stats.BaseStatistic;
import com.salesforce.hbase.stats.HistogramStatisticValue;
import com.salesforce.hbase.stats.StatisticReader;
import com.salesforce.hbase.stats.StatisticTracker;
import com.salesforce.hbase.stats.StatisticValue;
import com.salesforce.hbase.stats.StatisticsTable;
import com.salesforce.hbase.stats.serialization.HistogramStatisticReader;

/**
 * {@link StatisticTracker} that keeps track of an equal depth histogram.
 * <p>
 * This is different from a traditional histogram in that we just keep track of the key at every 'n'
 * bytes; another name for this is region "guide posts".
 * <p>
 * When using this statistic, be <b>very careful</b> when selecting the byte width of each column -
 * it could lead to an incredibly large histogram, which could crash the region server.
 */
public class EqualByteDepthHistogramStatisticTracker extends BaseStatistic {

  public static final String BYTE_DEPTH_CONF_KEY = "com.salesforce.guidepost.width";

  private final static byte[] NAME = Bytes.toBytes("equal_depth_histogram");

  private static final long DEFAULT_BYTE_DEPTH = 100;

  private long guidepostDepth;
  private long byteCount = 0;
  private HistogramStatisticValue histogram;

  public static void addToTable(HTableDescriptor desc, long depth) throws IOException {
    Map<String, String> props = Collections.singletonMap(BYTE_DEPTH_CONF_KEY, Long.toString(depth));
    desc.addCoprocessor(EqualByteDepthHistogramStatisticTracker.class.getName(), null,
      Coprocessor.PRIORITY_USER, props);
  }

  /**
   * Get a reader for the statistic
   * @param stats statistics table from which you want to read the stats
   * @return a {@link StatisticReader} to get the raw Histogram stats.
   */
  public static StatisticReader<HistogramStatisticValue> getStatistcReader(StatisticsTable stats) {
    return new StatisticReader<HistogramStatisticValue>(stats,
        new HistogramStatisticReader(), NAME);
  }

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
   super.start(e);
   //get the byte depth for this histogram
    guidepostDepth = e.getConfiguration().getLong(BYTE_DEPTH_CONF_KEY, DEFAULT_BYTE_DEPTH);
    this.histogram = newHistogram();
  }

  private HistogramStatisticValue newHistogram() {
    return new HistogramStatisticValue(NAME, Bytes.toBytes("equal_width_histogram_"
        + guidepostDepth + "bytes"), guidepostDepth);
  }

  @Override
  public List<StatisticValue> getCurrentStats() {
    return Collections.singletonList((StatisticValue) histogram);
  }

  @Override
  public void clear() {
    this.histogram = newHistogram();
    this.byteCount = 0;
  }

  @Override
  public void updateStatistic(KeyValue kv) {
    byteCount += kv.getLength();
    // if we are at the next guide-post, add it to the histogram
    if (byteCount >= guidepostDepth) {
      // update the histogram
      this.histogram.addColumn(ByteString.copyFrom(kv.getBuffer(), kv.getOffset(), kv.getLength()));

      //reset the count for the next key
      byteCount = 0;
    }
  }
}
