package com.salesforce.hbase.stats.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.hbase.stats.BaseStatistic;
import com.salesforce.hbase.stats.ColumnFamilyStatistic;
import com.salesforce.hbase.stats.StatisticReader;
import com.salesforce.hbase.stats.StatisticValue;
import com.salesforce.hbase.stats.StatisticsTable;
import com.salesforce.hbase.stats.serialization.PointStatisticReader;

/**
 * Coprocessor that just keeps track of the min/max key on a per-column family basis.
 * <p>
 * This can then also be used to find the per-table min/max key for the table.
 */
public class MinMaxKey extends BaseStatistic {

  public static void addToTable(HTableDescriptor desc) throws IOException {
    desc.addCoprocessor(MinMaxKey.class.getName());
  }

  private static final byte[] MAX_SUFFIX = Bytes.toBytes("max_region_key");
  private static final byte[] MIN_SUFFIX = Bytes.toBytes("min_region_key");
  private final static byte[] NAME = Bytes.toBytes("min_max_stat");

  private byte[] min;
  private byte[] max;

  @Override
  public List<StatisticValue> getCurrentStats() {
    List<StatisticValue> data = new ArrayList<StatisticValue>(2);
    data.add(new StatisticValue(NAME, MIN_SUFFIX, min));
    data.add(new StatisticValue(NAME, MAX_SUFFIX, max));
    return data;
  }

  @Override
  public void clear() {
    this.max = null;
    this.min = null;
  }

  @Override
  public void updateStatistic(KeyValue kv) {
    // first time through, so both are null
    if (min == null) {
      min = TrackerUtil.copyRow(kv);
      max = TrackerUtil.copyRow(kv);
      return;
    }
    if (Bytes.compareTo(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), min, 0, min.length) < 0) {
      min = TrackerUtil.copyRow(kv);
    }
    if (Bytes.compareTo(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), max, 0, max.length) > 0) {
      max = TrackerUtil.copyRow(kv);
    }
  }

  /**
   * Find a reader for the the min/max key based on the type of serialization of the key.
   * @param stats table from which you want to read the stats
   * @return a {@link StatisticReader} to get the raw Min/Max stats. Use {@link #interpret(List)} to
   *         get a list of the most recent min/max values on a per-column, per-region basis.
   */
  public static StatisticReader<StatisticValue> getStatistcReader(StatisticsTable stats) {
    return new StatisticReader<StatisticValue>(stats,
        new PointStatisticReader(), NAME);
  }

  /**
   * Combine the results from {@link #getStatistcReader(StatisticsTable)} into {@link MinMaxStat}
   * results for easy digestion
   * @param stat statistics from {@link #getStatistcReader(StatisticsTable)}.
   * @return the min/max per column family per region
   */
  public static List<MinMaxStat> interpret(List<ColumnFamilyStatistic<StatisticValue>> stat) {
    List<MinMaxStat> stats = new ArrayList<MinMaxStat>();
    for (int i = 0; i < stat.size(); i++) {
      // every two column family statistic is actually one statistic, so we need to combine them
      ColumnFamilyStatistic<StatisticValue> minmax = stat.get(i++);
      StatisticValue max = minmax.getValues().get(0);
      StatisticValue min = minmax.getValues().get(1);
      // we only return the most recent min/max combination for the column family/region
      stats.add(new MinMaxStat(minmax.getRegion(), minmax.getColumnfamily(), max, min));
    }
    return stats;

  }

  /**
   * Abstraction of a statistic that combines two {@link StatisticValue}s to generate a single
   * min/max stat for a single column family of a region.
   */
  public static class MinMaxStat {

    public final byte[] region;
    public final byte[] family;
    public final byte[] max;
    public final byte[] min;

    /**
     * @param region region where the stat was obtained
     * @param columnfamily column family for which the stat was calculated
     * @param min the min key as a {@link StatisticValue}
     * @param max the max key as a {@link StatisticValue}
     */
    public MinMaxStat(byte[] region, byte[] columnfamily, StatisticValue max, StatisticValue min) {
      this.region = region;
      this.family = columnfamily;
      this.max = max.getValue();
      this.min = min.getValue();
    }
  }
}
