package com.salesforce.hbase.stats;

import java.util.List;

import org.apache.hadoop.hbase.KeyValue;

/**
 * Track a statistic for the column on a given region
 */
public interface StatisticTracker {

  /**
   * Reset the statistic after the completion fo the compaction
   */
  public void clear();

  /**
   * @return the current statistics that the tracker has collected
   */
  public List<StatisticValue> getCurrentStats();

  /**
   * Update the current statistics with the next {@link KeyValue} to be written
   * @param kv next {@link KeyValue} to be written
   */
  public void updateStatistic(KeyValue kv);
}