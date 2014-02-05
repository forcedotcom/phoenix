package com.salesforce.hbase.stats.impl;

import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;

import com.salesforce.hbase.stats.StatisticTracker;

/**
 * Utilities for {@link StatisticTracker}s.
 */
public class TrackerUtil {

  private TrackerUtil() {
    // private ctor for utils
  }

  public static byte[] copyRow(KeyValue kv) {
    return Arrays.copyOfRange(kv.getBuffer(), kv.getRowOffset(),
      kv.getRowOffset() + kv.getRowLength());
  }

}
