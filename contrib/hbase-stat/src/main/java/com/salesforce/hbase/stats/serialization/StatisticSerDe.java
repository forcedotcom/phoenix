package com.salesforce.hbase.stats.serialization;

import org.apache.hadoop.hbase.util.Bytes;


/**
 * Simple utility class for managing multiple key parts of the statistic
 */
public class StatisticSerDe {

  private StatisticSerDe() {
    // private ctor for utility classes
  }

  /** Number of parts in our complex key */
  protected static final int NUM_KEY_PARTS = 4;

  /**
   * Get the prefix based on the region, column and name of the statistic
   * @param table name of the source table
   * @param statName name of the statistic
   * @return the row key that should be used for this statistic
   */
  public static byte[] getRowPrefix(byte[] table, byte[] statName) {
    byte[] prefix = table;
    prefix = Bytes.add(prefix, statName);
    return prefix;
  }

  /**
   * Get the prefix based on the region, column and name of the statistic
   * @param table name of the source table
   * @param regionname name of the region where the statistic was gathered
   * @param statName name of the statistic
   * @return the row key that should be used for this statistic
   */
  public static byte[] getRowPrefix(byte[] table, byte[] regionname, byte[] statName) {
    byte[] prefix = table;
    prefix = Bytes.add(prefix, statName);
    prefix = Bytes.add(prefix, regionname);
    return prefix;
  }

  /**
   * Get the prefix based on the region, column and name of the statistic
   * @param table name of the source table
   * @param region name of the region where the statistic was gathered
   * @param column column for which the statistic was gathered
   * @param statName name of the statistic
   * @return the row key that should be used for this statistic
   */
  public static byte[] getRowKey(byte[] table, byte[] region, byte[] column, byte[] statName) {
    // always starts with the source table
    byte[] prefix = new byte[0];
    // then append each part of the key and
    byte[][] parts = new byte[][] { table, statName, region, column };
    int[] sizes = new int[NUM_KEY_PARTS];
    // XXX - this where we would use orderly to get the sorting consistent
    for (int i = 0; i < NUM_KEY_PARTS; i++) {
      prefix = Bytes.add(prefix, parts[i]);
      sizes[i] = parts[i].length;
    }
    // then we add on the sizes to the end of the key
    for (int size : sizes) {
      prefix = Bytes.add(prefix, Bytes.toBytes(size));
    }

    return prefix;
  }
}