package com.salesforce.hbase.stats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Encapsulate all the values of the statistics for a given {@link StatisticValue} over a single
 * column family in a single region.
 * @param <S> type of statistic that is being retrieved/stored
 */
public class ColumnFamilyStatistic<S extends StatisticValue> {

  private List<S> values;
  private final byte[] region;
  private final byte[] columnfamily;

  public ColumnFamilyStatistic(byte[] region, byte[] columnfamily) {
    this.region = region;
    this.columnfamily = columnfamily;
    this.values = new ArrayList<S>();
  }

  public ColumnFamilyStatistic(byte[] region, byte[] columnfamily, S... values) {
    this(region, columnfamily, Arrays.asList(values));
  }

  public ColumnFamilyStatistic(byte[] region, byte[] columnfamily, List<S> values) {
    this(region, columnfamily);
    this.values.addAll(values);
  }

  public byte[] getRegion() {
    return region;
  }

  public byte[] getColumnfamily() {
    return columnfamily;
  }

  public void add(S value) {
    this.values.add(value);
  }

  public void setValues(List<S> values) {
    this.values = values;
  }

  public List<S> getValues() {
    return this.values;
  }

  public String toString() {
    return "stat:[region=" + Bytes.toString(region) + ", column="
 + Bytes.toString(columnfamily)
        + ", stats:" + values + "]";
  }
}