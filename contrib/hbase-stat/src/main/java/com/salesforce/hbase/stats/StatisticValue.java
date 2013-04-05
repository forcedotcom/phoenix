package com.salesforce.hbase.stats;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Simple holder class for a single statistics on a column in a region.
 * <p>
 * If you are build a histogram, should use the HistogramStat to store information, which internally
 * uses a collection of {@link StatisticValue}s to build a larger histogram
 */
public class StatisticValue {

  protected byte[] name;
  protected byte[] info;
  protected byte[] value;

  public StatisticValue(byte[] name, byte[] info, byte[] value) {
    this.name = name;
    this.info = info;
    this.value = value;
  }

  public byte[] getType() {
    return name;
  }

  public byte[] getInfo() {
    return info;
  }

  public byte[] getValue() {
    return value;
  }

  protected void setValue(byte[] value) {
    this.value = value;
  }

  public String toString(){
    return "stat " + Bytes.toString(name) + ": [info:" + Bytes.toString(info) + ", value:"
        + Bytes.toString(value) + "]";
  }
}