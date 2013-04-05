package com.salesforce.hbase.stats.serialization;

import org.apache.hadoop.hbase.client.Put;

import com.salesforce.hbase.stats.StatisticValue;
import com.salesforce.hbase.stats.util.Constants;

/**
 * Simple serializer that always puts generates the same formatted key for an individual
 * statistic. This writer is used to write a single {@link StatisticValue} to the statistics
 * table. They should be read back via an {@link IndividualStatisticReader}.
 */
public class IndividualStatisticWriter {
  private final byte[] source;
  private byte[] region;
  private byte[] column;

  public IndividualStatisticWriter(byte[] sourcetable, byte[] region, byte[] column) {
    this.source = sourcetable;
    this.region = region;
    this.column = column;
  }

  public Put serialize(StatisticValue value) {
    byte[] prefix = StatisticSerDe.getRowKey(source, region, column, value.getType());
    Put put = new Put(prefix);
    put.add(Constants.STATS_DATA_COLUMN_FAMILY, value.getInfo(), value.getValue());
    return put;
  }

}