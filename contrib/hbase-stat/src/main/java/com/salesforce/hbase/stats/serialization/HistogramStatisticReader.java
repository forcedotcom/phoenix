package com.salesforce.hbase.stats.serialization;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.hbase.stats.ColumnFamilyStatistic;
import com.salesforce.hbase.stats.HistogramStatisticValue;
import com.salesforce.hbase.stats.StatisticValue;

/**
 * Get {@link HistogramStatisticValue}s from the underlying bytes. Expects serialization with the
 * {@link IndividualStatisticWriter}.
 */
public class HistogramStatisticReader implements IndividualStatisticReader<HistogramStatisticValue> {
  private final PointStatisticReader delegate;

  public HistogramStatisticReader() {
    delegate = new PointStatisticReader();
  }

  public ColumnFamilyStatistic<HistogramStatisticValue> deserialize(Result r) throws IOException {
    ColumnFamilyStatistic<StatisticValue> raw = delegate.deserialize(r);
    // then re-wrap the results so we can read histograms
    ColumnFamilyStatistic<HistogramStatisticValue> ret =
        new ColumnFamilyStatistic<HistogramStatisticValue>(raw.getRegion(), raw.getColumnfamily());
    for (StatisticValue value : raw.getValues()) {
      try {
        ret.add(new HistogramStatisticValue(value));
      } catch (InvalidProtocolBufferException e) {
        throw new IOException(e);
      }
    }
    return ret;
  }
}