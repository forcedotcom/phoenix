package com.salesforce.hbase.stats.serialization;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;

import com.salesforce.hbase.stats.ColumnFamilyStatistic;
import com.salesforce.hbase.stats.StatisticValue;

/**
 * Deserializer for a {@link StatisticValue} from the raw {@link Result}. This is the complement
 * to the {@link IndividualStatisticWriter}.
 * @param <S> type of statistic value to deserialize
 */
public interface IndividualStatisticReader<S extends StatisticValue> {
  public ColumnFamilyStatistic<S> deserialize(Result r) throws IOException;
}