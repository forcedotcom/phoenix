package com.salesforce.hbase.stats;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;

import com.salesforce.hbase.stats.impl.MinMaxKey;
import com.salesforce.hbase.stats.impl.MinMaxKey.MinMaxStat;
import com.salesforce.hbase.stats.util.Constants;
import com.salesforce.hbase.stats.util.StatsTestUtil;

/**
 * Test the min/max key on a real table
 */
public class TestMinMaxKeyStats extends TestTrackerImpl {

  @Override
  protected void preparePrimaryTableDescriptor(HTableDescriptor primary) throws IOException {
    // just track the Min/Max Key
    MinMaxKey.addToTable(primary);
  }

  @Override
  protected void verifyStatistics(HTableDescriptor primary) throws IOException {
    // scan the stats table for a raw count
    HTable stats = new HTable(UTIL.getConfiguration(), Constants.STATS_TABLE_NAME);
    int count = StatsTestUtil.getKeyValueCount(stats);

    // we should have 2 stats - a min and a max for the one column of the one region of the table
    assertEquals("Got an unexpected amount of stats!", 2, count);

    // then do a read with the actual statistics
    // we know we are going to collect MinMaxKey so reading ensures we are collecting correctly
    StatisticsTable statTable = new StatisticsTable(UTIL.getConfiguration(), primary);
    StatisticReader<StatisticValue> reader = MinMaxKey.getStatistcReader(statTable);
    List<MinMaxStat> results = MinMaxKey.interpret(reader.read());
    assertEquals("Unexpected number of min/max results!", 1, results.size());
    assertArrayEquals("Unexpected number of min result!", new byte[] { 'a', 'a', 'a' },
      results.get(0).min);
    assertArrayEquals("Unexpected number of min result!", new byte[] { 'z', 'z', 'z' },
      results.get(0).max);

    // cleanup after ourselves
    stats.close();
    statTable.close();
  }

}
