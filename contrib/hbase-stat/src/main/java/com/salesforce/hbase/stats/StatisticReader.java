package com.salesforce.hbase.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.hbase.stats.serialization.IndividualStatisticReader;
import com.salesforce.hbase.stats.serialization.StatisticSerDe;
import com.salesforce.hbase.stats.util.Constants;

/**
 * Read a statistic from a {@link StatisticsTable}. This is an abstraction around the underlying
 * serialization format of a given statistic, allow us to change the format without exposing any
 * under the hood mechanics to the user.
 * @param <S> Type of statistic that should be read
 */
public class StatisticReader<S extends StatisticValue> {

  private static final Log LOG = LogFactory.getLog(StatisticReader.class);

  private IndividualStatisticReader<S> deserializer;
  private byte[] name;

  private HTableInterface table;

  private byte[] source;

  // by default, we only return the latest version of a statistc
  private static final int DEFAULT_VERSIONS = 1;

  public StatisticReader(StatisticsTable stats, IndividualStatisticReader<S> statReader,
      byte[] statisticName) {
    this.table = stats.getUnderlyingTable();
    this.source = stats.getSourceTableName();
    this.deserializer = statReader;
    this.name = statisticName;
  }

  /**
   * Read all the statistics with the reader for the current source table. If there have been
   * multiple statistics updates for the same column family/region (same name, over a time range)
   * then you will get only the latest version; to get older versions, use {@link #read(int)}. If
   * there are multiple regions or multiple column families, there will be one ColumnFamilyStatistic
   * per region-family pair (so with 2 regions, each with 2 column families who are tracked, there
   * will be 4 results).
   * @return a list of all the {@link ColumnFamilyStatistic}s gathered from that reader
   * @throws IOException if we cannot read the statistics table properly
   */
  public List<ColumnFamilyStatistic<S>> read() throws IOException {
    return read(DEFAULT_VERSIONS);
  }

  /**
   * Read all the statistics with the reader for the current source table. If there have been
   * multiple statistics updates for the same column family/region (same name, over a time range)
   * then you will get all the versions, one per {@link ColumnFamilyStatistic}, with the most recent
   * being <i>first</i> in the {@link ColumnFamilyStatistic#getValues()} list. If there are multiple
   * regions or multiple column families, there will be one ColumnFamilyStatistic per region-family
   * pair (so with 2 regions, each with 2 column families who are tracked and two different versions
   * of keys for each family, there will be 8 results).
   * @param versions max number of versions to read from the table
   * @return a list of all the {@link ColumnFamilyStatistic}s gathered from that reader
   * @throws IOException if we cannot read the statistics table properly
   */
  public List<ColumnFamilyStatistic<S>> read(int versions) throws IOException {
    byte[] scanPrefix = this.getRowKey();
    LOG.info("Reading for prefix: " + Bytes.toString(scanPrefix));
    return getResults(scanPrefix, versions);
  }

  /**
   * Read all the statistics with the reader for the current source table for the specified region
   * name. If there have been multiple statistics updates for the same column family/region (same
   * name, over a time range) then you will get only the latest version; to get older versions, use
   * {@link #read(byte[], int)}.If there are multiple column families in the region there will be
   * one ColumnFamilyStatistic per region-family pair (so with a single regions, with 2 column
   * families that are tracked, there will be 2 results).
   * @param region name of the region for which to get the stats
   * @return a list of all the {@link ColumnFamilyStatistic}s gathered from that reader
   * @throws IOException if we cannot read the statistics table properly
   */
  public List<ColumnFamilyStatistic<S>> read(byte[] region) throws IOException {
    return read(region, DEFAULT_VERSIONS);
  }

  /**
   * Read all the statistics with the reader for the current source table for the specified region
   * name. If there have been multiple statistics updates for the same column family/region (same
   * name, over a time range) then you will get all the versions, one per
   * {@link ColumnFamilyStatistic}, with the most recent being <i>first</i> in the
   * {@link ColumnFamilyStatistic#getValues()} list. If there are multiple column families in the
   * region there will be one ColumnFamilyStatistic per region-family pair (so with a single
   * regions, with 2 column families that are tracked and two different versions of stats, there
   * will be 4 results).
   * @param region name of the region for which to get the stats
   * @param versions max number of statistic versions to read
   * @return a list of all the {@link ColumnFamilyStatistic}s gathered from that reader
   * @throws IOException if we cannot read the statistics table properly
   */
  public List<ColumnFamilyStatistic<S>> read(byte[] region, int versions) throws IOException {
    byte[] scanPrefix = this.getRowKey(region);
    LOG.info("Reading for prefix: " + Bytes.toString(scanPrefix));
    return getResults(scanPrefix, versions);
  }

  /**
   * Read all the statistics with the reader for the current source table for the specified region
   * name. If there have been multiple statistics updates for the same column family/region (same
   * name, over a time range) then you will get only the latest version; to get older versions, use
   * {@link #read(byte[], byte[], int)}.
   * @param region name of the region for which to get the stats
   * @param column name of the column family for which to get the stats
   * @return a list of all the {@link ColumnFamilyStatistic}s gathered from that reader
   * @throws IOException if we cannot read the statistics table properly
   */
  public ColumnFamilyStatistic<S> read(byte[] region, byte[] column) throws IOException {
    return read(region, column, DEFAULT_VERSIONS);
  }

  /**
   * Read all the statistics with the reader for the current source table for the specified region
   * name. If there have been multiple statistics updates for the same column family/region (same
   * name, over a time range) then you will get all the versions, one per
   * {@link ColumnFamilyStatistic}, with the most recent being <i>first</i> in the
   * {@link ColumnFamilyStatistic#getValues()} list.
   * @param region name of the region for which to get the stats
   * @param column name of the column family for which to get the stats
   * @param versions max number of statistic versions to read
   * @return a list of all the {@link ColumnFamilyStatistic}s gathered from that reader
   * @throws IOException if we cannot read the statistics table properly
   */
  public ColumnFamilyStatistic<S> read(byte[] region, byte[] column, int versions)
      throws IOException {
    byte[] row = this.getRowKey(region, column);
    Get g = new Get(row);
    g.setMaxVersions(versions);
    Result r = table.get(g);
    return deserializer.deserialize(r);
  }

  /**
   * Read the latest version of the statistic from the primary table
   * @param t underlying table to read from
   * @param reader reader to use to deserialize the raw results
   * @param prefix key prefix to use when scanning
   * @param versions number of versions to read from the table
   * @return
   * @throws IOException
   */
  private List<ColumnFamilyStatistic<S>> getResults(byte[] prefix, int versions) throws IOException {
    Scan scan = new Scan(prefix);
    scan.addFamily(Constants.STATS_DATA_COLUMN_FAMILY);
    scan.setFilter(new PrefixFilter(prefix));
    // we only return the latest version of the statistic
    scan.setMaxVersions(versions);
    ResultScanner scanner = table.getScanner(scan);
    List<ColumnFamilyStatistic<S>> stats = new ArrayList<ColumnFamilyStatistic<S>>();
    for (Result r : scanner) {
      LOG.info("Got result:" + r);
      stats.add(deserializer.deserialize(r));
    }
    return stats;
  }

  private byte[] getRowKey() {
    return StatisticSerDe.getRowPrefix(source, name);
  }

  private byte[] getRowKey(byte[] regionname) {
    return StatisticSerDe.getRowPrefix(source, regionname, name);
  }

  private byte[] getRowKey(byte[] regionname, byte[] columnfamily) {
    return StatisticSerDe.getRowKey(source, regionname, columnfamily, name);
  }

}