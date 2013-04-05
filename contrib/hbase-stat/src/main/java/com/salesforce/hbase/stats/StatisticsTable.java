package com.salesforce.hbase.stats;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.hbase.stats.serialization.IndividualStatisticWriter;
import com.salesforce.hbase.stats.util.Constants;


/**
 * Wrapper to access the statistics table for an HTable.
 * <p>
 * Each {@link StatisticsTable} is bound to access the statistics for a single 'primary' table. This
 * helps decrease the chances of reading/writing the wrong statistic for the source table
 * <p>
 * Each statistic is prefixed with the tablename and region from whence it came.
 */
public class StatisticsTable implements Closeable {

  private static final Log LOG = LogFactory.getLog(StatisticsTable.class);
  /** Map of the currently open statistics tables */
  private static final Map<String, StatisticsTable> tableMap = new HashMap<String, StatisticsTable>();

  /**
   * @param env Environment wherein the coprocessor is attempting to update the stats table.
   * @param primaryTableName name of the primary table on which we should collect stats
   * @return the {@link StatisticsTable} for the given primary table.
   * @throws IOException if the table cannot be created due to an underlying HTable creation error
   */
  public synchronized static StatisticsTable getStatisticsTableForCoprocessor(
      CoprocessorEnvironment env, byte[] primaryTableName) throws IOException {
    StatisticsTable table = tableMap.get(primaryTableName);
    if (table == null) {
      table = new StatisticsTable(env.getTable(Constants.STATS_TABLE_NAME_BYTES), primaryTableName);
      tableMap.put(Bytes.toString(primaryTableName), table);
    }
    return table;
  }

  private final HTableInterface target;
  private final byte[] sourceTableName;

  private StatisticsTable(HTableInterface target, byte[] sourceTableName) {
    this.target = target;
    this.sourceTableName = sourceTableName;
  }

  public StatisticsTable(Configuration conf, HTableDescriptor source) throws IOException {
    this(new HTable(conf, Constants.STATS_TABLE_NAME), source.getName());
  }

  /**
   * Close the connection to the table
   */
  @Override
  public void close() throws IOException {
    target.close();
  }

  public void removeStats() throws IOException {
    removeRowsForPrefix(sourceTableName);
  }

  public void removeStatsForRegion(HRegionInfo region) throws IOException {
    removeRowsForPrefix(sourceTableName, region.getRegionName());
  }

  private void removeRowsForPrefix(byte[]... arrays) throws IOException {
    byte[] row = null;
    for (byte[] array : arrays) {
      row = ArrayUtils.addAll(row, array);
    }
    Scan scan = new Scan(row);
    scan.setFilter(new PrefixFilter(row));
    cleanupRows(scan);
  }

  /**
   * Delete all the rows that we find from the scanner
   * @param scan scan used on the statistics table to determine which keys need to be deleted
   * @throws IOException if we fail to communicate with the HTable
   */
  private void cleanupRows(Scan scan) throws IOException {
    // Because each region has, potentially, a bunch of different statistics, we need to go through
    // an delete each of them as we find them

    // TODO switch this to a CP that lets us just do a filtered delete

    // first we have to scan the table to find the rows to delete
    ResultScanner scanner = target.getScanner(scan);
    Delete d = null;
    // XXX possible memory issues here - we could be loading a LOT of stuff as we are doing a
    // copy for each result
    for (Result r : scanner) {
      // create a delete for each result
      d = new Delete(r.getRow());
      // let the table figure out when it wants to flush that stuff
      target.delete(d);
    }
  }

  /**
   * Update a list of statistics for the given region
   * @param serializer to convert the actual statistics to puts in the statistics table
   * @param data Statistics for the region that we should update. The type of the
   *          {@link StatisticValue} (T1), is used as a suffix on the row key; this groups different
   *          types of metrics together on a per-region basis. Then the
   *          {@link StatisticValue#getInfo()}is used as the column qualifier. Finally,
   *          {@link StatisticValue#getValue()} is used for the the value of the {@link Put}. This
   *          can be <tt>null</tt> or <tt>empty</tt>.
   * @throws IOException if we fail to do any of the puts. Any single failure will prevent any
   *           future attempts for the remaining list of stats to update
   */
  public void updateStats(IndividualStatisticWriter serializer, List<StatisticValue> data)
      throws IOException {
    // short circuit if we have nothing to write
    if (data == null || data.size() == 0) {
      return;
    }

    // serialize each of the metrics with the associated serializer
    for (StatisticValue metric : data) {
      LOG.info("Writing statistic: " + metric);
      target.put(serializer.serialize(metric));
    }
    // make sure it all reaches the target table when we are done
    target.flushCommits();
  }

  /**
   * @return the underlying {@link HTableInterface} to which this table is writing
   */
  HTableInterface getUnderlyingTable() {
    return target;
  }

  byte[] getSourceTableName() {
    return this.sourceTableName;
  }
}