package com.salesforce.hbase.stats.cleanup;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;

import com.salesforce.hbase.stats.StatisticsTable;
import com.salesforce.hbase.stats.util.SetupTableUtil;

/**
 * Cleanup the stats for the parent region on region split
 */
public class RemoveRegionOnSplit extends BaseRegionObserver {

  protected StatisticsTable stats;

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    HTableDescriptor desc = ((RegionCoprocessorEnvironment) e).getRegion().getTableDesc();
    if (SetupTableUtil.getStatsEnabled(desc)) {
      stats = StatisticsTable.getStatisticsTableForCoprocessor(e, desc.getName());
    }
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    if (stats != null) {
      stats.close();
    }
  }

  @Override
  public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e, HRegion l, HRegion r)
      throws IOException {
    // stats aren't enabled on the table, so we are done
    if (stats == null) {
      return;
    }
    // get the parent
    HRegion parent = e.getEnvironment().getRegion();
    // and remove it from the stats
    stats.removeStatsForRegion(parent.getRegionInfo());
  }

  /**
   * We override this method to ensure that any scanner from a previous coprocessor is returned. The
   * default behavior is to return <tt>null</tt>, which completely hoses any other coprocessors
   * setup before, making ordering of coprocessors very important. By returning the passed scanner,
   * we can avoid easy to make configuration errors.
   */
  @Override
  public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs,
      InternalScanner s) throws IOException {

    return s;
  }
}