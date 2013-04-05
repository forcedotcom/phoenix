package com.salesforce.hbase.stats.cleanup;

import java.io.IOException;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.hbase.stats.StatisticsTable;
import com.salesforce.hbase.stats.util.SetupTableUtil;

public class RemoveTableOnDelete extends BaseMasterObserver {

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName)
      throws IOException {
    HTableDescriptor desc = ctx.getEnvironment().getMasterServices().getTableDescriptors()
        .get(tableName);
    if (desc == null) {
      throw new IOException("Can't find table descriptor for table '" + Bytes.toString(tableName)
          + "' that is about to be deleted!");
    }
    // if we have turned on stats for this table
    if (SetupTableUtil.getStatsEnabled(desc)) {
      StatisticsTable stats = StatisticsTable.getStatisticsTableForCoprocessor(
        ctx.getEnvironment(), desc.getName());
      stats.removeStats();
      stats.close();
    }
  }
}