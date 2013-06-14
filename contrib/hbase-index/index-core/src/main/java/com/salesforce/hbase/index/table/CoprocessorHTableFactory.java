package com.salesforce.hbase.index.table;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;

public class CoprocessorHTableFactory implements HTableFactory {

  private CoprocessorEnvironment e;

  public CoprocessorHTableFactory(CoprocessorEnvironment e) {
    this.e = e;
  }

  @Override
  public HTableInterface getTable(byte[] tablename) throws IOException {
    Configuration conf = e.getConfiguration();
    // make sure writers fail fast
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    conf.setInt(HConstants.HBASE_CLIENT_PAUSE, 1000);
    conf.setInt("zookeeper.recovery.retry", 3);
    conf.setInt("zookeeper.recovery.retry.intervalmill", 100);
    conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 30000);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 5000);

    return this.e.getTable(tablename);
  }
}