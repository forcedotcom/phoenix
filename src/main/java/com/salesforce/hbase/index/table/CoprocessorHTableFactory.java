package com.salesforce.hbase.index.table;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.hbase.index.util.ImmutableBytesPtr;

public class CoprocessorHTableFactory implements HTableFactory {

  private static final Log LOG = LogFactory.getLog(CoprocessorHTableFactory.class);
  private CoprocessorEnvironment e;

  public CoprocessorHTableFactory(CoprocessorEnvironment e) {
    this.e = e;
  }

  @Override
  public HTableInterface getTable(ImmutableBytesPtr tablename) throws IOException {
    Configuration conf = e.getConfiguration();
    // make sure writers fail fast
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    conf.setInt(HConstants.HBASE_CLIENT_PAUSE, 1000);
    conf.setInt("zookeeper.recovery.retry", 3);
    conf.setInt("zookeeper.recovery.retry.intervalmill", 100);
    conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 30000);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 5000);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting access to new HTable: " + Bytes.toString(tablename.copyBytesIfNecessary()));
    }
    return this.e.getTable(tablename.copyBytesIfNecessary());
  }

  @Override
  public void shutdown() {
    // noop
  }
}