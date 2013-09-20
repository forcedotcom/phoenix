package com.salesforce.hbase.index.table;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;

import com.salesforce.hbase.index.util.ImmutableBytesPtr;

public class CoprocessorHTableFactory implements HTableFactory {

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

    return this.e.getTable(copyOrBytesIfExact(tablename));
  }

  /**
   * There are possible outcomes: a copy of the section of bytes that we care about, or the exact
   * byte array, if the {@link ImmutableBytesPtr} doesn't map 'into' the byte array. This saves us
   * doing an extra byte copy for each table name.
   * <p>
   * Either way, you should not modify the returned bytes - it should be assumed that they are
   * backing the {@link ImmutableBytesPtr}.
   * @param bytes to introspect
   * @return a byte[] from the {@link ImmutableBytesPtr}
   */
  private byte[] copyOrBytesIfExact(ImmutableBytesPtr bytes) {
    if (bytes.getOffset() == 0) {
      if (bytes.getLength() == bytes.get().length) {
        return bytes.get();
      }
    }
    return bytes.copyBytes();
  }

  @Override
  public void shutdown() {
    // noop
  }
}