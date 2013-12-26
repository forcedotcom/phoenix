package com.salesforce.hbase.index.write;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTableInterface;

import com.salesforce.hbase.index.table.HTableFactory;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;

/**
 * Simple table factory that just looks up the tables based on name. Useful for mocking up
 * {@link HTableInterface}s without having to mock up the factory too.
 */
class FakeTableFactory implements HTableFactory {

  boolean shutdown = false;
  private Map<ImmutableBytesPtr, HTableInterface> tables;

  public FakeTableFactory(Map<ImmutableBytesPtr, HTableInterface> tables) {
    this.tables = tables;
  }

  @Override
  public HTableInterface getTable(ImmutableBytesPtr tablename) throws IOException {
    return this.tables.get(tablename);
  }

  @Override
  public void shutdown() {
    shutdown = true;
  }
}