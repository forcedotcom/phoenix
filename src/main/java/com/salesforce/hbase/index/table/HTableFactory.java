package com.salesforce.hbase.index.table;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;

public interface HTableFactory {

  public HTableInterface getTable(byte [] tablename) throws IOException;
}
