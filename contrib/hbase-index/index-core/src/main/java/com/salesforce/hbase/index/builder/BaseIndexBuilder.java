package com.salesforce.hbase.index.builder;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

/**
 * Basic implementation of the {@link IndexBuilder} that doesn't do any actual work of indexing.
 * <p>
 * You should extend this class, rather than implementing IndexBuilder directly to maintain
 * compatability going forward.
 */
public class BaseIndexBuilder implements IndexBuilder {

  @Override
  public void extendBaseIndexBuilderInstead() { }
  
  @Override
  public void setup(Configuration conf) {
    // noop
  }

  @Override
  public Map<Mutation, String> getIndexUpdate(Put put) {
    return null;
  }

  @Override
  public Map<Mutation, String> getIndexUpdate(Delete delete) {
    return null;
  }



}
