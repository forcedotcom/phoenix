package com.salesforce.hbase.index.builder;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

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
  public void setup(RegionCoprocessorEnvironment conf) throws IOException {
    // noop
  }

  @Override
  public Map<Mutation, String> getIndexUpdate(Put put) throws IOException {
    return null;
  }

  @Override
  public Map<Mutation, String> getIndexUpdate(Delete delete) throws IOException {
    return null;
  }



}
