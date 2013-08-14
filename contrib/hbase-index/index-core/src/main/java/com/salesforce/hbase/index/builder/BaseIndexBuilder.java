package com.salesforce.hbase.index.builder;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Basic implementation of the {@link IndexBuilder} that doesn't do any actual work of indexing.
 * <p>
 * You should extend this class, rather than implementing IndexBuilder directly to maintain
 * compatability going forward.
 */
public abstract class BaseIndexBuilder implements IndexBuilder {

  @Override
  public void extendBaseIndexBuilderInstead() { }
  
  @Override
  public void setup(RegionCoprocessorEnvironment conf) throws IOException {
    // noop
  }

  @Override
  public void batchStarted(MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp) {
    // noop
  }

  @Override
  public void batchCompleted(MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp) {
    // noop
  }
}