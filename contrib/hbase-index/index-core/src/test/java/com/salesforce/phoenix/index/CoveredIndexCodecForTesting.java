package com.salesforce.phoenix.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import com.salesforce.hbase.index.builder.covered.IndexCodec;
import com.salesforce.hbase.index.builder.covered.IndexUpdate;
import com.salesforce.hbase.index.builder.covered.TableState;

/**
 * An {@link IndexCodec} for testing that allow you to specify the index updates/deletes, regardless
 * of the current tables' state.
 */
public class CoveredIndexCodecForTesting implements IndexCodec {

  private List<IndexUpdate> deletes = new ArrayList<IndexUpdate>();
  private List<IndexUpdate> updates = new ArrayList<IndexUpdate>();

  public void addIndexDelete(IndexUpdate... deletes) {
    this.deletes.addAll(Arrays.asList(deletes));
  }
  
  public void addIndexUpserts(IndexUpdate... updates) {
    this.updates.addAll(Arrays.asList(updates));
  }

  public void clear() {
    this.deletes.clear();
    this.updates.clear();
  }
  
  @Override
  public Iterable<IndexUpdate> getIndexDeletes(TableState state) {
    return this.deletes;
  }

  @Override
  public Iterable<IndexUpdate> getIndexUpserts(TableState state) {
    return this.updates;
  }

  @Override
  public void initialize(RegionCoprocessorEnvironment env) throws IOException {
    // noop
  }
}