/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *     Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *     Neither the name of Salesforce.com nor the names of its contributors may 
 *     be used to endorse or promote products derived from this software without 
 *     specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.hbase.index.covered;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.util.Pair;

import com.salesforce.hbase.index.covered.data.IndexMemStore;
import com.salesforce.hbase.index.covered.data.LocalHBaseState;
import com.salesforce.hbase.index.covered.update.ColumnReference;
import com.salesforce.hbase.index.covered.update.ColumnTracker;
import com.salesforce.hbase.index.covered.update.IndexedColumnGroup;
import com.salesforce.hbase.index.scanner.Scanner;
import com.salesforce.hbase.index.scanner.ScannerBuilder;

/**
 * Manage the state of the HRegion's view of the table, for the single row.
 * <p>
 * Currently, this is a single-use object - you need to create a new one for each row that you need
 * to manage. In the future, we could make this object reusable, but for the moment its easier to
 * manage as a throw-away object.
 * <p>
 * This class is <b>not</b> thread-safe - it requires external synchronization is access
 * concurrently.
 */
public class LocalTableState implements TableState {

  private long ts;
  private RegionCoprocessorEnvironment env;
  private KeyValueStore memstore;
  private LocalHBaseState table;
  private Mutation update;
  private Set<ColumnTracker> trackedColumns = new HashSet<ColumnTracker>();
  private ScannerBuilder scannerBuilder;
  private List<KeyValue> kvs = new ArrayList<KeyValue>();
  private List<? extends IndexedColumnGroup> hints;
  private CoveredColumns columnSet;

  public LocalTableState(RegionCoprocessorEnvironment environment, LocalHBaseState table, Mutation update) {
    this.env = environment;
    this.table = table;
    this.update = update;
    this.memstore = new IndexMemStore();
    this.scannerBuilder = new ScannerBuilder(memstore, update);
    this.columnSet = new CoveredColumns();
  }

  public void addPendingUpdates(KeyValue... kvs) {
    if (kvs == null) return;
    addPendingUpdates(Arrays.asList(kvs));
  }

  public void addPendingUpdates(List<KeyValue> kvs) {
    if(kvs == null) return;
    setPendingUpdates(kvs);
    addUpdate(kvs);
  }

  private void addUpdate(List<KeyValue> list) {
    addUpdate(list, true);
  }

  private void addUpdate(List<KeyValue> list, boolean overwrite) {
    if (list == null) return;
    for (KeyValue kv : list) {
      this.memstore.add(kv, overwrite);
    }
  }

  @Override
  public RegionCoprocessorEnvironment getEnvironment() {
    return this.env;
  }

  @Override
  public long getCurrentTimestamp() {
    return this.ts;
  }

  @Override
  public void setCurrentTimestamp(long timestamp) {
    this.ts = timestamp;
  }

  public void resetTrackedColumns() {
    this.trackedColumns.clear();
  }

  public Set<ColumnTracker> getTrackedColumns() {
    return this.trackedColumns;
  }

  @Override
  public Pair<Scanner, IndexUpdate> getIndexedColumnsTableState(
      Collection<? extends ColumnReference> indexedColumns) throws IOException {
    ensureLocalStateInitialized(indexedColumns);
    // filter out things with a newer timestamp and track the column references to which it applies
    ColumnTracker tracker = new ColumnTracker(indexedColumns);
    synchronized (this.trackedColumns) {
      // we haven't seen this set of columns before, so we need to create a new tracker
      if (!this.trackedColumns.contains(tracker)) {
        this.trackedColumns.add(tracker);
      }
    }

    Scanner scanner =
        this.scannerBuilder.buildIndexedColumnScanner(kvs, indexedColumns, tracker, ts);

    return new Pair<Scanner, IndexUpdate>(scanner, new IndexUpdate(tracker));
  }

  /**
   * Similar to {@link #getIndexedColumnsTableState(Collection)}, but doesn't update any
   * columnTrackers.
   * @param columns columns to scan
   * @return iterator over the requested columns
   * @throws IOException on failure to read the underlying state
   */
  public Scanner getNonIndexedColumnsTableState(List<? extends ColumnReference> columns)
      throws IOException {
    ensureLocalStateInitialized(columns);
    return this.scannerBuilder.buildNonIndexedColumnsScanner(columns, ts);
  }

  /**
   * Initialize the managed local state. Generally, this will only be called by
   * {@link #getNonIndexedColumnsTableState(List)}, which is unlikely to be called concurrently from the outside.
   * Even then, there is still fairly low contention as each new Put/Delete will have its own table
   * state.
   */
  private synchronized void ensureLocalStateInitialized(
      Collection<? extends ColumnReference> columns) throws IOException {
    // check to see if we haven't initialized any columns yet
    Collection<? extends ColumnReference> toCover = this.columnSet.findNonCoveredColumns(columns);
    // we have all the columns loaded, so we are good to go.
    if (toCover.isEmpty()) {
      return;
    }

    // add the current state of the row
    this.addUpdate(this.table.getCurrentRowState(update, toCover).list(), false);

    // add the covered columns to the set
    for (ColumnReference ref : toCover) {
      this.columnSet.addColumn(ref);
    }
  }

  @Override
  public Map<String, byte[]> getUpdateAttributes() {
    return this.update.getAttributesMap();
  }

  @Override
  public byte[] getCurrentRowKey() {
    return this.update.getRow();
  }

  public Result getCurrentRowState() {
    KeyValueScanner scanner = this.memstore.getScanner();
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    while (scanner.peek() != null) {
      try {
        kvs.add(scanner.next());
      } catch (IOException e) {
        // this should never happen - something has gone terribly arwy if it has
        throw new RuntimeException("Local MemStore threw IOException!");
      }
    }
    return new Result(kvs);
  }

  /**
   * Helper to add a {@link Mutation} to the values stored for the current row
   * @param pendingUpdate update to apply
   */
  public void addUpdateForTesting(Mutation pendingUpdate) {
    for (Map.Entry<byte[], List<KeyValue>> e : pendingUpdate.getFamilyMap().entrySet()) {
      List<KeyValue> edits = e.getValue();
      addUpdate(edits);
    }
  }

  /**
   * @param hints
   */
  public void setHints(List<? extends IndexedColumnGroup> hints) {
    this.hints = hints;
  }

  @Override
  public List<? extends IndexedColumnGroup> getIndexColumnHints() {
    return this.hints;
  }

  @Override
  public Collection<KeyValue> getPendingUpdate() {
    return this.kvs;
  }

  /**
   * Set the {@link KeyValue}s in the update for which we are currently building an index update,
   * but don't actually apply them.
   * @param update pending {@link KeyValue}s
   */
  public void setPendingUpdates(Collection<KeyValue> update) {
    this.kvs.clear();
    this.kvs.addAll(update);
  }

  /**
   * Apply the {@link KeyValue}s set in {@link #setPendingUpdates(Collection)}.
   */
  public void applyPendingUpdates() {
    this.addUpdate(kvs);
  }

  /**
   * Rollback all the given values from the underlying state.
   * @param values
   */
  public void rollback(Collection<KeyValue> values) {
    for (KeyValue kv : values) {
      this.memstore.rollback(kv);
    }
  }
}