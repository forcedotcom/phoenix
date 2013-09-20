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
package com.salesforce.hbase.index.builder;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Pair;

import com.salesforce.hbase.index.Indexer;

/**
 * Interface to build updates ({@link Mutation}s) to the index tables, based on the primary table
 * updates.
 * <p>
 * Either all the index updates will be applied to all tables or the primary table will kill itself
 * and will attempt to replay the index edits through the WAL replay mechanism.
 */
public interface IndexBuilder {

  /** Helper method signature to ensure people don't attempt to extend this class directly */
  public void extendBaseIndexBuilderInstead();

  /**
   * This is always called exactly once on install of {@link Indexer}, before any calls
   * {@link #getIndexUpdate} on
   * @param env in which the builder is running
   * @throws IOException on failure to setup the builder
   */
  public void setup(RegionCoprocessorEnvironment env) throws IOException;

  /**
   * Your opportunity to update any/all index tables based on the delete of the primary table row.
   * Its up to your implementation to ensure that timestamps match between the primary and index
   * tables.
   * @param put {@link Put} to the primary table that may be indexed
   * @return a Map of the mutations to make -> target index table name
   * @throws IOException on failure
   */
  public Collection<Pair<Mutation, String>> getIndexUpdate(Put put) throws IOException;

  /**
   * The counter-part to {@link #getIndexUpdate(Put)} - your opportunity to update any/all index
   * tables based on the delete of the primary table row. Its up to your implementation to ensure
   * that timestamps match between the primary and index tables.
   * @param delete {@link Delete} to the primary table that may be indexed
   * @return a {@link Map} of the mutations to make -> target index table name
   * @throws IOException on failure
   */
  public Collection<Pair<Mutation, String>> getIndexUpdate(Delete delete) throws IOException;

  /**
   * Build an index update to cleanup the index when we remove {@link KeyValue}s via the normal
   * flush or compaction mechanisms.
   * @param filtered {@link KeyValue}s that previously existed, but won't be included in further
   *          output from HBase.
   * @return a {@link Map} of the mutations to make -> target index table name
   * @throws IOException on failure
   */
  public Collection<Pair<Mutation, String>> getIndexUpdateForFilteredRows(
      Collection<KeyValue> filtered)
      throws IOException;

  /**
   * Notification that a batch of updates has successfully been written.
   * @param miniBatchOp the full batch operation that was written
   */
  public void batchCompleted(MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp);

  /**
   * Notification that a batch has been started.
   * <p>
   * Unfortunately, the way HBase has the coprocessor hooks setup, this is actually called
   * <i>after</i> the {@link #getIndexUpdate} methods. Therefore, you will likely need an attribute
   * on your {@link Put}/{@link Delete} to indicate it is a batch operation.
   * @param miniBatchOp the full batch operation to be written
 * @throws IOException 
   */
  public void batchStarted(MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp) throws IOException;

  /**
   * This allows the codec to dynamically change whether or not indexing should take place for a
   * table. If it doesn't take place, we can save a lot of time on the regular Put patch. By making
   * it dynamic, we can save offlining and then onlining a table just to turn indexing on.
   * <p>
   * We can also be smart about even indexing a given update here too - if the update doesn't
   * contain any columns that we care about indexing, we can save the effort of analyzing the put
   * and further.
   * @param m mutation that should be indexed.
   * @return <tt>true</tt> if indexing is enabled for the given table. This should be on a per-table
   *         basis, as each codec is instantiated per-region.
   */
  public boolean isEnabled(Mutation m);
}