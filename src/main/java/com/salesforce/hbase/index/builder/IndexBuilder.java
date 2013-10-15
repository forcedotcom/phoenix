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
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
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
public interface IndexBuilder extends Stoppable {

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
   * Your opportunity to update any/all index tables based on the update of the primary table row.
   * Its up to your implementation to ensure that timestamps match between the primary and index
   * tables.
   * <p>
   * The mutation is a generic mutation (not a {@link Put} or a {@link Delete}), as it actually
   * corresponds to a batch update. Its important to note that {@link Put}s always go through the
   * batch update code path, so a single {@link Put} will come through here and update the primary
   * table as the only update in the mutation.
   * <p>
   * Implementers must ensure that this method is thread-safe - it could (and probably will) be
   * called concurrently for different mutations, which may or may not be part of the same batch.
   * @param mutation update to the primary table to be indexed.
   * @return a Map of the mutations to make -> target index table name
   * @throws IOException on failure
   */
  public Collection<Pair<Mutation, byte[]>> getIndexUpdate(Mutation mutation) throws IOException;

  /**
   * The counter-part to {@link #getIndexUpdate(Mutation)} - your opportunity to update any/all
   * index tables based on the delete of the primary table row. This is only called for cases where
   * the client sends a single delete ({@link HTable#delete}). We separate this method from
   * {@link #getIndexUpdate(Mutation...)} only for the ease of implementation as the delete path has
   * subtly different semantics for updating the families/timestamps from the generic batch path.
   * <p>
   * Its up to your implementation to ensure that timestamps match between the primary and index
   * tables.
   * <p>
   * Implementers must ensure that this method is thread-safe - it could (and probably will) be
   * called concurrently for different mutations, which may or may not be part of the same batch.
   * @param delete {@link Delete} to the primary table that may be indexed
   * @return a {@link Map} of the mutations to make -> target index table name
   * @throws IOException on failure
   */
  public Collection<Pair<Mutation, byte[]>> getIndexUpdate(Delete delete) throws IOException;

  /**
   * Build an index update to cleanup the index when we remove {@link KeyValue}s via the normal
   * flush or compaction mechanisms.
   * @param filtered {@link KeyValue}s that previously existed, but won't be included in further
   *          output from HBase.
   * @return a {@link Map} of the mutations to make -> target index table name
   * @throws IOException on failure
   */
  public Collection<Pair<Mutation, byte[]>> getIndexUpdateForFilteredRows(
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
 * @throws IOException 
   */
  public boolean isEnabled(Mutation m) throws IOException;

  /**
   * @param m mutation that has been received by the indexer and is waiting to be indexed
   * @return the ID of batch to which the Mutation belongs, or <tt>null</tt> if the mutation is not
   *         part of a batch.
   */
  public byte[] getBatchId(Mutation m);
}