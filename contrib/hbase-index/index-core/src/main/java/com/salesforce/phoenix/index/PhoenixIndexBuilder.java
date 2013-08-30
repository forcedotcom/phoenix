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
package com.salesforce.phoenix.index;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.*;

import com.google.common.collect.*;
import com.google.common.primitives.Longs;
import com.salesforce.hbase.index.builder.BaseIndexBuilder;
import com.salesforce.hbase.index.builder.covered.*;

/**
 * Build covered indexes for phoenix updates.
 * <p>
 * Before any call to prePut/preDelete, the row has already been locked. This ensures that we don't
 * need to do any extra synchronization in the IndexBuilder.
 * <p>
 * <b>WARNING:</b> This builder does not correctly support adding the same row twice to a batch
 * update. For instance, adding a {@link Put} of the same row twice with different values at
 * different timestamps. In this case, the {@link TableState} will return the state of the table
 * before the batch; the second Put would <i>not</i> see the earlier Put applied. The reason for
 * this is that it is very hard to manage invalidating the local table state in case of failure and
 * maintaining a row cache across a batch (see {@link LocalTable} for more information on why). The
 * current implementation would manage the above case by possibly not realizing it needed to issue a
 * cleanup delete for the index row, leading to an invalid index as one of the updates wouldn't be
 * properly covered by a delete.
 * <p>
 * NOTE: This implementation doesn't cleanup the index when we remove a key-value on compaction or
 * flush, leading to a bloated index that needs to be cleaned up by a background process.
 */
public class PhoenixIndexBuilder extends BaseIndexBuilder {

  private static final Log LOG = LogFactory.getLog(PhoenixIndexBuilder.class);
  public static final String CODEC_CLASS_NAME_KEY = "com.salesforce.hbase.index.codec.class";

  protected RegionCoprocessorEnvironment env;
  private IndexCodec codec;
  protected LocalTable localTable;

  @Override
  public void setup(RegionCoprocessorEnvironment env) throws IOException {
    this.env = env;
    // setup the phoenix codec. Generally, this will just be in standard one, but abstracting here
    // so we can use it later when generalizing covered indexes
    Configuration conf = env.getConfiguration();
    Class<? extends IndexCodec> codecClass =
        conf.getClass(CODEC_CLASS_NAME_KEY, null, IndexCodec.class);
    try {
      Constructor<? extends IndexCodec> meth = codecClass.getDeclaredConstructor(new Class[0]);
      meth.setAccessible(true);
      this.codec = meth.newInstance();
      this.codec.initialize(env);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
    
    this.localTable = new LocalTable(env);
  }

  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdate(Put p) throws IOException {
    // build the index updates for each group
    List<Pair<Mutation, String>> updateMap = new ArrayList<Pair<Mutation, String>>();

    batchMutationAndAddUpdates(updateMap, p);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Found index updates for Put: " + updateMap);
    }

    return updateMap;
  }

  /**
   * Split the mutation into batches based on the timestamps of each keyvalue. We need to check each
   * key-value in the update to see if it matches the others. Generally, this will be the case, but
   * you can add kvs to a mutation that don't all have the timestamp, so we need to manage
   * everything in batches based on timestamp.
   * <p>
   * Adds all the updates in the {@link Mutation} to the state, as a side-effect.
   * @param updateMap index updates into which to add new updates. Modified as a side-effect.
   * @param state current state of the row for the mutation.
   * @param m mutation to batch
 * @throws IOException 
   */
  private void batchMutationAndAddUpdates(List<Pair<Mutation, String>> updateMap, Mutation m) throws IOException {
    // split the mutation into timestamp-based batches
    TreeMultimap<Long, KeyValue> batches = createTimestampBatchesFromMutation(m);

    // create a state manager, so we can manage each batch
    LocalTableState state = new LocalTableState(env, localTable, m);

    // go through each batch of keyvalues and build separate index entries for each
    for (Entry<Long, Collection<KeyValue>> batch : batches.asMap().entrySet()) {
      /*
       * We have to split the work between the cleanup and the update for each group because when we
       * update the current state of the row for the current batch (appending the mutations for the
       * current batch) the next group will see that as the current state, which will can cause the
       * a delete and a put to be created for the next group.
       */
      addMutationsForBatch(updateMap, batch, state);
    }
  }

  /**
   * Batch all the {@link KeyValue}s in a {@link Mutation} by timestamp. Updates any
   * {@link KeyValue} with a timestamp == {@link HConstants#LATEST_TIMESTAMP} to the timestamp at
   * the time the method is called.
   * @param m {@link Mutation} from which to extract the {@link KeyValue}s
   * @return map of timestamp to all the keyvalues with the same timestamp. the implict tree sorting
   *         in the returned ensures that batches (when iterating through the keys) will iterate the
   *         kvs in timestamp order
   */
  protected TreeMultimap<Long, KeyValue> createTimestampBatchesFromMutation(Mutation m) {
    TreeMultimap<Long, KeyValue> batches =
        TreeMultimap.create(Ordering.natural(), KeyValue.COMPARATOR);
    for (List<KeyValue> family : m.getFamilyMap().values()) {
      batches.putAll(createTimestampBatchesFromKeyValues(family));
    }
    return batches;
  }

  /**
   * Batch all the {@link KeyValue}s in a collection of kvs by timestamp. Updates any
   * {@link KeyValue} with a timestamp == {@link HConstants#LATEST_TIMESTAMP} to the timestamp at
   * the time the method is called.
   * @param kvs {@link KeyValue}s to break into batches
   * @return map of timestamp to all the keyvalues with the same timestamp. the implict tree sorting
   *         in the returned ensures that batches (when iterating through the keys) will iterate the
   *         kvs in timestamp order
   */
  protected TreeMultimap<Long, KeyValue> createTimestampBatchesFromKeyValues(
      Collection<KeyValue> kvs) {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    byte[] nowBytes = Bytes.toBytes(now);
    TreeMultimap<Long, KeyValue> batches =
        TreeMultimap.create(Ordering.natural(), KeyValue.COMPARATOR);

    // batch kvs by timestamp
    for (KeyValue kv : kvs) {
      long ts = kv.getTimestamp();
      // override the timestamp to the current time, so the index and primary tables match
      // all the keys with LATEST_TIMESTAMP will then be put into the same batch
      if (kv.updateLatestStamp(nowBytes)) {
        ts = now;
      }
      batches.put(ts, kv);
    }
    return batches;
  }

  /**
   * For a single batch, get all the index updates and add them to the updateMap
   * <p>
   * This method manages cleaning up the entire history of the row from the given timestamp forward
   * for out-of-order (e.g. 'back in time') updates.
   * <p>
   * If things arrive out of order (client is using custom timestamps) we should still see the index
   * in the correct order (assuming we scan after the out-of-order update in finished). Therefore,
   * we when we aren't the most recent update to the index, we need to delete the state at the
   * current timestamp (similar to above), but also issue a delete for the added index updates at
   * the next newest timestamp of any of the columns in the update; we need to cleanup the insert so
   * it looks like it was also deleted at that next newest timestamp. However, its not enough to
   * just update the one in front of us - that column will likely be applied to index entries up the
   * entire history in front of us, which also needs to be fixed up.
   * <p>
   * However, the current update usually will be the most recent thing to be added. In that case,
   * all we need to is issue a delete for the previous index row (the state of the row, without the
   * update applied) at the current timestamp. This gets rid of anything currently in the index for
   * the current state of the row (at the timestamp). Then we can just follow that by applying the
   * pending update and building the index update based on the new row state.
   * @param updateMap map to update with new index elements
   * @param batch timestamp-based batch of edits
   * @param state local state to update and pass to the codec
 * @throws IOException 
   */
  private void addMutationsForBatch(Collection<Pair<Mutation, String>> updateMap,
      Entry<Long, Collection<KeyValue>> batch, LocalTableState state) throws IOException {

    // do a single pass to figure out if we even need to fix up history.
    Iterable<IndexUpdate> upserts = getPendingIndexUpdates(state, batch);

    // timestamp of all the indexed columns is the most recent, so we are done (its all latest
    // timestmap updates)
    long minTs = ColumnTracker.NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP;
    for (IndexUpdate update : upserts) {
      ColumnTracker tracker = update.getIndexedColumns();
      if (minTs > tracker.getTS()) {
        minTs = tracker.getTS();
      }
      // TODO replace this as just storing a byte[], to avoid all the String <-> byte[] swapping
      // HBase does
      String table = Bytes.toString(update.getTableName());
      Put put = update.getUpdate();

      // get the column tracker so we can check the timestamps we need to complete again

      boolean needCleanup = tracker.getTS() < ColumnTracker.NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP;

      // only make the put if the index update has been setup
      if (put != null && table != null) {
        updateMap.add(new Pair<Mutation, String>(put, table));

        // only make the cleanup if we made a put and need cleanup
        if (needCleanup) {
          // there is a TS for the interested columns that is greater than the columns in the
          // put. Therefore, we need to issue a delete at the same timestamp
          Delete d = new Delete(put.getRow());
          d.setTimestamp(tracker.getTS());
          updateMap.add(new Pair<Mutation, String>(put, table));
        }
      }

      Queue<ColumnTrackerQueueElement> nextUpdateBatches = Queues.newSynchronousQueue(); // JT - to fix compiler error
      if (needCleanup) {
        // enque this tracker for another pass to for cleanup
        addTrackerToQueue(nextUpdateBatches, tracker);
      }
    }

    // roll up then entire history, with the pending update - this gets the correct index entries
    // and cleanup for the existing entries (with the update applied).

    // roll back the pending update. This is needed so we can remove all the 'old' index entries. We
    // don't need to do the puts here, but just the deletes at the given timestamps since we just
    // want to completely hide the incorrect entries.

    // cleanup the pending batch. If anything in the correct history is covered by Deletes used to
    // 'fix' history (same row key and ts), we just drop the delete (we don't want to drop both
    // because the update may have a different set of columns or value based on the update).

    // queue to manage the next step in history that we need to 'fix'
    Queue<ColumnTrackerQueueElement> nextUpdateBatches =
        new PriorityQueue<PhoenixIndexBuilder.ColumnTrackerQueueElement>();

    // start by getting the cleanup for the current state of the batch
    nextUpdateBatches.add(new ColumnTrackerQueueElement(batch.getKey()));

    // keep going while we don't need to roll back up the history
    while (!nextUpdateBatches.isEmpty()) {
      ColumnTrackerQueueElement elem = nextUpdateBatches.poll();
      long ts = elem.ts;
      state.setCurrentTimestamp(ts);
      // set the hints from the current element
      state.setHints(elem.trackers);
      // cleanup the current state of the table
      addDeleteUpdatesToMap(updateMap, state, ts);

      // ignore any index tracking from the delete
      state.resetTrackedColumns();

      // add the current batch to the map
      state.addPendingUpdates(batch.getValue());

      // get the updates to the current index
      // JT - rename from upserts to currentUpserts to fix compiler error
      Iterable<IndexUpdate> currentUpserts = codec.getIndexUpserts(state);
      state.resetTrackedColumns();

      for (IndexUpdate update : currentUpserts) {
        // TODO replace this as just storing a byte[], to avoid all the String <-> byte[] swapping
        // HBase does
        String table = Bytes.toString(update.getTableName());
        Put put = update.getUpdate();

        // get the column tracker so we can check the timestamps we need to complete again
        ColumnTracker tracker = update.getIndexedColumns();
        boolean needCleanup =
            tracker.getTS() < ColumnTracker.NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP;

        // only make the put if the index update has been setup
        if (put != null && table != null) {
          updateMap.add(new Pair<Mutation, String>(put, table));

          // only make the cleanup if we made a put and need cleanup
          if (needCleanup) {
            // there is a TS for the interested columns that is greater than the columns in the
            // put. Therefore, we need to issue a delete at the same timestamp
            Delete d = new Delete(put.getRow());
            d.setTimestamp(tracker.getTS());
            updateMap.add(new Pair<Mutation, String>(put, table));
          }
        }

        if (needCleanup) {
          // enque this tracker for another pass to for cleanup
          addTrackerToQueue(nextUpdateBatches, tracker);
        }
      }

    }
  }

  /**
   * @param state
   * @return
 * @throws IOException 
   */
  private Iterable<IndexUpdate> getPendingIndexUpdates(LocalTableState state,
      Entry<Long, Collection<KeyValue>> batch) throws IOException {
    // add the current batch to the map
    state.addPendingUpdates(batch.getValue());

    // get the updates to the current index
    Iterable<IndexUpdate> upserts = codec.getIndexUpserts(state);
    state.resetTrackedColumns();
    return upserts;
  }

  /**
   * @param nextUpdateBatches
   * @param tracker
   */
  private void addTrackerToQueue(Queue<ColumnTrackerQueueElement> nextUpdateBatches,
      ColumnTracker tracker) {

    // add the tracker based on the right batch
    ColumnTrackerQueueElement elem = new ColumnTrackerQueueElement(tracker.getTS());
    if (!nextUpdateBatches.contains(elem)) {
      nextUpdateBatches.add(elem);
    } else {
      // have to find the actual tracker from the batches
      for (ColumnTrackerQueueElement next : nextUpdateBatches) {
        if (next.equals(elem)) {
          elem = next;
          break;
        }
      }
    }
    elem.addTracker(tracker);
  }

  /**
   * Smaller wrapper around the timestamp of a bunch of {@link ColumnTracker}s.
   */
  class ColumnTrackerQueueElement implements Comparable<ColumnTrackerQueueElement> {
    private final long ts;
    private final List<ColumnTracker> trackers = new ArrayList<ColumnTracker>();

    public ColumnTrackerQueueElement(long ts) {
      this.ts = ts;
    }

    public void addTracker(ColumnTracker... tracker) {
      if (tracker == null) {
        return;
      }
      this.trackers.addAll(Arrays.asList(tracker));
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof ColumnTrackerQueueElement)) {
        return false;
      }
      ColumnTrackerQueueElement other = (ColumnTrackerQueueElement) o;
      return other.ts == this.ts;
    }

    @Override
    public int compareTo(ColumnTrackerQueueElement o) {
      return Longs.compare(ts, o.ts);
    }
  }

  /**
   * Get the index deletes from the codec (IndexCodec{@link #getIndexDeletes(TableState)} and then add them to the update map.
   */
  protected void addDeleteUpdatesToMap(Collection<Pair<Mutation, String>> updateMap,
      LocalTableState state, long ts) {
    Iterable<Pair<Delete, byte[]>> cleanup = codec.getIndexDeletes(state);
    if (cleanup != null) {
      for (Pair<Delete, byte[]> d : cleanup) {
        // override the timestamps in the delete to match the current batch.
        Delete remove = d.getFirst();
        remove.setTimestamp(ts);
        // TODO replace this as just storing a byte[], to avoid all the String <-> byte[] swapping
        // HBase does
        String table = Bytes.toString(d.getSecond());
        updateMap.add(new Pair<Mutation, String>(remove, table));
      }
    }
  }

  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdate(Delete d) throws IOException {
    // stores all the return values
    List<Pair<Mutation, String>> updateMap = new ArrayList<Pair<Mutation, String>>();

    // We have to figure out which kind of delete it is, since we need to do different things if its
    // a general (row) delete, versus a delete of just a single column or family
    Map<byte[], List<KeyValue>> families = d.getFamilyMap();

    /*
     * Option 1: its a row delete marker, so we just need to delete the most recent state for each
     * group, as of the specified timestamp in the delete. This can happen if we have a single row
     * update and it is part of a batch mutation (prepare doesn't happen until later... maybe a
     * bug?). In a single delete, this delete gets all the column families appended, so the family
     * map won't be empty by the time it gets here.
     */
    if (families.size() == 0) {
      LocalTableState state = new LocalTableState(env, localTable, d);
      // get a consistent view of name
      long now = d.getTimeStamp();
      if (now == HConstants.LATEST_TIMESTAMP) {
        now = EnvironmentEdgeManager.currentTimeMillis();
        // update the delete's idea of 'now' to be consistent with the index
        d.setTimestamp(now);
      }
      // get deletes from the codec
      // we only need to get deletes and not add puts because this delete covers all columns
      addDeleteUpdatesToMap(updateMap, state, now);

      /*
       * Update the current state for all the kvs in the delete. Generally, we would just iterate
       * the family map, but since we go here, the family map is empty! Therefore, we need to fake a
       * bunch of family deletes (just like hos HRegion#prepareDelete works). This is just needed
       * for current version of HBase that has an issue where the batch update doesn't update the
       * deletes before calling the hook.
       */
      byte[] deleteRow = d.getRow();
      for (byte[] family : this.env.getRegion().getTableDesc().getFamiliesKeys()) {
        state.addPendingUpdates(new KeyValue(deleteRow, family, null, now,
            KeyValue.Type.DeleteFamily));
      }
    } else {
      // Option 2: Its actually a bunch single updates, which can have different timestamps.
      // Therefore, we need to do something similar to the put case and batch by timestamp
      batchMutationAndAddUpdates(updateMap, d);
    }

    return updateMap;
  }

  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdateForFilteredRows(
      Collection<KeyValue> filtered) throws IOException {
    // TODO Implement IndexBuilder.getIndexUpdateForFilteredRows
    return null;
  }

  /**
   * Exposed for testing!
   * @param codec codec to use for this instance of the builder
   */
  public void setIndexCodecForTesting(IndexCodec codec) {
    this.codec = codec;
  }
}