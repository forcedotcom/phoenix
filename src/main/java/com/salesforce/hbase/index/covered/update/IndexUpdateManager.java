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
package com.salesforce.hbase.index.covered.update;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;

/**
 * Keeps track of the index updates
 */
public class IndexUpdateManager {

  public Comparator<Mutation> COMPARATOR = new MutationComparator();
  class MutationComparator implements Comparator<Mutation> {

    @Override
    public int compare(Mutation o1, Mutation o2) {
      // always sort rows first
      int compare = o1.compareTo(o2);
      if (compare != 0) {
        return compare;
      }

      // if same row, sort by reverse timestamp (larger first)
      compare = Longs.compare(o2.getTimeStamp(), o1.getTimeStamp());
      if (compare != 0) {
        return compare;
      }
      // deletes always sort before puts for the same row
      if (o1 instanceof Delete) {
        // same row, same ts == same delete since we only delete rows
        if (o2 instanceof Delete) {
          return 0;
        } else {
          // o2 has to be a put
          return -1;
        }
      }
      // o1 must be a put
      if (o2 instanceof Delete) {
        return 1;
      } else if (o2 instanceof Put) {
        return comparePuts((Put) o1, (Put) o2);
      }

      throw new RuntimeException(
          "Got unexpected mutation types! Can only be Put or Delete, but got: " + o1 + ", and "
              + o2);
    }

    private int comparePuts(Put p1, Put p2) {
      int p1Size = p1.size();
      int p2Size = p2.size();
      int compare = p1Size - p2Size;
      if (compare == 0) {
        // TODO: make this a real comparison
        // this is a little cheating, but we don't really need to worry too much about this being
        // the same - chances are that exact matches here are really the same update.
        return Longs.compare(p1.heapSize(), p2.heapSize());
      }
      return compare;
    }

  }

  private static final String PHOENIX_HBASE_TEMP_DELETE_MARKER = "phoenix.hbase.temp.delete.marker";
  private static final byte[] TRUE_MARKER = new byte[] { 1 };

  protected final Map<ImmutableBytesPtr, Collection<Mutation>> map =
      new HashMap<ImmutableBytesPtr, Collection<Mutation>>();

  /**
   * Add an index update. Keeps the latest {@link Put} for a given timestamp
   * @param tableName
   * @param m
   */
  public void addIndexUpdate(byte[] tableName, Mutation m) {
    // we only keep the most recent update
    ImmutableBytesPtr key = new ImmutableBytesPtr(tableName);
    Collection<Mutation> updates = map.get(key);
    if (updates == null) {
      updates = new SortedCollection<Mutation>(COMPARATOR);
      map.put(key, updates);
    }
    fixUpCurrentUpdates(updates, m);
  }

  /**
   * Fix up the current updates, given the pending mutation.
   * @param updates current updates
   * @param pendingMutation
   */
  protected void fixUpCurrentUpdates(Collection<Mutation> updates, Mutation pendingMutation) {
    // need to check for each entry to see if we have a duplicate
    Mutation toRemove = null;
    Delete pendingDelete = pendingMutation instanceof Delete ? (Delete) pendingMutation : null;
    boolean sawRowMatch = false;
    for (Mutation stored : updates) {
      int compare = pendingMutation.compareTo(stored);
      // skip to the right row
      if (compare < 0) {
        continue;
      } else if (compare > 0) {
        if (sawRowMatch) {
          break;
        }
        continue;
      }

      // set that we saw a row match, so any greater row will necessarily be the wrong
      sawRowMatch = true;

      // skip until we hit the right timestamp
      if (stored.getTimeStamp() < pendingMutation.getTimeStamp()) {
        continue;
      }

      if (stored instanceof Delete) {
        // we already have a delete for this row, so we are done.
        if (pendingDelete != null) {
          return;
        }
        // pending update must be a Put, so we ignore the Put.
        // add a marker in the this delete that it has been canceled out already. We need to keep
        // the delete around though so we can figure out if other Puts would also be canceled out.
        markMutationForRemoval(stored);
        return;
      }

      // otherwise, the stored mutation is a Put. Either way, we want to remove it. If the pending
      // update is a delete, we need to remove the entry (no longer applies - covered by the
      // delete), or its an older version of the row, so we cover it with the newer.
      toRemove = stored;
      if (pendingDelete != null) {
        // the pending mutation, but we need to mark the mutation for removal later
        markMutationForRemoval(pendingMutation);
        break;
      }
    }
    
    updates.remove(toRemove);
    updates.add(pendingMutation);
  }

  private void markMutationForRemoval(Mutation m) {
    m.setAttribute(PHOENIX_HBASE_TEMP_DELETE_MARKER, TRUE_MARKER);
  }

  public List<Pair<Mutation, byte[]>> toMap() {
    List<Pair<Mutation, byte[]>> updateMap = Lists.newArrayList();
    for (Entry<ImmutableBytesPtr, Collection<Mutation>> updates : map.entrySet()) {
      // get is ok because we always set with just the bytes
      byte[] tableName = updates.getKey().get();
      // TODO replace this as just storing a byte[], to avoid all the String <-> byte[] swapping
      // HBase does
      for (Mutation m : updates.getValue()) {
        // skip elements that have been marked for delete
        if (shouldBeRemoved(m)) {
          continue;
        }
        updateMap.add(new Pair<Mutation, byte[]>(m, tableName));
      }
    }
    return updateMap;
  }

  /**
   * @param updates
   */
  public void addAll(Collection<Pair<Mutation, String>> updates) {
    for (Pair<Mutation, String> update : updates) {
      addIndexUpdate(Bytes.toBytes(update.getSecond()), update.getFirst());
    }
  }

  private boolean shouldBeRemoved(Mutation m) {
    return m.getAttribute(PHOENIX_HBASE_TEMP_DELETE_MARKER) != null;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer("Pending Index Updates:\n");
    for (Entry<ImmutableBytesPtr, Collection<Mutation>> entry : map.entrySet()) {
      String tableName = Bytes.toString(entry.getKey().get());
      sb.append("   Table: '" + tableName + "'\n");
      for (Mutation m : entry.getValue()) {
        sb.append("\t");
        if (shouldBeRemoved(m)) {
          sb.append("[REMOVED]");
        }
        sb.append(m.getClass().getSimpleName() + ":"
            + ((m instanceof Put) ? m.getTimeStamp() + " " : ""));
        sb.append(" row=" + Bytes.toString(m.getRow()));
        sb.append("\n");
        if (m.getFamilyMap().isEmpty()) {
          sb.append("\t\t=== EMPTY ===\n");
        }
        for (List<KeyValue> kvs : m.getFamilyMap().values()) {
          for (KeyValue kv : kvs) {
            sb.append("\t\t" + kv.toString() + "/value=" + Bytes.toStringBinary(kv.getValue()));
            sb.append("\n");
          }
        }
      }
    }
    return sb.toString();
  }
}