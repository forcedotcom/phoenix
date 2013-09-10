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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;

import com.salesforce.phoenix.index.IndexUpdateManager;

public class TestIndexUpdateManager {

  private static final byte[] row = Bytes.toBytes("row");
  private static final String TABLE_NAME = "table";
  private static final byte[] table = Bytes.toBytes(TABLE_NAME);

  @Test
  public void testMutationComparator() throws Exception {
    IndexUpdateManager manager = new IndexUpdateManager();
    Comparator<Mutation> comparator = manager.COMPARATOR;
    Put p = new Put(row, 10);
    // lexigraphically earlier should sort earlier
    Put p1 = new Put(Bytes.toBytes("ro"), 10);
    assertTrue("lexigraphically later sorting first, should be earlier first.",
      comparator.compare(p, p1) > 0);
    p1 = new Put(Bytes.toBytes("row1"), 10);
    assertTrue("lexigraphically later sorting first, should be earlier first.",
      comparator.compare(p1, p) > 0);

    // larger ts sorts before smaller, for the same row
    p1 = new Put(row, 11);
    assertTrue("Smaller timestamp sorting first, should be larger first.",
      comparator.compare(p, p1) > 0);
    // still true, even for deletes
    Delete d = new Delete(row, 11);
    assertTrue("Smaller timestamp sorting first, should be larger first.",
      comparator.compare(p, d) > 0);

    // for the same row, t1, the delete should sort earlier
    d = new Delete(row, 10);
    assertTrue("Delete doesn't sort before put, for the same row and ts",
      comparator.compare(p, d) > 0);

    // but for different rows, we still respect the row sorting.
    d = new Delete(Bytes.toBytes("row1"), 10);
    assertTrue("Delete doesn't sort before put, for the same row and ts",
      comparator.compare(p, d) < 0);
  }

  /**
   * When making updates we need to cancel out {@link Delete} and {@link Put}s for the same row.
   * @throws Exception on failure
   */
  @Test
  public void testCancelingUpdates() throws Exception {
    IndexUpdateManager manager = new IndexUpdateManager();

    long ts1 = 10, ts2 = 11;
    // at different timestamps, so both should be retained
    Delete d = new Delete(row, ts1);
    Put p = new Put(row, ts2);
    manager.addIndexUpdate(table, d);
    manager.addIndexUpdate(table, p);
    List<Mutation> pending = new ArrayList<Mutation>();
    pending.add(p);
    pending.add(d);
    validate(manager, pending);

    // add a delete that should cancel out the put, leading to only one delete remaining
    Delete d2 = new Delete(row, ts2);
    manager.addIndexUpdate(table, d2);
    pending.add(d);
    validate(manager, pending);

    // double-deletes of the same row only retain the existing one, which was already canceled out
    // above
    Delete d3 = new Delete(row, ts2);
    manager.addIndexUpdate(table, d3);
    pending.add(d);
    validate(manager, pending);

    // if there is just a put and a delete at the same ts, no pending updates should be returned
    manager = new IndexUpdateManager();
    manager.addIndexUpdate(table, d2);
    manager.addIndexUpdate(table, p);
    validate(manager, Collections.<Mutation> emptyList());

    // different row insertions can be tricky too, if you don't get the base cases right
    manager = new IndexUpdateManager();
    manager.addIndexUpdate(table, p);
    // this row definitely sorts after the current row
    byte[] row1 = Bytes.toBytes("row1");
    Put p1 = new Put(row1, ts1);
    manager.addIndexUpdate(table, p1);
    // this delete should completely cover the given put and both should be removed
    Delete d4 = new Delete(row1, ts1);
    manager.addIndexUpdate(table, d4);
    pending.clear();
    pending.add(p);
    validate(manager, pending);
  }

  private void validate(IndexUpdateManager manager, List<Mutation> pending) {
    for (Pair<Mutation, String> entry : manager.toMap()) {
      assertEquals("Table name didn't match for stored entry!", TABLE_NAME, entry.getSecond());
      Mutation m = pending.remove(0);
      // test with == to match the exact entries, Mutation.equals just checks the row
      assertTrue(
        "Didn't get the expected mutation! Expected: " + m + ", but got: " + entry.getFirst(),
        m == entry.getFirst());
    }
    assertTrue("Missing pending updates: " + pending, pending.isEmpty());
  }
}