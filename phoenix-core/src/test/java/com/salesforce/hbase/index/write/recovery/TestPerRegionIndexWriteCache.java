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
package com.salesforce.hbase.index.write.recovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.salesforce.hbase.index.table.HTableInterfaceReference;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.hbase.index.write.recovery.PerRegionIndexWriteCache;

public class TestPerRegionIndexWriteCache {

  private static final byte[] row = Bytes.toBytes("row");
  private static final byte[] family = Bytes.toBytes("family");
  private static final byte[] qual = Bytes.toBytes("qual");
  private static final byte[] val = Bytes.toBytes("val");

  Put p = new Put(row);
  Put p2 = new Put(Bytes.toBytes("other row"));
  {
    p.add(family, qual, val);
    p2.add(family, qual, val);
  }


  HRegion r1 = new HRegion() {
    @Override
    public int hashCode() {
      return 1;
    }

    @Override
    public String toString() {
      return "testRegion1";
    }
  };
  HRegion r2 = new HRegion() {
    @Override
    public int hashCode() {
      return 2;
    }

    @Override
    public String toString() {
      return "testRegion1";
    }
  };

  @Test
  public void testAddRemoveSingleRegion() {
    PerRegionIndexWriteCache cache = new PerRegionIndexWriteCache();
    HTableInterfaceReference t1 = new HTableInterfaceReference(new ImmutableBytesPtr(Bytes.toBytes("t1")));
    List<Mutation> mutations = new ArrayList<Mutation>();
    mutations.add(p);
    cache.addEdits(r1, t1, mutations);
    Multimap<HTableInterfaceReference, Mutation> edits = cache.getEdits(r1);
    Set<Entry<HTableInterfaceReference, Collection<Mutation>>> entries = edits.asMap().entrySet();
    assertEquals("Got more than one table in the the edit map!", 1, entries.size());
    for (Entry<HTableInterfaceReference, Collection<Mutation>> entry : entries) {
     //ensure that we are still storing a list here - otherwise it breaks the parallel writer implementation
      final List<Mutation> stored = (List<Mutation>) entry.getValue();
      assertEquals("Got an unexpected amount of mutations in the entry", 1, stored.size());
      assertEquals("Got an unexpected mutation in the entry", p, stored.get(0));
    }

    // ensure that a second get doesn't have any more edits. This ensures that we don't keep
    // references around to these edits and have a memory leak
    assertNull("Got an entry for a region we removed", cache.getEdits(r1));
  }

  @Test
  public void testMultipleAddsForSingleRegion() {
    PerRegionIndexWriteCache cache = new PerRegionIndexWriteCache();
    HTableInterfaceReference t1 =
        new HTableInterfaceReference(new ImmutableBytesPtr(Bytes.toBytes("t1")));
    List<Mutation> mutations = Lists.<Mutation> newArrayList(p);
    cache.addEdits(r1, t1, mutations);

    // add a second set
    mutations = Lists.<Mutation> newArrayList(p2);
    cache.addEdits(r1, t1, mutations);

    Multimap<HTableInterfaceReference, Mutation> edits = cache.getEdits(r1);
    Set<Entry<HTableInterfaceReference, Collection<Mutation>>> entries = edits.asMap().entrySet();
    assertEquals("Got more than one table in the the edit map!", 1, entries.size());
    for (Entry<HTableInterfaceReference, Collection<Mutation>> entry : entries) {
      // ensure that we are still storing a list here - otherwise it breaks the parallel writer
      // implementation
      final List<Mutation> stored = (List<Mutation>) entry.getValue();
      assertEquals("Got an unexpected amount of mutations in the entry", 2, stored.size());
      assertEquals("Got an unexpected mutation in the entry", p, stored.get(0));
      assertEquals("Got an unexpected mutation in the entry", p2, stored.get(1));
    }
  }

  @Test
  public void testMultipleRegions() {
    PerRegionIndexWriteCache cache = new PerRegionIndexWriteCache();
    HTableInterfaceReference t1 =
        new HTableInterfaceReference(new ImmutableBytesPtr(Bytes.toBytes("t1")));
    List<Mutation> mutations = Lists.<Mutation> newArrayList(p);
    List<Mutation> m2 = Lists.<Mutation> newArrayList(p2);
    // add each region
    cache.addEdits(r1, t1, mutations);
    cache.addEdits(r2, t1, m2);

    // check region1
    Multimap<HTableInterfaceReference, Mutation> edits = cache.getEdits(r1);
    Set<Entry<HTableInterfaceReference, Collection<Mutation>>> entries = edits.asMap().entrySet();
    assertEquals("Got more than one table in the the edit map!", 1, entries.size());
    for (Entry<HTableInterfaceReference, Collection<Mutation>> entry : entries) {
      // ensure that we are still storing a list here - otherwise it breaks the parallel writer
      // implementation
      final List<Mutation> stored = (List<Mutation>) entry.getValue();
      assertEquals("Got an unexpected amount of mutations in the entry for region1", 1,
        stored.size());
      assertEquals("Got an unexpected mutation in the entry for region2", p, stored.get(0));
    }

    // check region2
    edits = cache.getEdits(r2);
    entries = edits.asMap().entrySet();
    assertEquals("Got more than one table in the the edit map!", 1, entries.size());
    for (Entry<HTableInterfaceReference, Collection<Mutation>> entry : entries) {
      // ensure that we are still storing a list here - otherwise it breaks the parallel writer
      // implementation
      final List<Mutation> stored = (List<Mutation>) entry.getValue();
      assertEquals("Got an unexpected amount of mutations in the entry for region2", 1,
        stored.size());
      assertEquals("Got an unexpected mutation in the entry for region2", p2, stored.get(0));
    }


    // ensure that a second get doesn't have any more edits. This ensures that we don't keep
    // references around to these edits and have a memory leak
    assertNull("Got an entry for a region we removed", cache.getEdits(r1));
  }
}