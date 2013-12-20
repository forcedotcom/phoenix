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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.HRegion;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.salesforce.hbase.index.table.HTableInterfaceReference;


public class PerRegionIndexWriteCache {

  private final Map<HRegion, Multimap<HTableInterfaceReference, Mutation>> cache =
      new HashMap<HRegion, Multimap<HTableInterfaceReference, Mutation>>();


  /**
   * Get the edits for the current region. Removes the edits from the cache. To add them back, call
   * {@link #addEdits(HRegion, HTableInterfaceReference, Collection)}.
   * @param region
   * @return Get the edits for the given region. Returns <tt>null</tt> if there are no pending edits
   *         for the region
   */
  public Multimap<HTableInterfaceReference, Mutation> getEdits(HRegion region) {
    return cache.remove(region);
  }

  /**
   * @param region
   * @param table
   * @param collection
   */
  public void addEdits(HRegion region, HTableInterfaceReference table,
      Collection<Mutation> collection) {
    Multimap<HTableInterfaceReference, Mutation> edits = cache.get(region);
    if (edits == null) {
      edits = ArrayListMultimap.<HTableInterfaceReference, Mutation> create();
      cache.put(region, edits);
    }
    edits.putAll(table, collection);
  }
}