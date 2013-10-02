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
package com.salesforce.hbase.index.covered.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import com.salesforce.hbase.index.covered.update.ColumnReference;
import com.salesforce.hbase.index.util.IndexManagementUtil;

/**
 * Wrapper around a lazily instantiated, local HTable.
 * <p>
 * Previously, we had used various row and batch caches. However, this ends up being very
 * complicated when attempting manage updating and invalidating the cache with no real gain as any
 * row accessed multiple times will likely be in HBase's block cache, invalidating any extra caching
 * we are doing here. In the end, its simpler and about as efficient to just get the current state
 * of the row from HBase and let HBase manage caching the row from disk on its own.
 */
public class LocalTable implements LocalHBaseState {

  private RegionCoprocessorEnvironment env;

  public LocalTable(RegionCoprocessorEnvironment env) {
    this.env = env;
  }

  @Override
  public Result getCurrentRowState(Mutation m, Collection<? extends ColumnReference> columns)
      throws IOException {
    byte[] row = m.getRow();
    // need to use a scan here so we can get raw state, which Get doesn't provide.
    Scan s = IndexManagementUtil.newLocalStateScan(Collections.singletonList(columns));
    s.setStartRow(row);
    s.setStopRow(row); // FIXME: isn't this non inclusive?
    HRegion region = this.env.getRegion();
    RegionScanner scanner = region.getScanner(s);
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    boolean more = scanner.next(kvs);
    assert !more : "Got more than one result when scanning" + " a single row in the primary table!";

    Result r = new Result(kvs);
    scanner.close();
    return r;
  }
}