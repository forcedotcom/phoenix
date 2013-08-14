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
package com.salesforce.hbase.index.builder.covered;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

/**
 * Wrapper around a lazily instantiated, local HTable.
 * <p>
 * Previously, we had used various row and batch caches. However, this ends up being very
 * complicated when attempting manage updating and invalidating the cache with no real gain as any
 * row accessed multiple times will likely be in HBase's block cache, invalidating any extra caching
 * we are doing here. In the end, its simpler and about as efficient to just get the current state
 * of the row from HBase and let HBase manage caching the row from disk on its own.
 */
public class LocalTable {

  private volatile HTableInterface localTable;
  private RegionCoprocessorEnvironment env;

  public LocalTable(RegionCoprocessorEnvironment env) {
    this.env = env;
  }

  /**
   * @param m mutation for which we should get the current table state
   * @return the full state of the given row. Includes all current versions (even if they are not
   *         usually visible to the client (unless they are also doing a raw scan)). Never returns a
   *         <tt>null</tt> {@link Result} - instead, when there is not data for the row, returns a
   *         {@link Result} with no stored {@link KeyValue}s.
   * @throws IOException if there is an issue reading the row
   */
  public Result getCurrentRowState(Mutation m) throws IOException {
    byte[] row = m.getRow();
    // need to use a scan here so we can get raw state, which Get doesn't provide.
    Scan s = new Scan(row, row);
    s.setRaw(true);
    s.setMaxVersions();
    ResultScanner results = getLocalTable().getScanner(s);
    Result r = results.next();
    assert results.next() == null : "Got more than one result when scanning"
        + " a single row in the primary table!";
    results.close();
    return r == null ? new Result() : r;
  }

  /**
   * Ensure we have a connection to the local table. We need to do this after
   * {@link #setup(RegionCoprocessorEnvironment)} because we are created on region startup and the
   * table isn't actually accessible until later.
   * @throws IOException if we can't reach the table
   */
  private HTableInterface getLocalTable() throws IOException {
    if (this.localTable == null) {
      synchronized (this) {
        if (this.localTable == null) {
          localTable = env.getTable(env.getRegion().getTableDesc().getName());
        }
      }
    }
    return this.localTable;
  }
}