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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.Lists;
import com.salesforce.hbase.index.builder.BaseIndexBuilder;
import com.salesforce.hbase.index.builder.covered.CoveredColumnIndexer;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.PTableImpl;
import com.salesforce.phoenix.util.IndexUtil;

/**
 * Build covered indexes for phoenix updates.
 * <p>
 * This is a very simple mechanism that doesn't do covered indexes (as in
 * {@link CoveredColumnIndexer}), but just serves as a starting point for implementing comprehensive
 * indexing in phoenix.
 * <p>
 * NOTE: This implementation doesn't cleanup the index when we remove a key-value on compaction or
 * flush, leading to a bloated index that needs to be cleaned up by a background process.
 */
public class PhoenixIndexBuilder extends BaseIndexBuilder {

  private static final Log LOG = LogFactory.getLog(PhoenixIndexBuilder.class);
  private static final String PTABLE_ATTRIBUTE_KEY = "phoenix.ptable.primary";
  private static final String INDEX_PTABLE_ATTRIBUTE_KEY = "phoenix.ptable.index";


  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdate(Put p) throws IOException {
    return getIndexUpdatesForMutation(p);
  }


  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdate(Delete d) throws IOException {
    return getIndexUpdatesForMutation(d);
  }
  
  private Collection<Pair<Mutation, String>> getIndexUpdatesForMutation(Mutation m)
      throws IOException {
    // deserialize the ptable for the put
    byte[] serializedPtable = m.getAttribute(PTABLE_ATTRIBUTE_KEY);
    PTable table = new PTableImpl();
    table.readFields(new DataInputStream(new ByteArrayInputStream(serializedPtable)));
    // read in the index table
    serializedPtable = m.getAttribute(INDEX_PTABLE_ATTRIBUTE_KEY);
    PTable indexTable = new PTableImpl();
    indexTable.readFields(new DataInputStream(new ByteArrayInputStream(serializedPtable)));

    //generate the index updates 
    try {
      List<Mutation> mutations =
          IndexUtil.generateIndexData(table, indexTable, Lists.<Mutation> newArrayList(m));
      List<Pair<Mutation, String>> updates =
          new ArrayList<Pair<Mutation, String>>(mutations.size());
      // XXX is this right? Seems a bit convoluted for the real table name...
      String indexTableName = Bytes.toString(indexTable.getName().getBytes());
      for (Mutation update : mutations) {
        updates.add(new Pair<Mutation, String>(update, indexTableName));
      }
      return updates;

    } catch (SQLException e) {
      throw new IOException("Failed to build index updates from update!", e);
    }
  }

  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdateForFilteredRows(
      Collection<KeyValue> filtered) throws IOException {
    // TODO Implement IndexBuilder.getIndexUpdateForFilteredRows
    return null;
  }
}