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
package com.salesforce.hbase.index;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.wal.IndexedWALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.salesforce.hbase.index.table.HTableFactory;
import com.salesforce.hbase.index.table.HTableInterfaceReference;

/**
 * Do the actual work of writing to the index tables. Ensures that if we do fail to write to the
 * index table that we cleanly kill the region/server to ensure that the region's WAL gets replayed.
 */
public class IndexWriter {

  private static final Log LOG = LogFactory.getLog(IndexWriter.class);

  private final String sourceInfo;
  private final Abortable abortable;
  private final HTableFactory factory;

  /**
   * @param sourceInfo log info string about where we are writing from
   * @param abortable to notify in the case of failure
   * @param factory Factory to use when resolving the {@link HTableInterfaceReference}. If
   *          <tt>null</tt>, its assumed that the {@link HTableInterfaceReference} already has its
   *          factory set (e.g. by {@link HTableInterfaceReference#setFactory(HTableFactory)} - if
   *          its not already set, a {@link NullPointerException} is thrown.
   */
  public IndexWriter(String sourceInfo, Abortable abortable, HTableFactory factory) {
    this.sourceInfo = sourceInfo;
    this.abortable = abortable;
    this.factory = factory;
  }

  /**
   * Just write the index update portions of of the edit, if it is an {@link IndexedWALEdit}. If it
   * is not passed an {@link IndexedWALEdit}, any further actions are ignored.
   * <p>
   * Internally, uses {@link #write(Collection)} to make the write and if is receives a
   * {@link CannotReachIndexException}, it attempts to move (
   * {@link HBaseAdmin#unassign(byte[], boolean)}) the region and then failing that calls
   * {@link System#exit(int)} to kill the server.
   */
  public void writeAndKillYourselfOnFailure(Collection<Pair<Mutation, String>> indexUpdates) {
    try {
      write(indexUpdates);
    } catch (Exception e) {
      killYourself(e);
    }
  }
  /**
   * Write the mutations to their respective table using the provided factory.
   * <p>
   * This method is not thread-safe and if accessed in a non-serial manner could leak HTables.
   * @param updates Updates to write
   * @throws CannotReachIndexException if we cannot successfully write a single index entry. We stop
   *           immediately on the first failed index write, rather than attempting all writes.
   */
  public void write(Collection<Pair<Mutation, String>> updates)
      throws CannotReachIndexException {
    // convert the strings to htableinterfaces to which we can talk and group by TABLE
    Multimap<HTableInterfaceReference, Mutation> toWrite =
        resolveTableReferences(factory, updates);

    // write each mutation, as a part of a batch, to its respective table
    List<Mutation> mutations;
    Set<HTableInterface> tables = new HashSet<HTableInterface>();
    for (Entry<HTableInterfaceReference, Collection<Mutation>> entry : toWrite.asMap().entrySet()) {
      // get the mutations for each table. We leak the implementation here a little bit to save
      // doing a complete copy over of all the index update for each table.
      mutations = (List<Mutation>) entry.getValue();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Writing index update:" + mutations + " to table: "
            + entry.getKey().getTableName());
      }
      try {
        HTableInterface table;

        if (factory == null) {
          table = entry.getKey().getTable();
        } else {
          table = entry.getKey().getTable(factory);
        }
        table.batch(mutations);
        tables.add(table);
      } catch (IOException e) {
        throw new CannotReachIndexException(entry.getKey().getTableName(), mutations, e);
      } catch (InterruptedException e) {
        throw new CannotReachIndexException(entry.getKey().getTableName(), mutations, e);
      }
    }
    // go through each reference and close the connection
    // we can't do this earlier as we may reuse table references between different index entries,
    // which would prematurely close a table before we could write the later update
    for (HTableInterface table : tables) {
      try {
        table.close();
      } catch (IOException e) {
        LOG.error("Failed to close connection to table:" + Bytes.toString(table.getTableName()), e);
      }
    }

    LOG.info("Done writing all index updates");
  }

  /**
   * @param logEdit edit for which we need to kill ourselves
   * @param info region from which we are attempting to write the log
   */
  private void killYourself(Throwable cause) {
    String msg = "Could not update the index table, killing server region from: " + this.sourceInfo;
    LOG.error(msg);
    try {
      this.abortable.abort(msg, cause);
    } catch (Exception e) {
      LOG.fatal("Couldn't abort this server to preserve index writes, attempting to hard kill the server from"
          + this.sourceInfo);
      System.exit(1);
    }
  }

  /**
   * Convert the passed index updates to {@link HTableInterfaceReference}s.
   * @param factory factory to use when resolving the table references.
   * @param indexUpdates from the index builder
   * @return pairs that can then be written by an {@link IndexWriter}.
   */
  public static Multimap<HTableInterfaceReference, Mutation> resolveTableReferences(
      HTableFactory factory, Collection<Pair<Mutation, String>> indexUpdates) {
    Multimap<HTableInterfaceReference, Mutation> updates =
        ArrayListMultimap.<HTableInterfaceReference, Mutation> create();
    Map<String, HTableInterfaceReference> tables =
        new HashMap<String, HTableInterfaceReference>(updates.size());
    for (Pair<Mutation, String> entry : indexUpdates) {
      String tableName = entry.getSecond();
      HTableInterfaceReference table = tables.get(tableName);
      if (table == null) {
        table = new HTableInterfaceReference(entry.getSecond(), factory);
        tables.put(tableName, table);
      }
      updates.put(table, entry.getFirst());
    }

    return updates;
  }
}
