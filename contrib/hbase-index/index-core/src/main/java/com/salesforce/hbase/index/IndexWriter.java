package com.salesforce.hbase.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
   * Internally, uses {@link #wri to make the write and if is receives a {
   * @link CannotReachIndexException}, it attempts to move (
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
    // conver the strings to htableinterfaces to which we can talk
    Collection<Pair<Mutation, HTableInterfaceReference>> toWrite =
        resolveTableReferences(factory, updates);

    // write each mutation, as a part of a batch, to its respective table
    List<Mutation> singleMutation = new ArrayList<Mutation>(1);
    Set<HTableInterface> tables = new HashSet<HTableInterface>();
    for (Pair<Mutation, HTableInterfaceReference> entry : toWrite) {
      // do the put into the index table
      singleMutation.add(entry.getFirst());
      LOG.info("Writing index update:" + entry.getFirst() + " to table: "
          + entry.getSecond().getTableName());
      try {
        HTableInterface table;

        if (factory == null) {
          table = entry.getSecond().getTable();
        } else {
          table = entry.getSecond().getTable(factory);
        }
        // do the update
        table.batch(singleMutation);
        tables.add(table);
      } catch (IOException e) {
        throw new CannotReachIndexException(entry.getSecond().getTableName(), entry.getFirst(), e);
      } catch (InterruptedException e) {
        throw new CannotReachIndexException(entry.getSecond().getTableName(), entry.getFirst(), e);
      }
      singleMutation.clear();
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
  public static Collection<Pair<Mutation, HTableInterfaceReference>> resolveTableReferences(
      HTableFactory factory, Collection<Pair<Mutation, String>> indexUpdates) {

    Collection<Pair<Mutation, HTableInterfaceReference>> updates =
        new ArrayList<Pair<Mutation, HTableInterfaceReference>>(indexUpdates.size());
    Map<String, HTableInterfaceReference> tables =
        new HashMap<String, HTableInterfaceReference>(updates.size());
    for (Pair<Mutation, String> entry : indexUpdates) {
      String tableName = entry.getSecond();
      HTableInterfaceReference table = tables.get(tableName);
      if (table == null) {
        table = new HTableInterfaceReference(entry.getSecond(), factory);
        tables.put(tableName, table);
      }
      updates.add(new Pair<Mutation, HTableInterfaceReference>(entry.getFirst(), table));
    }

    return updates;
  }
}
