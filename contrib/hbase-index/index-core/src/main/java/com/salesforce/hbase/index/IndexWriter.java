package com.salesforce.hbase.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

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

  public IndexWriter(String sourceInfo, Abortable abortable) {
    this.sourceInfo = sourceInfo;
    this.abortable = abortable;
  }

  /**
   * Just write the index update portions of of the edit, if it is an {@link IndexedWALEdit}. If it
   * is not passed an {@link IndexedWALEdit}, any further actions are ignored.
   * <p>
   * Internally, uses {@link #write(HRegionInfo, WALEdit)} to make the write and if is receives a
   * {@link CannotReachIndexException}, it attempts to move (
   * {@link HBaseAdmin#unassign(byte[], boolean)}) the region and then failing that calls
   * {@link System#exit(int)} to kill the server.
   * @param factory Factory to use when resolving the {@link HTableInterfaceReference}. If
   *          <tt>null</tt>, its assumed that the {@link HTableInterfaceReference} already has its
   *          factory set (e.g. by {@link HTableInterfaceReference#setFactory(HTableFactory)} - if
   *          its not already set, a {@link NullPointerException} is thrown.
   * @param source source region from which we are writing
   * @param edit log edit to attempt to use to write to the idnex table
   * @return <tt>true</tt> if we successfully wrote to the index table. Also, returns <tt>false</tt>
   *         if we are not passed an {@link IndexedWALEdit}.
   */
  public void writeAndKillYourselfOnFailure(Map<Mutation, HTableInterfaceReference> indexUpdates,
      HTableFactory factory) {
    try {
      write(indexUpdates, factory);
    } catch (Exception e) {
      killYourself(e);
    }
  }

  /**
   * Write the mutations to their respective table using the {@link HTableFactory} accompanying each
   * reference.
   * @param updates Updates to write
   * @param factory Factory to use when resolving the {@link HTableInterfaceReference}. If
   *          <tt>null</tt>, its assumed that the {@link HTableInterfaceReference} already has its
   *          factory set (e.g. by {@link HTableInterfaceReference#setFactory(HTableFactory)} - if
   *          its not already set, a {@link NullPointerException} is thrown.
   * @throws CannotReachIndexException if we cannot successfully write a single index entry. We stop
   *           immediately on the first failed index write, rather than attempting all writes.
   */
  public void write(Map<Mutation, HTableInterfaceReference> updates)
      throws CannotReachIndexException {
    this.write(updates, null);
  }

  /**
   * Write the mutations to their respective table using the provided factory.
   * <p>
   * This method is not thread-safe and if accessed in a non-serial manner could leak HTables.
   * @param updates Updates to write
   * @param factory Factory to use when resolving the {@link HTableInterfaceReference}. If
   *          <tt>null</tt>, its assumed that the {@link HTableInterfaceReference} already has its
   *          factory set (e.g. by {@link HTableInterfaceReference#setFactory(HTableFactory)} - if
   *          its not already set, a {@link NullPointerException} is thrown.
   * @throws CannotReachIndexException if we cannot successfully write a single index entry. We stop
   *           immediately on the first failed index write, rather than attempting all writes.
   */
  private void write(Map<Mutation, HTableInterfaceReference> updates, HTableFactory factory)
      throws CannotReachIndexException {
    List<Mutation> singleMutation = new ArrayList<Mutation>(1);
    Set<HTableInterface> tables = new HashSet<HTableInterface>();
    for (Entry<Mutation, HTableInterfaceReference> entry : updates.entrySet()) {
      // do the put into the index table
      singleMutation.add(entry.getKey());
      LOG.info("Writing index update:" + entry.getKey() + " to table: "
          + entry.getValue().getTableName());
      try {
        HTableInterface table;

        if (factory == null) {
          table = entry.getValue().getTable();
        } else {
          table = entry.getValue().getTable(factory);
        }
        // do the update
        table.batch(singleMutation);
        tables.add(table);
      } catch (IOException e) {
        throw new CannotReachIndexException(entry.getValue().getTableName(), entry.getKey(), e);
      } catch (InterruptedException e) {
        throw new CannotReachIndexException(entry.getValue().getTableName(), entry.getKey(), e);
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
}
