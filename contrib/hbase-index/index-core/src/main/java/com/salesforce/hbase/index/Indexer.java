package com.salesforce.hbase.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Pair;

import com.salesforce.hbase.index.builder.IndexBuilder;
import com.salesforce.hbase.index.table.CoprocessorHTableFactory;
import com.salesforce.hbase.index.table.HTableFactory;
import com.salesforce.hbase.index.wal.IndexedKeyValue;

/**
 * Do all the work of managing index updates from a single coprocessor. All Puts/Delets are passed
 * to an {@link IndexBuilder} to determine the actual updates to make.
 * <p>
 * If the WAL is enabled, these updates are then added to the WALEdit and attempted to be written to
 * the WAL after the WALEdit has been saved. If any of the index updates fail, this server is
 * immediately terminated and we rely on WAL replay to attempt the index updates again (see
 * {@link #preWALRestore(ObserverContext, HRegionInfo, HLogKey, WALEdit)}).
 * <p>
 * If the WAL is disabled, the updates are attempted immediately. No consistency guarantees are made
 * if the WAL is disabled - some or none of the index updates may be successful.
 */
public class Indexer extends BaseRegionObserver {

  private static final Log LOG = LogFactory.getLog(Indexer.class);

  /** WAL on this server */
  private HLog log;
  protected IndexWriter writer;
  protected HTableFactory factory;

  protected IndexBuilder builder;

  // Setup out locking on the index edits/WAL so we can be sure that we don't lose a roll a WAL edit
  // before an edit is applied to the index tables
  private static final ReentrantReadWriteLock INDEX_READ_WRITE_LOCK = new ReentrantReadWriteLock(
      true);
  public static final ReadLock INDEX_UPDATE_LOCK = INDEX_READ_WRITE_LOCK.readLock();

  /**
   * Configuration key for if the indexer should check the version of HBase is running. Generally,
   * you only want to ignore this for testing or for custom versions of HBase.
   */
  public static final String CHECK_VERSION_CONF_KEY = "com.saleforce.hbase.index.checkversion";

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    this.factory = new CoprocessorHTableFactory(e);

    final RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) e;

    if (env.getConfiguration().getBoolean(CHECK_VERSION_CONF_KEY, true)) {
      // make sure the right version <-> combinations are allowed.
      String errormsg = IndexUtil.validateVersion(env.getHBaseVersion(), env.getConfiguration());
      if (errormsg != null) {
        IOException ioe = new IOException(errormsg);
        env.getRegionServerServices().abort(errormsg, ioe);
        throw ioe;
      }
    }

    // setup the index entry builder so we can build edits for the index tables
    Configuration conf = e.getConfiguration();
    Class<? extends IndexBuilder> builderClass = conf.getClass(IndexUtil.INDEX_BUILDER_CONF_KEY,
      null, IndexBuilder.class);
    try {
      this.builder = builderClass.newInstance();
    } catch (InstantiationException e1) {
      throw new IOException("Couldn't instantiate index builder:" + builderClass
          + ", disabling indexing on table " + env.getRegion().getTableDesc().getNameAsString());
    } catch (IllegalAccessException e1) {
      throw new IOException("Couldn't instantiate index builder:" + builderClass
          + ", disabling indexing on table " + env.getRegion().getTableDesc().getNameAsString());
    }
    this.builder.setup(env);

    // get a reference to the WAL
    log = env.getRegionServerServices().getWAL();
    // add a synchronizer so we don't archive a WAL that we need
    log.registerWALActionsListener(new IndexLogRollSynchronizer(INDEX_READ_WRITE_LOCK.writeLock()));

    // and setup the actual index writer
    this.writer = new IndexWriter("Region: " + env.getRegion().getRegionNameAsString(),
            env.getRegionServerServices(), factory);
  }

  @Override
  public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c, final Put put,
      final WALEdit edit, final boolean writeToWAL) throws IOException {
    // get the mapping for index column -> target index table
    Collection<Pair<Mutation, String>> indexUpdates = this.builder.getIndexUpdate(put);

    doPre(indexUpdates, edit, writeToWAL);
  }

  @Override
  public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete,
      WALEdit edit, boolean writeToWAL) throws IOException {
    // get the mapping for index column -> target index table
    Collection<Pair<Mutation, String>> indexUpdates = this.builder.getIndexUpdate(delete);

    doPre(indexUpdates, edit, writeToWAL);
  }

  private void doPre(Collection<Pair<Mutation, String>> updates,
      final WALEdit edit, final boolean writeToWAL) throws IOException {
    // no index updates, so we are done
    if (updates == null || updates.size() == 0) {
      return;
    }

    // if writing to wal is disabled, we never see the WALEdit updates down the way, so do the index
    // update right away
    if (!writeToWAL) {
      try {
        this.writer.write(updates);
        return;
      } catch (CannotReachIndexException e) {
        LOG.error("Failed to update index with entries:" + updates, e);
        throw new IOException(e);
      }
    }

    // we have all the WAL durability, so we just update the WAL entry and move on
    for (Pair<Mutation, String> entry : updates) {
      edit.add(new IndexedKeyValue(entry.getSecond(), entry.getFirst()));
    }

    // lock the log, so we are sure that index write gets atomically committed
    INDEX_UPDATE_LOCK.lock();
  }

  @Override
  public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit,
      boolean writeToWAL) throws IOException {
    doPost(edit, writeToWAL);
  }

  @Override
  public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete,
      WALEdit edit, boolean writeToWAL) throws IOException {
    doPost(edit, writeToWAL);
  }

  /**
   * @param edit
   * @param writeToWAL
   */
  private void doPost(WALEdit edit, boolean writeToWAL) {
    if (!writeToWAL) {
      // already did the index update in prePut, so we are done
      return;
    }

    Collection<Pair<Mutation, String>> indexUpdates = extractIndexUpdate(edit);

    // early exit - we have nothing to write, so we don't need to do anything else. NOTE: we don't
    // release the WAL Rolling lock (INDEX_UPDATE_LOCK) since we never take it in doPre if there are
    // no index updates.
    if (indexUpdates.size() == 0) {
      return;
    }

    // the WAL edit is kept in memory and we already specified the factory when we created the
    // references originally - therefore, we just pass in a null factory here and use the ones
    // already specified on each reference
    writer.writeAndKillYourselfOnFailure(indexUpdates);

    // release the lock on the index, we wrote everything properly
    INDEX_UPDATE_LOCK.unlock();
  }

  /**
   * Extract the index updates from the WAL Edit
   * @param edit to search for index updates
   * @return the mutations to apply to the index tables
   */
  private Collection<Pair<Mutation, String>> extractIndexUpdate(WALEdit edit) {
    Collection<Pair<Mutation, String>> indexUpdates = new ArrayList<Pair<Mutation, String>>();
    for (KeyValue kv : edit.getKeyValues()) {
      if (kv instanceof IndexedKeyValue) {
        IndexedKeyValue ikv = (IndexedKeyValue) kv;
        indexUpdates.add(new Pair<Mutation, String>(ikv.getMutation(), ikv
            .getIndexTable()));
      }
    }

    return indexUpdates;
  }

  @Override
  public void preWALRestore(ObserverContext<RegionCoprocessorEnvironment> env, HRegionInfo info,
      HLogKey logKey, WALEdit logEdit) throws IOException {
    Collection<Pair<Mutation, String>> indexUpdates = extractIndexUpdate(logEdit);
    writer.writeAndKillYourselfOnFailure(indexUpdates);
  }

  /**
   * Create a custom {@link InternalScanner} for a compaction that tracks the versions of rows that
   * are removed so we can clean then up from the the index table(s).
   */
  @Override
  public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs,
      InternalScanner s) throws IOException {
    // if (true) {
    // throw new RuntimeException("not yet implemented");
    // }
    return super.preCompactScannerOpen(c, store, scanners, scanType, earliestPutTs, s);
  }

}