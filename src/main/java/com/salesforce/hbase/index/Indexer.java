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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
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

  /** Configuration key for the {@link IndexBuilder} to use */
  public static final String INDEX_BUILDER_CONF_KEY = "index.builder";

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
      String errormsg = Indexer.validateVersion(env.getHBaseVersion(), env.getConfiguration());
      if (errormsg != null) {
        IOException ioe = new IOException(errormsg);
        env.getRegionServerServices().abort(errormsg, ioe);
        throw ioe;
      }
    }

    // setup the index entry builder so we can build edits for the index tables
    Configuration conf = e.getConfiguration();
    Class<? extends IndexBuilder> builderClass = conf.getClass(Indexer.INDEX_BUILDER_CONF_KEY,
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
  public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp) throws IOException {
    this.builder.batchStarted(miniBatchOp);
  }

  @Override
  public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp) throws IOException {
    this.builder.batchCompleted(miniBatchOp);
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
   * <p>
   * This is not yet implemented - its not clear if we should even mess around with the Index table
   * for these rows as those points still existed. TODO: v2 of indexing
   */
  @Override
  public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs,
      InternalScanner s) throws IOException {
    return super.preCompactScannerOpen(c, store, scanners, scanType, earliestPutTs, s);
  }

  /**
   * Exposed for testing!
   * @return the currently instantiated index buidler
   */
  public IndexBuilder getBuilderForTesting() {
    return this.builder;
  }

  /**
   * Validate that the version and configuration parameters are supported
   * @param hBaseVersion current version of HBase on which <tt>this</tt> coprocessor is installed
   * @param conf configuration to check for allowed parameters (e.g. WAL Compression only if >=
   *          0.94.9)
   */
  public static String validateVersion(String hBaseVersion, Configuration conf) {
    String[] versions = hBaseVersion.split("[.]");
    if (versions.length < 3) {
      return "HBase version could not be read, expected three parts, but found: "
          + Arrays.toString(versions);
    }
  
    if (versions[1].equals("94")) {
      String pointVersion = versions[2];
      //remove -SNAPSHOT if applicable
      int snapshot = pointVersion.indexOf("-");
      if(snapshot > 0){
        pointVersion = pointVersion.substring(0, snapshot);
      }
      // less than 0.94.9, so we need to check if WAL Compression is enabled
      if (Integer.parseInt(pointVersion) < 9) {
        if (conf.getBoolean(HConstants.ENABLE_WAL_COMPRESSION, false)) {
          return
                "Indexing not supported with WAL Compression for versions of HBase older than 0.94.9 - found version:"
              + Arrays.toString(versions);
        }
      }
    }
    return null;
  }

  /**
   * Enable indexing on the given table
   * @param desc {@link HTableDescriptor} for the table on which indexing should be enabled
   * @param builder class to use when building the index for this table
   * @param properties map of custom configuration options to make available to your
   *          {@link IndexBuilder} on the server-side
   * @throws IOException the Indexer coprocessor cannot be added
   */
  public static void enableIndexing(HTableDescriptor desc, Class<? extends IndexBuilder> builder,
      Map<String, String> properties) throws IOException {
    if (properties == null) {
      properties = new HashMap<String, String>();
    }
    properties.put(Indexer.INDEX_BUILDER_CONF_KEY, builder.getName());
    desc.addCoprocessor(Indexer.class.getName(), null, Coprocessor.PRIORITY_USER, properties);
  }
}