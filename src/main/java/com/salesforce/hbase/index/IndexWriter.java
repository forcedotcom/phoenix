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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.IndexedWALEdit;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.salesforce.hbase.index.table.CachingHTableFactory;
import com.salesforce.hbase.index.table.CoprocessorHTableFactory;
import com.salesforce.hbase.index.table.HTableFactory;
import com.salesforce.hbase.index.table.HTableInterfaceReference;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;

/**
 * Do the actual work of writing to the index tables. Ensures that if we do fail to write to the
 * index table that we cleanly kill the region/server to ensure that the region's WAL gets replayed.
 * <p>
 * We attempt to do the index updates in parallel using a backing threadpool. All threads are daemon
 * threads, so it will not block the region from shutting down.
 */
public class IndexWriter implements Stoppable {

  public static String NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY = "index.writer.threads.max";
  private static final Log LOG = LogFactory.getLog(IndexWriter.class);
  private static final int DEFAULT_CONCURRENT_INDEX_WRITER_THREADS = 10;
  private static final String INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY =
      "index.writer.threads.keepalivetime";

  private final String sourceInfo;
  private final CapturingAbortable abortable;
  private final HTableFactory factory;
  private ListeningExecutorService writerPool;
  private AtomicBoolean stopped = new AtomicBoolean(false);

  /**
   * @param sourceInfo log info string about where we are writing from
   * @param abortable to notify in the case of failure
   * @param env Factory to use when resolving the {@link HTableInterfaceReference}. If <tt>null</tt>
   *          , its assumed that the {@link HTableInterfaceReference} already has its factory set
   *          (e.g. by {@link HTableInterfaceReference#setFactory(HTableFactory)} - if its not
   *          already set, a {@link NullPointerException} is thrown.
   */
  public IndexWriter(String sourceInfo, Abortable abortable, RegionCoprocessorEnvironment env,
      Configuration conf) {
    this(sourceInfo, abortable, env, getDefaultExecutor(conf));
  }

  /**
   * @param conf
   * @return
   */
  private static ThreadPoolExecutor getDefaultExecutor(Configuration conf) {
    int maxThreads =
        conf.getInt(NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY,
          DEFAULT_CONCURRENT_INDEX_WRITER_THREADS);
    if (maxThreads == 0) {
      maxThreads = 1; // is there a better default?
    }
    long keepAliveTime = conf.getLong(INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY, 60);

    // we prefer starting a new thread to queuing (the opposite of the usual ThreadPoolExecutor)
    // since we are probably writing to a bunch of index tables in this case. Any pending requests
    // are then queued up in an infinite (Integer.MAX_VALUE) queue. However, we allow core threads
    // to timeout, to we tune up/down for bursty situations. We could be a bit smarter and more
    // closely manage the core-thread pool size to handle the bursty traffic (so we can always keep
    // some core threads on hand, rather than starting from scratch each time), but that would take
    // even more time. If we shutdown the pool, but are still putting new tasks, we can just do the
    // usual policy and throw a RejectedExecutionException because we are shutting down anyways and
    // the worst thing is that this gets unloaded.
    ThreadPoolExecutor pool =
        new ThreadPoolExecutor(maxThreads, maxThreads, keepAliveTime, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(), Threads.newDaemonThreadFactory("index-writer-"));
    pool.allowCoreThreadTimeOut(true);
    return pool;
  }

  public IndexWriter(String sourceInfo, Abortable abortable, CoprocessorEnvironment env,
      ExecutorService pool) {
    this(sourceInfo, abortable, pool, getDefaultDelegateHTableFactory(env));
  }

  private static HTableFactory getDefaultDelegateHTableFactory(CoprocessorEnvironment env) {
    // create a simple delegate factory, setup the way we need
    Configuration conf = env.getConfiguration();
    // only have one thread per table - all the writes are already batched per table.

    conf.setInt("hbase.htable.threads.max", 1);
    return new CoprocessorHTableFactory(env);
  }

  /**
   * Internal constructor - Exposed for testing!
   * @param sourceInfo
   * @param abortable
   * @param pool
   * @param delegate
   */
  IndexWriter(String sourceInfo, Abortable abortable, ExecutorService pool, HTableFactory delegate) {
    this.sourceInfo = sourceInfo;
    this.abortable = new CapturingAbortable(abortable);
    this.writerPool = MoreExecutors.listeningDecorator(pool);
    this.factory = new CachingHTableFactory(delegate);
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
  public void writeAndKillYourselfOnFailure(Collection<Pair<Mutation, byte[]>> indexUpdates) {
    try {
      write(indexUpdates);
    } catch (Exception e) {
      killYourself(e);
    }
  }

  /**
   * Write the mutations to their respective table.
   * <p>
   * This method is blocking and could potentially cause the writer to block for a long time as we
   * write the index updates. We only return when either:
   * <ol>
   *   <li>All index writes have returned, OR</li>
   *   <li>Any single index write has failed</li>
   * </ol>
   * We attempt to quickly determine if any write has failed and not write to the remaining indexes
   * to ensure a timely recovery of the failed index writes.
   * @param indexUpdates Updates to write
   * @throws CannotReachIndexException if we cannot successfully write a single index entry. We stop
   *           immediately on the first failed index write, rather than attempting all writes.
   */
  public void write(Collection<Pair<Mutation, byte[]>> indexUpdates)
      throws CannotReachIndexException {
    // convert the strings to htableinterfaces to which we can talk and group by TABLE
    Multimap<HTableInterfaceReference, Mutation> toWrite = resolveTableReferences(factory,
      indexUpdates);

    /*
     * This bit here is a little odd, so let's explain what's going on. Basically, we want to do the
     * writes in parallel to each index table, so each table gets its own task and is submitted to
     * the pool. Where it gets tricky is that we want to block the calling thread until one of two
     * things happens: (1) all index tables get successfully updated, or (2) any one of the index
     * table writes fail; in either case, we should return as quickly as possible. We get a little
     * more complicated in that if we do get a single failure, but any of the index writes hasn't
     * been started yet (its been queued up, but not submitted to a thread) we want to that task to
     * fail immediately as we know that write is a waste and will need to be replayed anyways.
     */

    Set<Entry<HTableInterfaceReference, Collection<Mutation>>> entries = toWrite.asMap().entrySet();
    CompletionService<Void> ops = new ExecutorCompletionService<Void>(this.writerPool);
    for (Entry<HTableInterfaceReference, Collection<Mutation>> entry : entries) {
      // get the mutations for each table. We leak the implementation here a little bit to save
      // doing a complete copy over of all the index update for each table.
      final List<Mutation> mutations = (List<Mutation>) entry.getValue();
      final HTableInterfaceReference tableReference = entry.getKey();
      // early exit - no need to submit new tasks if we are shutting down
      if (this.stopped.get() || this.abortable.isAborted()) {
        break;
      }

      /*
       * Write a batch of index updates to an index table. This operation stops (is cancelable) via
       * two mechanisms: (1) setting aborted or stopped on the IndexWriter or, (2) interrupting the
       * running thread. The former will only work if we are not in the midst of writing the current
       * batch to the table, though we do check these status variables before starting and before
       * writing the batch. The latter usage, interrupting the thread, will work in the previous
       * situations as was at some points while writing the batch, depending on the underlying
       * writer implementation (HTableInterface#batch is blocking, but doesn't elaborate when is
       * supports an interrupt).
       */
      ops.submit(new Callable<Void>() {

        /**
         * Do the actual write to the primary table. We don't need to worry about closing the table
         * because that is handled the {@link CachingHTableFactory}.
         */
        @Override
        public Void call() throws Exception {
          // this may have been queued, so another task infront of us may have failed, so we should
          // early exit, if that's the case
          throwFailureIfDone();

          if (LOG.isDebugEnabled()) {
            LOG.debug("Writing index update:" + mutations + " to table: " + tableReference);
          }
          try {
            HTableInterface table = factory.getTable(tableReference.get());
            throwFailureIfDone();
            table.batch(mutations);
          } catch (CannotReachIndexException e) {
            throw e;
          } catch (IOException e) {
            throw new CannotReachIndexException(tableReference.toString(), mutations, e);
          } catch (InterruptedException e) {
            // reset the interrupt status on the thread
            Thread.currentThread().interrupt();
            throw new CannotReachIndexException(tableReference.toString(), mutations, e);
          }
          return null;
        }

        private void throwFailureIfDone() throws CannotReachIndexException {
          if (stopped.get() || abortable.isAborted() || Thread.currentThread().isInterrupted()) {
            throw new CannotReachIndexException(
                "Pool closed, not attempting to write to the index!", null);
          }

        }
      });
    }

    boolean interrupted = false;
    // we can use a simple counter here because its ever only modified by the waiting thread
    int completedWrites = 0;
    /*
     * wait for all index writes to complete, or there to be a failure. We could be faster here in
     * terms of watching for a failed index write. Right now, we need to wade through any successful
     * attempts that happen to finish before we get to the failed update. For right now, that's fine
     * as we don't really spend a lot time getting through the successes and a slight delay on the
     * abort really isn't the end of the world. We could be smarter and use a Guava ListenableFuture
     * to handle a callback off the future that updates the abort status, but for now we don't need
     * the extra complexity.
     */
    while (!this.abortable.isAborted() && !this.isStopped() && completedWrites < entries.size()) {
      try {
        Future<Void> status = ops.take();
        try {
          // we don't care what the status is - success is binary, so no error == success
          status.get();
          completedWrites++;
        } catch (CancellationException e) {
          // if we get a cancellation, we already failed for some other reason, so we can ignore it
          LOG.debug("Found canceled index write - ignoring!");
        } catch (ExecutionException e) {
          LOG.error("Found a failed index update!");
          abortable.abort("Failed ot writer to an index table!", e.getCause());
          break;
        }
      } catch (InterruptedException e) {
        LOG.info("Index writer interrupted, continuing if not aborted or stopped.");
        // reset the interrupt status so we can wait out that latch
        interrupted = true;
      }
    }
    // reset the interrupt status after we are done
    if (interrupted) {
      Thread.currentThread().interrupt();
    }

    // propagate the failure up to the caller
    try {
      this.abortable.throwCauseIfAborted();
    } catch (CannotReachIndexException e) {
      throw e;
    } catch (Throwable e) {
      throw new CannotReachIndexException("Got an abort notification while writing to the index!",
          e);
    }

    LOG.info("Done writing all index updates!");
  }

  /**
   * @param logEdit edit for which we need to kill ourselves
   * @param info region from which we are attempting to write the log
   */
  private void killYourself(Throwable cause) {
    // cleanup resources
    this.stop("Killing ourselves because of an error:" + cause);
    // notify the regionserver of the failure
    String msg = "Could not update the index table, killing server region from: " + this.sourceInfo;
    LOG.error(msg);
    try {
      this.abortable.abort(msg, cause);
    } catch (Exception e) {
      LOG.fatal("Couldn't abort this server to preserve index writes, "
          + "attempting to hard kill the server from" + this.sourceInfo);
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
      HTableFactory factory, Collection<Pair<Mutation, byte[]>> indexUpdates) {
    Multimap<HTableInterfaceReference, Mutation> updates = ArrayListMultimap
        .<HTableInterfaceReference, Mutation> create();
    // simple map to make lookups easy while we build the map of tables to create
    Map<ImmutableBytesPtr, HTableInterfaceReference> tables =
        new HashMap<ImmutableBytesPtr, HTableInterfaceReference>(updates.size());
    for (Pair<Mutation, byte[]> entry : indexUpdates) {
      byte[] tableName = entry.getSecond();
      ImmutableBytesPtr ptr = new ImmutableBytesPtr(tableName);
      HTableInterfaceReference table = tables.get(ptr);
      if (table == null) {
        table = new HTableInterfaceReference(ptr);
        tables.put(ptr, table);
      }
      updates.put(table, entry.getFirst());
    }

    return updates;
  }

  @Override
  public void stop(String why) {
    if (!this.stopped.compareAndSet(false, true)) {
      // already stopped
      return;
    }
    LOG.debug("Stopping because " + why);
    this.writerPool.shutdownNow();
    this.factory.shutdown();
  }

  @Override
  public boolean isStopped() {
    return this.stopped.get();
  }
}