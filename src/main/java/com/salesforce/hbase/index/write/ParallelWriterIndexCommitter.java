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
package com.salesforce.hbase.index.write;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import com.google.common.collect.Multimap;
import com.salesforce.hbase.index.exception.SingleIndexWriteFailureException;
import com.salesforce.hbase.index.parallel.EarlyExitFailure;
import com.salesforce.hbase.index.parallel.QuickFailingTaskRunner;
import com.salesforce.hbase.index.parallel.Task;
import com.salesforce.hbase.index.parallel.TaskBatch;
import com.salesforce.hbase.index.parallel.ThreadPoolBuilder;
import com.salesforce.hbase.index.parallel.ThreadPoolManager;
import com.salesforce.hbase.index.table.CachingHTableFactory;
import com.salesforce.hbase.index.table.HTableFactory;
import com.salesforce.hbase.index.table.HTableInterfaceReference;

/**
 * Write index updates to the index tables in parallel. We attempt to early exit from the writes if
 * any of the index updates fails. Completion is determined by the following criteria: *
 * <ol>
 * <li>All index writes have returned, OR</li>
 * <li>Any single index write has failed</li>
 * </ol>
 * We attempt to quickly determine if any write has failed and not write to the remaining indexes to
 * ensure a timely recovery of the failed index writes.
 */
public class ParallelWriterIndexCommitter implements IndexCommitter {

  public static String NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY = "index.writer.threads.max";
  private static final int DEFAULT_CONCURRENT_INDEX_WRITER_THREADS = 10;
  private static final String INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY =
      "index.writer.threads.keepalivetime";
  private static final Log LOG = LogFactory.getLog(ParallelWriterIndexCommitter.class);

  private HTableFactory factory;
  private Stoppable stopped;
  private QuickFailingTaskRunner pool;

  @Override
  public void setup(IndexWriter parent, RegionCoprocessorEnvironment env, String name) {
    Configuration conf = env.getConfiguration();
    setup(IndexWriterUtils.getDefaultDelegateHTableFactory(env),
      ThreadPoolManager.getExecutor(
        new ThreadPoolBuilder(name, conf).
          setMaxThread(NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY,
            DEFAULT_CONCURRENT_INDEX_WRITER_THREADS).
          setCoreTimeout(INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY), env),
      env.getRegionServerServices(), parent, CachingHTableFactory.getCacheSize(conf));
  }

  /**
   * Setup <tt>this</tt>.
   * <p>
   * Exposed for TESTING
   */
  void setup(HTableFactory factory, ExecutorService pool, Abortable abortable, Stoppable stop,
      int cacheSize) {
    this.factory = new CachingHTableFactory(factory, cacheSize);
    this.pool = new QuickFailingTaskRunner(pool);
    this.stopped = stop;
  }

  @Override
  public void write(Multimap<HTableInterfaceReference, Mutation> toWrite)
      throws SingleIndexWriteFailureException {
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
    TaskBatch<Void> tasks = new TaskBatch<Void>(entries.size());
    for (Entry<HTableInterfaceReference, Collection<Mutation>> entry : entries) {
      // get the mutations for each table. We leak the implementation here a little bit to save
      // doing a complete copy over of all the index update for each table.
      final List<Mutation> mutations = (List<Mutation>) entry.getValue();
      final HTableInterfaceReference tableReference = entry.getKey();
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
      tasks.add(new Task<Void>() {

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
          } catch (SingleIndexWriteFailureException e) {
            throw e;
          } catch (IOException e) {
            throw new SingleIndexWriteFailureException(tableReference.toString(), mutations, e);
          } catch (InterruptedException e) {
            // reset the interrupt status on the thread
            Thread.currentThread().interrupt();
            throw new SingleIndexWriteFailureException(tableReference.toString(), mutations, e);
          }
          return null;
        }

        private void throwFailureIfDone() throws SingleIndexWriteFailureException {
          if (this.isBatchFailed() || Thread.currentThread().isInterrupted()) {
            throw new SingleIndexWriteFailureException(
                "Pool closed, not attempting to write to the index!", null);
          }

        }
      });
    }

    // actually submit the tasks to the pool and wait for them to finish/fail
    try {
      pool.submitUninterruptible(tasks);
    } catch (EarlyExitFailure e) {
      propagateFailure(e);
    } catch (ExecutionException e) {
      LOG.error("Found a failed index update!");
      propagateFailure(e.getCause());
    }

  }

  private void propagateFailure(Throwable throwable) throws SingleIndexWriteFailureException {
    try {
      throw throwable;
    } catch (SingleIndexWriteFailureException e1) {
      throw e1;
    } catch (Throwable e1) {
      throw new SingleIndexWriteFailureException(
          "Got an abort notification while writing to the index!", e1);
    }

  }

  /**
   * {@inheritDoc}
   * <p>
   * This method should only be called <b>once</b>. Stopped state ({@link #isStopped()}) is managed
   * by the external {@link Stoppable}. This call does not delegate the stop down to the
   * {@link Stoppable} passed in the constructor.
   * @param why the reason for stopping
   */
  @Override
  public void stop(String why) {
    LOG.info("Shutting down " + this.getClass().getSimpleName() + " because " + why);
    this.pool.stop(why);
    this.factory.shutdown();
  }

  @Override
  public boolean isStopped() {
    return this.stopped.isStopped();
  }
}