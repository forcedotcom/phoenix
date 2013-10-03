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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Threads;

import com.salesforce.hbase.index.table.CoprocessorHTableFactory;
import com.salesforce.hbase.index.table.HTableFactory;

public class IndexWriterUtils {

  private static final Log LOG = LogFactory.getLog(IndexWriterUtils.class);

  public static String NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY = "index.writer.threads.max";
  private static final int DEFAULT_CONCURRENT_INDEX_WRITER_THREADS = 10;
  private static final String INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY =
      "index.writer.threads.keepalivetime";

  /**
   * Maximum number of threads to allow per-table when writing. Each writer thread (from
   * {@link IndexWriterUtils#NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY}) has a single HTable.
   * However, each table is backed by a threadpool to manage the updates to that table. this
   * specifies the number of threads to allow in each of those tables. Generally, you shouldn't need
   * to change this, unless you have a small number of indexes to which most of the writes go.
   * Defaults to: {@value #DEFAULT_NUM_PER_TABLE_THREADS}.
   * <p>
   * For tables to which there are not a lot of writes, the thread pool automatically will decrease
   * the number of threads to one (though it can burst up to the specified max for any given table),
   * so increasing this to meet the max case is reasonable.
   */
  private static final String INDEX_WRITER_PER_TABLE_THREADS_CONF_KEY =
      "index.writer.threads.pertable.max";
  private static final int DEFAULT_NUM_PER_TABLE_THREADS = 1;

  private IndexWriterUtils() {
    // private ctor for utilites
  }

  /**
   * @param conf
   * @return a thread pool based on the passed configuration whose threads are all daemon threads.
   */
  public static ThreadPoolExecutor getDefaultExecutor(Configuration conf) {
    int maxThreads =
        conf.getInt(IndexWriterUtils.NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY,
          IndexWriterUtils.DEFAULT_CONCURRENT_INDEX_WRITER_THREADS);
    if (maxThreads == 0) {
      maxThreads = 1; // is there a better default?
    }
    LOG.info("Starting writer with " + maxThreads + " threads for all tables");
    long keepAliveTime = conf.getLong(IndexWriterUtils.INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY, 60);
  
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

  public static HTableFactory getDefaultDelegateHTableFactory(CoprocessorEnvironment env) {
    // create a simple delegate factory, setup the way we need
    Configuration conf = env.getConfiguration();
    // set the number of threads allowed per table.
    int htableThreads =
        conf.getInt(IndexWriterUtils.INDEX_WRITER_PER_TABLE_THREADS_CONF_KEY, IndexWriterUtils.DEFAULT_NUM_PER_TABLE_THREADS);
    LOG.info("Starting index writer with " + htableThreads + " threads for each HTable.");
    conf.setInt("hbase.htable.threads.max", htableThreads);
    return new CoprocessorHTableFactory(env);
  }
}
