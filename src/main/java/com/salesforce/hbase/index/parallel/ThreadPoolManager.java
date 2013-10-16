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
package com.salesforce.hbase.index.parallel;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Threads;

/**
 * Manage access to thread pools
 */
public class ThreadPoolManager {

  private static final Log LOG = LogFactory.getLog(ThreadPoolManager.class);

  /**
   * Get an executor for the given name, based on the passed {@link Configuration}. If a thread pool
   * already exists with that name, it will be returned.
   * @param name
   * @param conf
   * @return a {@link ThreadPoolExecutor} for the given name. Thread pool that only shuts down when
   *         there are no more explicit references to it. You do not need to shutdown the threadpool
   *         on your own - it is managed for you. When you are done, you merely need to release your
   *         reference. If you do attempt to shutdown the pool, you should be careful to call
   *         {@link #shutdown()} XOR {@link #shutdownNow()} - extra calls to either can lead to
   *         early shutdown of the pool.
   */
  public static synchronized ThreadPoolExecutor getExecutor(ThreadPoolBuilder builder,
      RegionCoprocessorEnvironment env) {
    return getExecutor(builder, env.getSharedData());
  }

  static synchronized ThreadPoolExecutor getExecutor(ThreadPoolBuilder builder,
      Map<String, Object> poolCache) {
    ThreadPoolExecutor pool = (ThreadPoolExecutor) poolCache.get(builder.getName());
    if (pool == null || pool.isTerminating() || pool.isShutdown()) {
      pool = getDefaultExecutor(builder);
      LOG.info("Creating new pool for " + builder.getName());
      poolCache.put(builder.getName(), pool);
    }
    ((ShutdownOnUnusedThreadPoolExecutor) pool).addReference();

    return pool;
  }

  /**
   * @param conf
   * @return
   */
  private static ShutdownOnUnusedThreadPoolExecutor getDefaultExecutor(ThreadPoolBuilder builder) {
    int maxThreads = builder.getMaxThreads();
    long keepAliveTime = builder.getKeepAliveTime();

    // we prefer starting a new thread to queuing (the opposite of the usual ThreadPoolExecutor)
    // since we are probably writing to a bunch of index tables in this case. Any pending requests
    // are then queued up in an infinite (Integer.MAX_VALUE) queue. However, we allow core threads
    // to timeout, to we tune up/down for bursty situations. We could be a bit smarter and more
    // closely manage the core-thread pool size to handle the bursty traffic (so we can always keep
    // some core threads on hand, rather than starting from scratch each time), but that would take
    // even more time. If we shutdown the pool, but are still putting new tasks, we can just do the
    // usual policy and throw a RejectedExecutionException because we are shutting down anyways and
    // the worst thing is that this gets unloaded.
    ShutdownOnUnusedThreadPoolExecutor pool =
        new ShutdownOnUnusedThreadPoolExecutor(maxThreads, maxThreads, keepAliveTime,
            TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
            Threads.newDaemonThreadFactory(builder.getName() + "-"), builder.getName());
    pool.allowCoreThreadTimeOut(true);
    return pool;
  }

  /**
   * Thread pool that only shuts down when there are no more explicit references to it. A reference
   * is when obtained and released on calls to {@link #shutdown()} or {@link #shutdownNow()}.
   * Therefore, users should be careful to call {@link #shutdown()} XOR {@link #shutdownNow()} -
   * extra calls to either can lead to early shutdown of the pool.
   */
  private static class ShutdownOnUnusedThreadPoolExecutor extends ThreadPoolExecutor {

    private AtomicInteger references;
    private String poolName;

    public ShutdownOnUnusedThreadPoolExecutor(int coreThreads, int maxThreads, long keepAliveTime,
        TimeUnit timeUnit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory,
        String poolName) {
      super(coreThreads, maxThreads, keepAliveTime, timeUnit, workQueue, threadFactory);
      this.references = new AtomicInteger();
      this.poolName = poolName;
    }

    public void addReference() {
      this.references.incrementAndGet();
    }

    @Override
    protected void finalize() {
      // override references counter if we go out of scope - ensures the pool gets cleaned up
      LOG.info("Shutting down pool '" + poolName + "' because no more references");
      super.shutdown();
    }

    @Override
    public void shutdown() {
      if (references.decrementAndGet() <= 0) {
        LOG.debug("Shutting down pool " + this.poolName);
        super.shutdown();
      }
    }

    @Override
    public List<Runnable> shutdownNow() {
      if (references.decrementAndGet() <= 0) {
        LOG.debug("Shutting down pool " + this.poolName + " NOW!");
        return super.shutdownNow();
      }
      return Collections.emptyList();
    }

  }
}