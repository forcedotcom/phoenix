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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Stoppable;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 *
 */
public class QuickFailingTaskRunner implements Stoppable {

  private static final Log LOG = LogFactory.getLog(QuickFailingTaskRunner.class);
  private ListeningExecutorService writerPool;
  private boolean stopped;
  
  public QuickFailingTaskRunner(ExecutorService service) {
    this.writerPool = MoreExecutors.listeningDecorator(service);
  }

  /**
   * Submit the given tasks to the pool and wait for them to complete, or for one of the tasks to
   * fail.
   * <p>
   * Non-interruptible method. To stop any running tasks call {@link #stop(String)} - this will
   * shutdown the thread pool, causing any pending tasks to be failed early (whose failure will be
   * ignored) and interrupt any running tasks. It is up to the passed tasks to respect the interrupt
   * notification
   * @param tasks to run
   * @throws EarlyExitFailure if there are still tasks to submit to the pool, but there is a stop
   *           notification
   * @throws ExecutionException if any of the tasks fails. Wraps the underyling failure, which can
   *           be retrieved via {@link ExecutionException#getCause()}.
   */
  public <R> List<R> submit(Callable<R>... tasks) throws EarlyExitFailure, ExecutionException {
    if (tasks == null || tasks.length == 0) {
      return Collections.emptyList();
    }
    boolean earlyExit = false;
    CompletionService<R> ops = new ExecutorCompletionService<R>(this.writerPool);
    for (Callable<R> task : tasks) {
      // early exit - no need to submit new tasks if we are shutting down
      if (this.isStopped()) {
        earlyExit = true;
        break;
      }

      ops.submit(task);
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
    List<R> results = new ArrayList<R>();
    try {
      while (!this.isStopped() && completedWrites < tasks.length) {
        try {
          Future<R> status = ops.take();
          try {
            // we don't care what the status is - success is binary, so no error == success
            results.add(status.get());
            completedWrites++;
          } catch (CancellationException e) {
            // if we get a cancellation, we already failed for some other reason, so we can ignore
            LOG.debug("Found canceled task - ignoring!");
          } catch (ExecutionException e) {
            // propagate the failure back out
            LOG.error("Found a failed task!", e);
            throw e;
          }
        } catch (InterruptedException e) {
          LOG.info("Task runner interrupted, continuing if not aborted or stopped.");
          // reset the interrupt status so we can wait out that latch
          interrupted = true;
        }
      }
    } finally {
      // reset the interrupt status after we are done
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }

    if(earlyExit){
      throw new EarlyExitFailure("Found a stop notification mid-task submission. Quitting early!");
    }

    return results;
  }

  @Override
  public void stop(String why) {
    if(this.stopped){
      return;
    }
    LOG.info("Shutting down task runner because "+why);
    this.writerPool.shutdownNow();
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }
}