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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 */
public class QuickFailingTaskRunner extends BaseTaskRunner implements TaskRunner {

  static final Log LOG = LogFactory.getLog(QuickFailingTaskRunner.class);
  public QuickFailingTaskRunner(ExecutorService service) {
    super(service);
  }

  /**
   * {@inheritDoc}
   * <p>
   * We return immediately if any of the submitted tasks fails, not waiting for the remaining tasks
   * to complete.
   */
  @Override
  public <R> List<R> submit(TaskBatch<R> tasks) throws EarlyExitFailure, ExecutionException {
    if (tasks == null || tasks.size() == 0) {
      return Collections.emptyList();
    }
    boolean earlyExit = false;
    CompletionService<R> ops = new ExecutorCompletionService<R>(this.writerPool);
    for (Task<R> task : tasks.getTasks()) {
      // early exit - no need to submit new tasks if we are shutting down
      if (this.isStopped()) {
        String msg = "Found a stop, need to fail early";
        tasks.abort(msg, new EarlyExitFailure(msg));
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
      while (!this.isStopped() && completedWrites < tasks.size()) {
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
            String msg = "Found a failed task!";
            LOG.error(msg, e);
            tasks.abort(msg, e.getCause());
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
}