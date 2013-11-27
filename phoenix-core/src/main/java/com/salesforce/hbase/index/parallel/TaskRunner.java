/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc. All rights reserved. Redistribution and use in source
 * and binary forms, with or without modification, are permitted provided that the following
 * conditions are met: Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. Redistributions in binary form must reproduce
 * the above copyright notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution. Neither the name of
 * Salesforce.com nor the names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission. THIS SOFTWARE IS PROVIDED
 * BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.hbase.index.parallel;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.Stoppable;

/**
 *
 */
public interface TaskRunner extends Stoppable {

  /**
   * Submit the given tasks to the pool and wait for them to complete. fail.
   * <p>
   * Non-interruptible method. To stop any running tasks call {@link #stop(String)} - this will
   * shutdown the thread pool, causing any pending tasks to be failed early (whose failure will be
   * ignored) and interrupt any running tasks. It is up to the passed tasks to respect the interrupt
   * notification
   * @param tasks to run
   * @return the result from each task
   * @throws ExecutionException if any of the tasks fails. Wraps the underyling failure, which can
   *           be retrieved via {@link ExecutionException#getCause()}.
   * @throws InterruptedException if the current thread is interrupted while waiting for the batch
   *           to complete
   */
  public <R> List<R> submit(TaskBatch<R> tasks) throws
      ExecutionException, InterruptedException;

  /**
   * Similar to {@link #submit(TaskBatch)}, but is not interruptible. If an interrupt is found while
   * waiting for results, we ignore it and only stop is {@link #stop(String)} has been called. On
   * return from the method, the interrupt status of the thread is restored.
   * @param tasks to run
   * @return the result from each task
   * @throws EarlyExitFailure if there are still tasks to submit to the pool, but there is a stop
   *           notification
   * @throws ExecutionException if any of the tasks fails. Wraps the underyling failure, which can
   *           be retrieved via {@link ExecutionException#getCause()}.
   */
  public <R> List<R> submitUninterruptible(TaskBatch<R> tasks) throws EarlyExitFailure,
      ExecutionException;
}