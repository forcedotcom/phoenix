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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;

/**
 * A group of {@link Task}s. The tasks are all bound together using the same {@link Abortable} (
 * <tt>this</tt>) to ensure that all tasks are aware when any of the other tasks fails.
 * @param <V> expected result type from all the tasks
 */
public class TaskBatch<V> implements Abortable {
  private static final Log LOG = LogFactory.getLog(TaskBatch.class);
  private AtomicBoolean aborted = new AtomicBoolean();
  private List<Task<V>> tasks;

  /**
   * @param size expected number of tasks
   */
  public TaskBatch(int size) {
    this.tasks = new ArrayList<Task<V>>(size);
  }

  public void add(Task<V> task) {
    this.tasks.add(task);
    task.setBatchMonitor(this);
  }

  public Collection<Task<V>> getTasks() {
    return this.tasks;
  }

  @Override
  public void abort(String why, Throwable e) {
    if (this.aborted.getAndSet(true)) {
      return;
    }
    LOG.info("Aborting batch of tasks because " + why);
  }

  @Override
  public boolean isAborted() {
    return this.aborted.get();
  }

  /**
   * @return the number of tasks assigned to this batch
   */
  public int size() {
    return this.tasks.size();
  }
}