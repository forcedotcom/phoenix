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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;

import com.salesforce.hbase.index.TableName;

public class TestThreadPoolManager {

  @Rule
  public TableName name = new TableName();

  @Test
  public void testShutdownGetsNewThreadPool() throws Exception{
    Map<String, Object> cache = new HashMap<String, Object>();
    ThreadPoolBuilder builder = new ThreadPoolBuilder(name.getTableNameString(), new Configuration(false));
    ThreadPoolExecutor exec = ThreadPoolManager.getExecutor(builder, cache);
    assertNotNull("Got a null exector from the pool!", exec);
    //shutdown the pool and ensure that it actually shutdown
    exec.shutdown();
    ThreadPoolExecutor exec2 = ThreadPoolManager.getExecutor(builder, cache);
    assertFalse("Got the same exectuor, even though the original shutdown", exec2 == exec);
  }

  @Test
  public void testShutdownWithReferencesDoesNotStopExecutor() throws Exception {
    Map<String, Object> cache = new HashMap<String, Object>();
    ThreadPoolBuilder builder =
        new ThreadPoolBuilder(name.getTableNameString(), new Configuration(false));
    ThreadPoolExecutor exec = ThreadPoolManager.getExecutor(builder, cache);
    assertNotNull("Got a null exector from the pool!", exec);
    ThreadPoolExecutor exec2 = ThreadPoolManager.getExecutor(builder, cache);
    assertTrue("Should have gotten the same executor", exec2 == exec);
    exec.shutdown();
    assertFalse("Executor is shutting down, even though we have a live reference!",
      exec.isShutdown() || exec.isTerminating());
    exec2.shutdown();
    // wait 5 minutes for thread pool to shutdown
    assertTrue("Executor is NOT shutting down, after releasing live reference!",
      exec.awaitTermination(300, TimeUnit.SECONDS));
  }

  @Test
  public void testGetExpectedExecutorForName() throws Exception {
    Map<String, Object> cache = new HashMap<String, Object>();
    ThreadPoolBuilder builder =
        new ThreadPoolBuilder(name.getTableNameString(), new Configuration(false));
    ThreadPoolExecutor exec = ThreadPoolManager.getExecutor(builder, cache);
    assertNotNull("Got a null exector from the pool!", exec);
    ThreadPoolExecutor exec2 = ThreadPoolManager.getExecutor(builder, cache);
    assertTrue("Got a different exectuor, even though they have the same name", exec2 == exec);
    builder = new ThreadPoolBuilder(name.getTableNameString(), new Configuration(false));
    exec2 = ThreadPoolManager.getExecutor(builder, cache);
    assertTrue(
      "Got a different exectuor, even though they have the same name, but different confs",
      exec2 == exec);

    builder =
        new ThreadPoolBuilder(name.getTableNameString() + "-some-other-pool", new Configuration(
            false));
    exec2 = ThreadPoolManager.getExecutor(builder, cache);
    assertFalse(
      "Got a different exectuor, even though they have the same name, but different confs",
      exec2 == exec);
  }
}