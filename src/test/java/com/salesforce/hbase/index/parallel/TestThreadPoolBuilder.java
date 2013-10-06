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

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;

import com.salesforce.hbase.index.TableName;

public class TestThreadPoolBuilder {

  @Rule
  public TableName name = new TableName();

  @Test
  public void testCoreThreadTimeoutNonZero() {
    Configuration conf = new Configuration(false);
    String key = name.getTableNameString()+"-key";
    ThreadPoolBuilder builder = new ThreadPoolBuilder(name.getTableNameString(), conf);
    assertTrue("core threads not set, but failed return", builder.getKeepAliveTime() > 0);
    // set an negative value
    builder.setCoreTimeout(key, -1);
    assertTrue("core threads not set, but failed return", builder.getKeepAliveTime() > 0);
    // set a positive value
    builder.setCoreTimeout(key, 1234);
    assertEquals("core threads not set, but failed return", 1234, builder.getKeepAliveTime());
    // set an empty value
    builder.setCoreTimeout(key);
    assertTrue("core threads not set, but failed return", builder.getKeepAliveTime() > 0);
  }
  
  @Test
  public void testMaxThreadsNonZero() {
    Configuration conf = new Configuration(false);
    String key = name.getTableNameString()+"-key";
    ThreadPoolBuilder builder = new ThreadPoolBuilder(name.getTableNameString(), conf);
    assertTrue("core threads not set, but failed return", builder.getMaxThreads() > 0);
    // set an negative value
    builder.setMaxThread(key, -1);
    assertTrue("core threads not set, but failed return", builder.getMaxThreads() > 0);
    // set a positive value
    builder.setMaxThread(key, 1234);
    assertEquals("core threads not set, but failed return", 1234, builder.getMaxThreads());
  }
}