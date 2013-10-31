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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Helper utility to make a thread pool from a configuration based on reasonable defaults and passed
 * configuration keys.
 */
public class ThreadPoolBuilder {

  private static final Log LOG = LogFactory.getLog(ThreadPoolBuilder.class);
  private static final long DEFAULT_TIMEOUT = 60;
  private static final int DEFAULT_MAX_THREADS = 1;// is there a better default?
  private Pair<String, Long> timeout;
  private Pair<String, Integer> maxThreads;
  private String name;
  private Configuration conf;

  public ThreadPoolBuilder(String poolName, Configuration conf) {
    this.name = poolName;
    this.conf = conf;
  }

  public ThreadPoolBuilder setCoreTimeout(String confkey, long defaultTime) {
    if (defaultTime <= 0) {
      defaultTime = DEFAULT_TIMEOUT;
    }
    this.timeout = new Pair<String, Long>(confkey, defaultTime);
    return this;
  }

  public ThreadPoolBuilder setCoreTimeout(String confKey) {
    return this.setCoreTimeout(confKey, DEFAULT_TIMEOUT);
  }

  public ThreadPoolBuilder setMaxThread(String confkey, int defaultThreads) {
    if (defaultThreads <= 0) {
      defaultThreads = DEFAULT_MAX_THREADS;
    }
    this.maxThreads = new Pair<String, Integer>(confkey, defaultThreads);
    return this;
  }

  String getName() {
   return this.name;
  }

  int getMaxThreads() {
    int maxThreads = DEFAULT_MAX_THREADS;
    if (this.maxThreads != null) {
      String key = this.maxThreads.getFirst();
      maxThreads =
          key == null ? this.maxThreads.getSecond() : conf.getInt(key, this.maxThreads.getSecond());
    }
    LOG.trace("Creating pool builder with max " + maxThreads + " threads ");
    return maxThreads;
  }

  long getKeepAliveTime() {
    long timeout =DEFAULT_TIMEOUT;
    if (this.timeout != null) {
      String key = this.timeout.getFirst();
      timeout =
          key == null ? this.timeout.getSecond() : conf.getLong(key, this.timeout.getSecond());
    }

    LOG.trace("Creating pool builder with core thread timeout of " + timeout + " seconds ");
    return timeout;
  }
}