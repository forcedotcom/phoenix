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
package com.salesforce.hbase.index.builder;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Pair;

import com.salesforce.hbase.index.covered.CoveredColumnsIndexBuilder;

/**
 * Basic implementation of the {@link IndexBuilder} that doesn't do any actual work of indexing.
 * <p>
 * You should extend this class, rather than implementing IndexBuilder directly to maintain
 * compatability going forward.
 * <p>
 * Generally, you should consider using one of the implemented IndexBuilders (e.g
 * {@link CoveredColumnsIndexBuilder}) as there is a lot of work required to keep an index table
 * up-to-date.
 */
public abstract class BaseIndexBuilder implements IndexBuilder {

  private static final Log LOG = LogFactory.getLog(BaseIndexBuilder.class);
  protected boolean stopped;

  @Override
  public void extendBaseIndexBuilderInstead() { }
  
  @Override
  public void setup(RegionCoprocessorEnvironment conf) throws IOException {
    // noop
  }

  @Override
  public void batchStarted(MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp) throws IOException {
    // noop
  }

  @Override
  public void batchCompleted(MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp) {
    // noop
  }
  
  /**
   * By default, we always attempt to index the mutation. Commonly this can be slow (because the
   * framework spends the time to do the indexing, only to realize that you don't need it) or not
   * ideal (if you want to turn on/off indexing on a table without completely reloading it).
 * @throws IOException 
   */
  @Override
  public boolean isEnabled(Mutation m) throws IOException {
    return true; 
  }

  /**
   * {@inheritDoc}
   * <p>
   * By default, assumes that all mutations should <b>not be batched</b>. That is to say, each
   * mutation always applies to different rows, even if they are in the same batch, or are
   * independent updates.
   */
  @Override
  public byte[] getBatchId(Mutation m) {
    return null;
  }

  @Override
  public void stop(String why) {
    LOG.debug("Stopping because: " + why);
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }
}