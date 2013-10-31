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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import com.google.common.collect.Multimap;
import com.salesforce.hbase.index.table.HTableInterfaceReference;

/**
 * Naive failure policy - kills the server on which it resides
 */
public class KillServerOnFailurePolicy implements IndexFailurePolicy {

  private static final Log LOG = LogFactory.getLog(KillServerOnFailurePolicy.class);
  private Abortable abortable;
  private Stoppable stoppable;

  @Override
  public void setup(Stoppable parent, RegionCoprocessorEnvironment env) {
    setup(parent, env.getRegionServerServices());
  }

  public void setup(Stoppable parent, Abortable abort) {
    this.stoppable = parent;
    this.abortable = abort;
  }

  @Override
  public void stop(String why) {
    // noop
  }

  @Override
  public boolean isStopped() {
    return this.stoppable.isStopped();
  }

  @Override
  public void
      handleFailure(Multimap<HTableInterfaceReference, Mutation> attempted, Exception cause) throws IOException {
    // cleanup resources
    this.stop("Killing ourselves because of an error:" + cause);
    // notify the regionserver of the failure
    String msg =
        "Could not update the index table, killing server region because couldn't write to an index table";
    LOG.error(msg, cause);
    try {
      this.abortable.abort(msg, cause);
    } catch (Exception e) {
      LOG.fatal("Couldn't abort this server to preserve index writes, "
          + "attempting to hard kill the server");
      System.exit(1);
    }

  }

}
