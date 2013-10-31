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
package com.salesforce.hbase.index;

import org.apache.hadoop.hbase.Abortable;

/**
 * {@link Abortable} that can rethrow the cause of the abort.
 */
public class CapturingAbortable implements Abortable {

  private Abortable delegate;
  private Throwable cause;
  private String why;

  public CapturingAbortable(Abortable delegate) {
    this.delegate = delegate;
  }

  @Override
  public void abort(String why, Throwable e) {
    if (delegate.isAborted()) {
      return;
    }
    this.why = why;
    this.cause = e;
    delegate.abort(why, e);

  }

  @Override
  public boolean isAborted() {
    return delegate.isAborted();
  }

  /**
   * Throw the cause of the abort, if <tt>this</tt> was aborted. If there was an exception causing
   * the abort, re-throws that. Otherwise, just throws a generic {@link Exception} with the reason
   * why the abort was caused.
   * @throws Throwable the cause of the abort.
   */
  public void throwCauseIfAborted() throws Throwable {
    if (!this.isAborted()) {
      return;
    }
    if (cause == null) {
      throw new Exception(why);
    }
    throw cause;
  }
}