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
package com.salesforce.phoenix.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * Build {@link KeyValue} in an efficient way
 */
public abstract class KeyValueBuilder {

  public static KeyValueBuilder get(Configuration conf) {
    // TODO Implement builder creation based on correct HBase version
    return null;
  }

  public KeyValue buildPut(ImmutableBytesWritable row, ImmutableBytesWritable family,
      ImmutableBytesWritable qualifier, ImmutableBytesWritable value) {
    return buildPut(row, family, qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  public abstract KeyValue buildPut(ImmutableBytesWritable row, ImmutableBytesWritable family,
      ImmutableBytesWritable qualifier, long ts, ImmutableBytesWritable value);

  public KeyValue buildDeleteFamily(ImmutableBytesWritable row, ImmutableBytesWritable family,
      ImmutableBytesWritable qualifier, ImmutableBytesWritable value) {
    return buildDeleteFamily(row, family, qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  public abstract KeyValue buildDeleteFamily(ImmutableBytesWritable row,
      ImmutableBytesWritable family, ImmutableBytesWritable qualifier, long ts,
      ImmutableBytesWritable value);

  public KeyValue buildDeleteColumns(ImmutableBytesWritable row, ImmutableBytesWritable family,
      ImmutableBytesWritable qualifier, ImmutableBytesWritable value) {
    return buildDeleteColumns(row, family, qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  public abstract KeyValue buildDeleteColumns(ImmutableBytesWritable row,
      ImmutableBytesWritable family, ImmutableBytesWritable qualifier, long ts,
      ImmutableBytesWritable value);

  public KeyValue buildDeleteColumn(ImmutableBytesWritable row, ImmutableBytesWritable family,
      ImmutableBytesWritable qualifier, ImmutableBytesWritable value) {
    return buildDeleteColumn(row, family, qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  public abstract KeyValue buildDeleteColumn(ImmutableBytesWritable row,
      ImmutableBytesWritable family, ImmutableBytesWritable qualifier, long ts,
      ImmutableBytesWritable value);
}