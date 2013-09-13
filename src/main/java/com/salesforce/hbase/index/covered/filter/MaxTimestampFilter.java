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
package com.salesforce.hbase.index.covered.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Inclusive filter on the maximum timestamp allowed. Excludes all elements greater than (but not
 * equal to) the given timestamp, so given ts = 5, a {@link KeyValue} with ts 6 is excluded, but not
 * one with ts = 5.
 */
public class MaxTimestampFilter extends FilterBase {

  private long ts;

  public MaxTimestampFilter(long maxTime) {
    this.ts = maxTime;
  }

  @Override
  public KeyValue getNextKeyHint(KeyValue currentKV) {
    // this might be a little excessive right now - better safe than sorry though, so we don't mess
    // with other filters too much.
    KeyValue kv = currentKV.deepCopy();
    int offset =kv.getTimestampOffset();
    //set the timestamp in the buffer
    byte[] buffer = kv.getBuffer();
    byte[] ts = Bytes.toBytes(this.ts);
    System.arraycopy(ts, 0, buffer, offset, ts.length);

    return kv;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    long timestamp = v.getTimestamp();
    if (timestamp > ts) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
    return ReturnCode.INCLUDE;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    throw new UnsupportedOperationException("Server-side only filter, cannot be serialized!");

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException("Server-side only filter, cannot be deserialized!");
  }
}