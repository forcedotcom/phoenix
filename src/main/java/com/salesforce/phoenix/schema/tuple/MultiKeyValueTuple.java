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
package com.salesforce.phoenix.schema.tuple;

import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.util.KeyValueUtil;


public class MultiKeyValueTuple implements Tuple {
    private List<KeyValue> values;
    
    public MultiKeyValueTuple(List<KeyValue> values) {
        this.values = values;
    }
    
    public MultiKeyValueTuple() {
    }

    public void setKeyValues(List<KeyValue> values) {
        this.values = values;
    }
    
    @Override
    public void getKey(ImmutableBytesWritable ptr) {
        KeyValue value = values.get(0);
        ptr.set(value.getBuffer(), value.getRowOffset(), value.getRowLength());
    }

    @Override
    public boolean isImmutable() {
        return true;
    }

    @Override
    public KeyValue getValue(byte[] family, byte[] qualifier) {
        return KeyValueUtil.getColumnLatest(values, family, qualifier);
    }

    @Override
    public String toString() {
        return values.toString();
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public KeyValue getValue(int index) {
        return values.get(index);
    }

    @Override
    public boolean getKey(ImmutableBytesWritable ptr, byte[] cfPrefix) {
        for (KeyValue kv : values) {
            int len = kv.getFamilyLength();
            if (len >= cfPrefix.length 
                    && Bytes.equals(cfPrefix, 0, cfPrefix.length, kv.getBuffer(), kv.getFamilyOffset(), len)) {
                ptr.set(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
                return true;
            }
        }
        return false;
    }
}
