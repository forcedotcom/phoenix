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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;


public class SingleKeyValueTuple implements Tuple {
    private static final byte[] UNITIALIZED_KEY_BUFFER = new byte[0];
    private KeyValue keyValue;
    private final ImmutableBytesWritable keyPtr = new ImmutableBytesWritable(UNITIALIZED_KEY_BUFFER);
    
    public SingleKeyValueTuple() {
    }
    
    public SingleKeyValueTuple(KeyValue keyValue) {
        if (keyValue == null) {
            throw new NullPointerException();
        }
        setKeyValue(keyValue);
    }
    
    public boolean hasKey() {
        return keyPtr.get() != UNITIALIZED_KEY_BUFFER;
    }
    
    public void reset() {
        this.keyValue = null;
        keyPtr.set(UNITIALIZED_KEY_BUFFER);
    }
    
    public void setKeyValue(KeyValue keyValue) {
        if (keyValue == null) {
            throw new IllegalArgumentException();
        }
        this.keyValue = keyValue;
        setKey(keyValue);
    }
    
    public void setKey(ImmutableBytesWritable ptr) {
        keyPtr.set(ptr.get(), ptr.getOffset(), ptr.getLength());
    }
    
    public void setKey(KeyValue keyValue) {
        if (keyValue == null) {
            throw new IllegalArgumentException();
        }
        keyPtr.set(keyValue.getBuffer(), keyValue.getRowOffset(), keyValue.getRowLength());
    }
    
    @Override
    public void getKey(ImmutableBytesWritable ptr) {
        ptr.set(keyPtr.get(), keyPtr.getOffset(), keyPtr.getLength());
    }
    
    @Override
    public KeyValue getValue(byte[] cf, byte[] cq) {
        return keyValue;
    }

    @Override
    public boolean isImmutable() {
        return true;
    }
    
    @Override
    public String toString() {
        return "SingleKeyValueTuple[" + keyValue == null ? keyPtr.get() == UNITIALIZED_KEY_BUFFER ? "null" : Bytes.toStringBinary(keyPtr.get(),keyPtr.getOffset(),keyPtr.getLength()) : keyValue.toString() + "]";
    }

    @Override
    public int size() {
        return keyValue == null ? 0 : 1;
    }

    @Override
    public KeyValue getValue(int index) {
        if (index != 0 || keyValue == null) {
            throw new IndexOutOfBoundsException(Integer.toString(index));
        }
        return keyValue;
    }
}
