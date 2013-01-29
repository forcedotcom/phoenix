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
package com.salesforce.phoenix.util;

import static com.salesforce.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static com.salesforce.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Utilities for Tuple
 *
 * @author jtaylor
 * @since 0.1
 */
public class TupleUtil {
    private TupleUtil() {
    }
    
    public static boolean equals(Tuple t1, Tuple t2, ImmutableBytesWritable ptr) {
        t1.getKey(ptr);
        byte[] buf = ptr.get();
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        t2.getKey(ptr);
        return Bytes.compareTo(buf, offset, length, ptr.get(), ptr.getOffset(), ptr.getLength()) == 0;
    }
    
    public static int compare(Tuple t1, Tuple t2, ImmutableBytesWritable ptr) {
        t1.getKey(ptr);
        byte[] buf = ptr.get();
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        t2.getKey(ptr);
        return Bytes.compareTo(buf, offset, length, ptr.get(), ptr.getOffset(), ptr.getLength());
    }
    
    /**
     * Set ptr to point to the value contained in the first KeyValue without
     * exploding Result into KeyValue array.
     * @param r
     * @param ptr
     */
    public static void getAggregateValue(Tuple r, ImmutableBytesWritable ptr) {
        if (r.size() == 1) {
            KeyValue kv = r.getValue(0); // Just one KV for aggregation
            if (Bytes.compareTo(SINGLE_COLUMN_FAMILY, 0, SINGLE_COLUMN_FAMILY.length, kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength()) == 0) {
                if (Bytes.compareTo(SINGLE_COLUMN, 0, SINGLE_COLUMN.length, kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength()) == 0) {
                    ptr.set(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
                    return;
                }
            }
        }
        throw new IllegalStateException("Expected single, aggregated KeyValue from coprocessor, but instead received " + r + ". Ensure aggregating coprocessors are loaded correctly on server");
    }
    
    public static int write(Tuple result, DataOutput out) throws IOException {
        int size = 0;
        for(int i = 0; i < result.size(); i++) {
            KeyValue kv = result.getValue(i);
            size += kv.getLength();
            size += Bytes.SIZEOF_INT; // kv.getLength
          }

        WritableUtils.writeVInt(out, size);
        for(int i = 0; i < result.size(); i++) {
            KeyValue kv = result.getValue(i);
            out.writeInt(kv.getLength());
            out.write(kv.getBuffer(), kv.getOffset(), kv.getLength());
          }
        return size;
    }
}
