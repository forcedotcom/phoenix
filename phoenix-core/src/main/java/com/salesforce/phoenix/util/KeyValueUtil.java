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

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * Utilities for KeyValue. Where there's duplication with KeyValue methods,
 * these avoid creating new objects when not necessary (primary preventing
 * byte array copying).
 *
 * @author jtaylor
 * @since 0.1
 */
public class KeyValueUtil {
    private KeyValueUtil() {
    }

    public static KeyValue newKeyValue(Result r, byte[] cf, byte[] cq, long ts, byte[] value, int valueOffset, int valueLength) {
        byte[] bytes = ResultUtil.getRawBytes(r);
        return new KeyValue(bytes, ResultUtil.getKeyOffset(r), ResultUtil.getKeyLength(r),
                cf, 0, cf.length,
                cq, 0, cq.length,
                ts, Type.Put,
                value, valueOffset, valueLength);
    }

    public static KeyValue newKeyValue(byte[] key, byte[] cf, byte[] cq, long ts, byte[] value, int valueOffset, int valueLength) {
        return new KeyValue(key, 0, key.length,
                cf, 0, cf.length,
                cq, 0, cq.length,
                ts, Type.Put,
                value, valueOffset, valueLength);
    }

    public static KeyValue newKeyValue(ImmutableBytesWritable key, byte[] cf, byte[] cq, long ts, byte[] value, int valueOffset, int valueLength) {
        return new KeyValue(key.get(), key.getOffset(), key.getLength(),
                cf, 0, cf.length,
                cq, 0, cq.length,
                ts, Type.Put,
                value, valueOffset, valueLength);
    }

    public static KeyValue newKeyValue(byte[] key, int keyOffset, int keyLength, byte[] cf, byte[] cq, long ts, byte[] value, int valueOffset, int valueLength) {
        return new KeyValue(key, keyOffset, keyLength,
                cf, 0, cf.length,
                cq, 0, cq.length,
                ts, Type.Put,
                value, valueOffset, valueLength);
    }

    public static KeyValue newKeyValue(byte[] key, byte[] cf, byte[] cq, long ts, byte[] value) {
        return newKeyValue(key,cf,cq,ts,value,0,value.length);
    }

    public static KeyValue newKeyValue(Result r, byte[] cf, byte[] cq, long ts, byte[] value) {
        return newKeyValue(r,cf,cq,ts,value,0,value.length);
    }

    /**
     * Binary search for latest column value without allocating memory in the process
     * @param kvs
     * @param searchTerm
     */
    public static KeyValue getColumnLatest(List<KeyValue>kvs, KeyValue searchTerm) {
        if (kvs.size() == 0) {
          return null;
        }
        
        // pos === ( -(insertion point) - 1)
        int pos = Collections.binarySearch(kvs, searchTerm, KeyValue.COMPARATOR);
        // never will exact match
        if (pos < 0) {
          pos = (pos+1) * -1;
          // pos is now insertion point
        }
        if (pos == kvs.size()) {
          return null; // doesn't exist
        }
    
        KeyValue kv = kvs.get(pos);
        if (Bytes.compareTo(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(),
                searchTerm.getBuffer(), searchTerm.getFamilyOffset(), searchTerm.getFamilyLength()) != 0) {
            return null;
        }
        if (Bytes.compareTo(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(),
                searchTerm.getBuffer(), searchTerm.getQualifierOffset(), searchTerm.getQualifierLength()) != 0) {
            return null;
        }
        return kv;
    }

    /**
     * Binary search for latest column value without allocating memory in the process
     */
    public static KeyValue getColumnLatest(List<KeyValue>kvs, byte[] family, byte[] qualifier) {
        KeyValue kv = kvs.get(0);
        return KeyValueUtil.getColumnLatest(kvs, kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), family, 0, family.length, qualifier, 0, qualifier.length);
    }

    /**
     * Binary search for latest column value without allocating memory in the process
     */
    public static KeyValue getColumnLatest(List<KeyValue>kvs, byte[] row, int roffset, int rlength, byte[] family, int foffset, int flength, byte[] qualifier, int qoffset, int qlength) {
        KeyValue searchTerm = KeyValue.createFirstOnRow(row, roffset, rlength, family, foffset, flength, qualifier, qoffset, qlength);
        return getColumnLatest(kvs,searchTerm);
        
    }
}
