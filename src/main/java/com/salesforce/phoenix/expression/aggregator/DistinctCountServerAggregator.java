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
package com.salesforce.phoenix.expression.aggregator;

import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.SizedUtil;

/**
 * 
 * Server side Aggregator for DISTINCT COUNT aggregations
 * 
 * @author anoopsjohn
 * @since 1.2.1
 */
public class DistinctCountServerAggregator extends BaseAggregator {
    private byte[] buffer = null;
    private Expression expression = null;

    private TreeMap<byte[], Integer> countMap = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);

    public DistinctCountServerAggregator(List<Expression> expressions) {
        super(null);
        this.expression = expressions.get(0);
    }

    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        ImmutableBytesWritable colValue = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
        if (expression.evaluate(tuple, colValue)) {
            byte[] key = colValue.copyBytes();
            Integer count = this.countMap.get(key);
            if (count == null) {
                this.countMap.put(key, 1);
            } else {
                this.countMap.put(key, count++);
            }
        }
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        // This serializes the Map. The format is as follows
        // Map size(Int ie. 4 bytes) + 
        // ( key length [Int ie. 4 bytes] + key bytes + value [Int ie. 4 bytes] )*
        buffer = new byte[countMapSerializationSize()];
        byte[] rowsInMap = Bytes.toBytes(this.countMap.size());
        int offset = 0;
        System.arraycopy(rowsInMap, 0, buffer, offset, rowsInMap.length);
        offset += rowsInMap.length;
        for (Entry<byte[], Integer> entry : this.countMap.entrySet()) {
            byte[] key = entry.getKey();
            byte[] keyLen = Bytes.toBytes(key.length);
            System.arraycopy(keyLen, 0, buffer, offset, keyLen.length);
            offset += keyLen.length;
            System.arraycopy(key, 0, buffer, offset, key.length);
            offset += key.length;
            byte[] value = Bytes.toBytes(entry.getValue().intValue());
            System.arraycopy(value, 0, buffer, offset, value.length);
            offset += value.length;
        }
        ptr.set(buffer);
        return true;
    }

    // The #bytes required to serialize the count map.
    private int countMapSerializationSize() {
        int size = Bytes.SIZEOF_INT;// Write the number of entries in the Map
        for (byte[] key : this.countMap.keySet()) {
            // Add up the key and key's lengths (Int) and the value
            size += key.length + Bytes.SIZEOF_INT + Bytes.SIZEOF_INT;
        }
        return size;
    }

    // The heap size which will be taken by the count map.
    private int countMapHeapSize() {
        int size = 0;
        for (byte[] key : this.countMap.keySet()) {
            size += SizedUtil.MAP_ENTRY_SIZE + // entry
                    Bytes.SIZEOF_INT + // key size
                    key.length + SizedUtil.ARRAY_SIZE; // value size
        }
        return size;
    }
    
    @Override
    public final PDataType getDataType() {
        return PDataType.VARBINARY;
    }

    @Override
    public void reset() {
        countMap = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
        buffer = null;
        super.reset();
    }

    @Override
    public String toString() {
        return "DISTINCT COUNT";
    }

    @Override
    public int getSize() {
        // TODO make this size correct.??
        // This size is being called initially at the begin of the scanner open. At that time we any
        // way can not tell the exact size of the Map. The Aggregators get size from all Aggregator
        // and stores in a variable for future use. This size of the Aggregators is being used in
        // Grouped unordered scan. Do we need some changes there in that calculation?
        return super.getSize() + SizedUtil.ARRAY_SIZE + countMapHeapSize();
    }
}
