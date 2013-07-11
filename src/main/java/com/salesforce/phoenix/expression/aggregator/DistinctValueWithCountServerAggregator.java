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

import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.*;

/**
 * Server side Aggregator which will aggregate data and find distinct values with number of occurrences for each.
 * 
 * @author anoopsjohn
 * @since 1.2.1
 */
public class DistinctValueWithCountServerAggregator extends BaseAggregator {
    public static final int DEFAULT_ESTIMATED_DISTINCT_VALUES = 10000;
    
    private byte[] buffer = null;
    private Map<ImmutableBytesPtr, Integer> valueVsCount = new HashMap<ImmutableBytesPtr, Integer>();

    public DistinctValueWithCountServerAggregator() {
        super(null);
    }

    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        ImmutableBytesPtr key = new ImmutableBytesPtr(ptr.get(), ptr.getOffset(), ptr.getLength());
        Integer count = this.valueVsCount.get(key);
        if (count == null) {
            this.valueVsCount.put(key, 1);
        } else {
            this.valueVsCount.put(key, ++count);
        }
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        // This serializes the Map. The format is as follows
        // Map size(VInt ie. 1 to 5 bytes) +
        // ( key length [VInt ie. 1 to 5 bytes] + key bytes + value [VInt ie. 1 to 5 bytes] )*
        buffer = new byte[countMapSerializationSize()];
        int offset = 0;
        offset += ByteUtil.vintToBytes(buffer, offset, this.valueVsCount.size());
        for (Entry<ImmutableBytesPtr, Integer> entry : this.valueVsCount.entrySet()) {
            ImmutableBytesPtr key = entry.getKey();
            offset += ByteUtil.vintToBytes(buffer, offset, key.getLength());
            System.arraycopy(key.get(), key.getOffset(), buffer, offset, key.getLength());
            offset += key.getLength();
            offset += ByteUtil.vintToBytes(buffer, offset, entry.getValue().intValue());
        }
        ptr.set(buffer, 0, offset);
        return true;
    }

    // The #bytes required to serialize the count map.
    // Here let us assume to use 4 bytes for each of the int items. Normally it will consume lesser
    // bytes as we will use vints.
    // TODO Do we need to consider 5 as the number of bytes for each of the int field? Else there is
    // a chance of ArrayIndexOutOfBoundsException when all the int fields are having very large
    // values. Will that ever occur?
    private int countMapSerializationSize() {
        int size = Bytes.SIZEOF_INT;// Write the number of entries in the Map
        for (ImmutableBytesPtr key : this.valueVsCount.keySet()) {
            // Add up the key and key's lengths (Int) and the value
            size += key.getLength() + Bytes.SIZEOF_INT + Bytes.SIZEOF_INT;
        }
        return size;
    }

    // The heap size which will be taken by the count map.
    private int countMapHeapSize() {
        int size = 0;
        if (this.valueVsCount.size() > 0) {
            for (ImmutableBytesPtr key : this.valueVsCount.keySet()) {
                size += SizedUtil.MAP_ENTRY_SIZE + // entry
                        Bytes.SIZEOF_INT + // key size
                        key.getLength() + SizedUtil.ARRAY_SIZE; // value size
            }
        } else {
            // Initially when the getSize() is called, we dont have any entries in the map so as to
            // tell the exact heap need. Let us approximate the #entries
            SizedUtil.sizeOfMap(DEFAULT_ESTIMATED_DISTINCT_VALUES,
                    SizedUtil.IMMUTABLE_BYTES_PTR_SIZE, Bytes.SIZEOF_INT);
        }
        return size;
    }

    @Override
    public final PDataType getDataType() {
        return PDataType.VARBINARY;
    }

    @Override
    public void reset() {
        valueVsCount = new HashMap<ImmutableBytesPtr, Integer>();
        buffer = null;
        super.reset();
    }

    @Override
    public String toString() {
        return "DISTINCT VALUE vs COUNT";
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
