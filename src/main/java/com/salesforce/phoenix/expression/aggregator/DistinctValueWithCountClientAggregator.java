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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;

/**
 * Client side Aggregator which will aggregate data and find distinct values with number of occurrences for each.
 * 
 * @author anoopsjohn
 * @since 1.2.1
 */
public abstract class DistinctValueWithCountClientAggregator extends BaseAggregator {
    protected Map<ImmutableBytesPtr, Integer> valueVsCount = new HashMap<ImmutableBytesPtr, Integer>();
    protected byte[] buffer;
    protected long totalCount = 0L;

    public DistinctValueWithCountClientAggregator(ColumnModifier columnModifier) {
        super(columnModifier);
    }

    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        InputStream is = new ByteArrayInputStream(ptr.get(), ptr.getOffset() + 1, ptr.getLength() - 1);
        try {
            if (Bytes.equals(ptr.get(), ptr.getOffset(), 1, DistinctValueWithCountServerAggregator.COMPRESS_MARKER, 0,
                    1)) {
                is = DistinctValueWithCountServerAggregator.COMPRESS_ALGO
                        .createDecompressionStream(is,
                                DistinctValueWithCountServerAggregator.COMPRESS_ALGO.getDecompressor(), 0);
            }
            DataInputStream in = new DataInputStream(is);
            int mapSize = WritableUtils.readVInt(in);
            for (int i = 0; i < mapSize; i++) {
                int keyLen = WritableUtils.readVInt(in);
                byte[] keyBytes = new byte[keyLen];
                in.read(keyBytes, 0, keyLen);
                ImmutableBytesPtr key = new ImmutableBytesPtr(keyBytes);
                int value = WritableUtils.readVInt(in);
                Integer curCount = valueVsCount.get(key);
                if (curCount == null) {
                    valueVsCount.put(key, value);
                } else {
                    valueVsCount.put(key, curCount + value);
                }
                totalCount += value;
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe); // Impossible as we're using a ByteArrayInputStream
        }
        if (buffer == null) {
            initBuffer();
        }
    }

    protected abstract int getBufferLength();

    protected void initBuffer() {
        buffer = new byte[getBufferLength()];
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public PDataType getDataType() {
        return PDataType.VARBINARY;
    }

    @Override
    public void reset() {
        valueVsCount = new HashMap<ImmutableBytesPtr, Integer>();
        buffer = null;
        totalCount = 0L;
        super.reset();
    }
    
    protected Map<Object, Integer> getSortedValueVsCount(final boolean ascending, final PDataType type) {
        // To sort the valueVsCount
        Comparator<Object> comparator = new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
                if (ascending) { 
                    return type.compareTo(o1, o2); 
                }
                return type.compareTo(o2, o1);
            }
        };
        Map<Object, Integer> sorted = new TreeMap<Object, Integer>(comparator);
        for (Entry<ImmutableBytesPtr, Integer> entry : valueVsCount.entrySet()) {
            sorted.put(type.toObject(entry.getKey(), columnModifier), entry.getValue());
        }
        return sorted;
    }
}
