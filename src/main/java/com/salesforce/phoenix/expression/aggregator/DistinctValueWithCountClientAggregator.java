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

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.phoenix.exception.PhoenixIOException;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ImmutableBytesPtr;

/**
 * Client side Aggregator which will aggregate data and find distinct values with number of occurrences for each.
 * 
 * @author anoopsjohn
 * @since 1.2.1
 */
public abstract class DistinctValueWithCountClientAggregator extends BaseAggregator {
    private static final Logger LOG = LoggerFactory.getLogger(DistinctValueWithCountClientAggregator.class);

    protected Map<ImmutableBytesPtr, Integer> valueVsCount = new HashMap<ImmutableBytesPtr, Integer>();
    protected byte[] buffer;
    protected long totalCount = 0L;

    public DistinctValueWithCountClientAggregator() {
        super(null);
    }

    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(ptr.get(),
                ptr.getOffset(), ptr.getLength()));
        try {
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
            LOG.error("Error in deserializing the data from "
                    + DistinctValueWithCountServerAggregator.class.getSimpleName(), ioe);
            new PhoenixIOException(ioe);
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
    
    protected Entry<ImmutableBytesPtr, Integer>[] getSortedValueVsCount(final boolean ascending) {
        // To sort the valueVsCount
        @SuppressWarnings("unchecked")
        Entry<ImmutableBytesPtr, Integer>[] entries = new Entry[valueVsCount.size()];
        valueVsCount.entrySet().toArray(entries);
        Comparator<Entry<ImmutableBytesPtr, Integer>> comparator = new Comparator<Entry<ImmutableBytesPtr, Integer>>() {
            @Override
            public int compare(Entry<ImmutableBytesPtr, Integer> o1, Entry<ImmutableBytesPtr, Integer> o2) {
                ImmutableBytesPtr k1 = o1.getKey();
                ImmutableBytesPtr k2 = o2.getKey();
                if (ascending) {
                    return WritableComparator.compareBytes(k1.get(), k1.getOffset(), k1.getLength(),
                        k2.get(), k2.getOffset(), k2.getLength());
                }
                return WritableComparator.compareBytes(k2.get(), k2.getOffset(), k2.getLength(), k1.get(),
                        k1.getOffset(), k1.getLength());
            }
        };
        Arrays.sort(entries, comparator);
        return entries;
    }
}
