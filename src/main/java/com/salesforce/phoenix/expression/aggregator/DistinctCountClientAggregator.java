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
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.phoenix.exception.PhoenixIOException;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;

/**
 * 
 * Client side Aggregator for DISTINCT COUNT aggregations
 * 
 * @author anoopsjohn
 * @since 1.2.1
 */
public class DistinctCountClientAggregator extends BaseAggregator {
    private static final Logger LOG = LoggerFactory.getLogger(DistinctCountClientAggregator.class);

    private Set<byte[]> uniqueColValues = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

    private byte[] buffer;

    public DistinctCountClientAggregator() {
        super(null);
    }

    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        byte[] mapBytes = ptr.copyBytes();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(mapBytes));
        try {
            int mapSize = in.readInt();
            for (int i = 0; i < mapSize; i++) {
                int keyLen = in.readInt();
                byte[] key = new byte[keyLen];
                in.read(key, 0, keyLen);
                // TODO Right now we don't need this count.
                int value = in.readInt();
                uniqueColValues.add(key);
            }
        } catch (IOException ioe) {
            LOG.error("Error in deserializing the data from DistinctCountAggregator", ioe);
            new PhoenixIOException(ioe);
        }
        if (buffer == null) {
            initBuffer();
        }
    }

    private int getBufferLength() {
        return PDataType.LONG.getByteSize();
    }

    private void initBuffer() {
        buffer = new byte[getBufferLength()];
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (buffer == null) {
            if (isNullable()) {
                return false;
            }
            initBuffer();
        }
        long value = this.uniqueColValues.size();
        byte[] valueBytes = Bytes.toBytes(value);
        System.arraycopy(valueBytes, 0, buffer, 0, valueBytes.length);
        ptr.set(buffer);
        return true;
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
        uniqueColValues = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        buffer = null;
        super.reset();
    }
}
