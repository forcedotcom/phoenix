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
package com.salesforce.phoenix.join;

import java.io.*;
import java.sql.SQLException;
import java.util.*;

import net.jcip.annotations.Immutable;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.xerial.snappy.Snappy;

import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.cache.HashCache;
import com.salesforce.phoenix.coprocessor.ServerCachingProtocol.ServerCacheFactory;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.ExpressionType;
import com.salesforce.phoenix.memory.MemoryManager.MemoryChunk;
import com.salesforce.phoenix.schema.tuple.ResultTuple;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.*;

public class HashCacheFactory implements ServerCacheFactory {

    public HashCacheFactory() {
    }

    @Override
    public void readFields(DataInput input) throws IOException {
    }

    @Override
    public void write(DataOutput output) throws IOException {
    }

    @Override
    public Closeable newCache(ImmutableBytesWritable cachePtr, MemoryChunk chunk) throws SQLException {
        try {
            int size = Snappy.uncompressedLength(cachePtr.get());
            byte[] uncompressed = new byte[size];
            Snappy.uncompress(cachePtr.get(), 0, cachePtr.getLength(), uncompressed, 0);
            return new HashCacheImpl(uncompressed, chunk);
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        }
    }

    @Immutable
    private class HashCacheImpl implements HashCache {
        private final Map<ImmutableBytesPtr,List<Tuple>> hashCache;
        private final MemoryChunk memoryChunk;
        
        private HashCacheImpl(byte[] hashCacheBytes, MemoryChunk memoryChunk) {
            try {
                this.memoryChunk = memoryChunk;
                int offset = 0;
                ByteArrayInputStream input = new ByteArrayInputStream(hashCacheBytes, offset, hashCacheBytes.length);
                DataInputStream dataInput = new DataInputStream(input);
                int nExprs = dataInput.readInt();
                List<Expression> onExpressions = new ArrayList<Expression>(nExprs);
                for (int i = 0; i < nExprs; i++) {
                    int expressionOrdinal = WritableUtils.readVInt(dataInput);
                    Expression expression = ExpressionType.values()[expressionOrdinal].newInstance();
                    expression.readFields(dataInput);
                    onExpressions.add(expression);                        
                }
                int exprSize = dataInput.readInt();
                offset += exprSize;
                int nRows = dataInput.readInt();
                int estimatedSize = SizedUtil.sizeOfMap(nRows, SizedUtil.IMMUTABLE_BYTES_WRITABLE_SIZE, SizedUtil.RESULT_SIZE) + hashCacheBytes.length;
                this.memoryChunk.resize(estimatedSize);
                HashMap<ImmutableBytesPtr,List<Tuple>> hashCacheMap = new HashMap<ImmutableBytesPtr,List<Tuple>>(nRows * 5 / 4);
                offset += Bytes.SIZEOF_INT;
                // Build Map with evaluated hash key as key and row as value
                for (int i = 0; i < nRows; i++) {
                    int resultSize = (int)Bytes.readVLong(hashCacheBytes, offset);
                    offset += WritableUtils.decodeVIntSize(hashCacheBytes[offset]);
                    ImmutableBytesWritable value = new ImmutableBytesWritable(hashCacheBytes,offset,resultSize);
                    Tuple result = new ResultTuple(new Result(value));
                    ImmutableBytesPtr key = TupleUtil.getConcatenatedValue(result, onExpressions);
                    List<Tuple> tuples = hashCacheMap.get(key);
                    if (tuples == null) {
                        tuples = new ArrayList<Tuple>(1);
                        hashCacheMap.put(key, tuples);
                    }
                    tuples.add(result);
                    offset += resultSize;
                }
                this.hashCache = Collections.unmodifiableMap(hashCacheMap);
            } catch (IOException e) { // Not possible with ByteArrayInputStream
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            memoryChunk.close();
        }
        
        @Override
        public List<Tuple> get(ImmutableBytesPtr hashKey) {
            return hashCache.get(hashKey);
        }
    }
}
