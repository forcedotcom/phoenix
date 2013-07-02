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
package com.salesforce.phoenix.cache;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.http.annotation.Immutable;
import org.xerial.snappy.Snappy;

import com.salesforce.phoenix.exception.PhoenixIOException;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.ExpressionType;
import com.salesforce.phoenix.memory.MemoryManager;
import com.salesforce.phoenix.memory.MemoryManager.MemoryChunk;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.tuple.ResultTuple;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.*;

/**
 * 
 * Cache per org on server side.  Tracks memory usage for each
 * org as well and rolls up usage to global memory manager.
 * 
 * TODO: Look at using Google Guava instead
 *
 * @author jtaylor
 * @since 0.1
 */
public class TenantCacheImpl implements TenantCache {
    private final ScheduledExecutorService timerExecutor;
    private final int maxHashCacheTimeToLiveMs;
    private final MemoryManager memoryManager;
    private volatile Map<ImmutableBytesWritable,AgeOutWrapper> hashCaches;

    public TenantCacheImpl(ScheduledExecutorService timerExecutor, MemoryManager memoryManager, int maxHashCacheTimeToLiveMs) {
        this.memoryManager = memoryManager;
        this.timerExecutor = timerExecutor;
        this.maxHashCacheTimeToLiveMs = maxHashCacheTimeToLiveMs;
    }
    
    @Override
    public ScheduledExecutorService getTimerExecutor() {
        return timerExecutor;
    }
    
    @Override
    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    private Map<ImmutableBytesWritable,AgeOutWrapper> getHashCaches() {
        /* Delay creation of this map until it's needed */
        if (hashCaches == null) {
            synchronized(this) {
                if (hashCaches == null) {
                    hashCaches = new ConcurrentHashMap<ImmutableBytesWritable,AgeOutWrapper>();
                }
            }
        }
        return hashCaches;
    }
    
    @Override
    public HashCache getHashCache(ImmutableBytesWritable joinId) {
        AgeOutWrapper wrapper = getHashCaches().get(joinId);
        if (wrapper == null) {
            return null;
        }
        return wrapper.getHashCache();
    }
    
    @Override
    public HashCache addHashCache(ImmutableBytesWritable joinId, ImmutableBytesWritable hashCacheBytes) throws SQLException {
        try {
            int size = Snappy.uncompressedLength(hashCacheBytes.get());
            byte[] uncompressed = new byte[size];
            Snappy.uncompress(hashCacheBytes.get(), hashCacheBytes.getOffset(), hashCacheBytes.getLength(), uncompressed, 0);
            hashCacheBytes = new ImmutableBytesWritable(uncompressed);
            final Map<ImmutableBytesWritable,AgeOutWrapper> hashCaches = getHashCaches();
            final AgeOutWrapper wrapper = new AgeOutWrapper(joinId, hashCacheBytes);
            hashCaches.put(joinId, wrapper);
            return wrapper.getHashCache();
        } catch (IOException e) {
            throw new PhoenixIOException(e);
        }
    }
    
    @Override
    public HashCache removeHashCache(ImmutableBytesWritable joinId) throws SQLException {
        AgeOutWrapper wrapper = getHashCaches().remove(joinId);
        if (wrapper == null) {
            return null;
        }
        wrapper.close();
        return wrapper.getHashCache();
    }
    
    private class AgeOutWrapper implements SQLCloseable {
        private final HashCache hashCache;
        private final ScheduledFuture<?> future;
        private volatile long lastAccessTime;
        
        private AgeOutWrapper(final ImmutableBytesWritable joinId, final ImmutableBytesWritable hashCacheBytes) {
            hashCache = new AgeOutHashCache(hashCacheBytes);
            // Setup timer task to age-out hash cache after a configurable amount of time.
            // This is necessary in case the client that added the cache dies before the
            // cache is removed.
            Callable<Void> command = new Callable<Void>() {
                @Override
                public Void call() throws SQLException {
                    if (System.currentTimeMillis() - lastAccessTime >= maxHashCacheTimeToLiveMs) {
                        removeHashCache(joinId);
                    }
                    return null;
                }
            };
            lastAccessTime = System.currentTimeMillis();
            future = timerExecutor.schedule(command, maxHashCacheTimeToLiveMs, TimeUnit.MILLISECONDS);
        }
        
        public HashCache getHashCache() {
            return hashCache;
        }

        @Override
        public void close() throws SQLException {
            try {
                future.cancel(true);
            } finally {
                hashCache.close();
            }
        }

        @Immutable
        private class AgeOutHashCache implements HashCache {
            private final Map<ImmutableBytesPtr,Tuple> hashCache;
            private final MemoryChunk memoryChunk;
            private final byte[][] cfs;
            private final byte[] tableName;
            
            private AgeOutHashCache(ImmutableBytesWritable hashCacheBytes) {
                try {
                    byte[] hashCacheByteArray = hashCacheBytes.get();
                    int offset = hashCacheBytes.getOffset();
                    ByteArrayInputStream input = new ByteArrayInputStream(hashCacheByteArray, offset, hashCacheBytes.getLength());
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
                    int estimatedSize = SizedUtil.sizeOfMap(nRows, SizedUtil.IMMUTABLE_BYTES_WRITABLE_SIZE, SizedUtil.RESULT_SIZE) + hashCacheBytes.getLength();
                    this.memoryChunk = memoryManager.allocate(estimatedSize);
                    HashMap<ImmutableBytesPtr,Tuple> hashCacheMap = new HashMap<ImmutableBytesPtr,Tuple>(nRows * 5 / 4);
                    offset += Bytes.SIZEOF_INT;
                    // Build Map with evaluated hash key as key and row as value
                    for (int i = 0; i < nRows; i++) {
                        int resultSize = (int)Bytes.readVLong(hashCacheByteArray, offset);
                        offset += WritableUtils.decodeVIntSize(hashCacheByteArray[offset]);
                        ImmutableBytesWritable value = new ImmutableBytesWritable(hashCacheByteArray,offset,resultSize);
                        Tuple result = new ResultTuple(new Result(value));
                        ImmutableBytesPtr key = getHashKey(result, onExpressions);
                        hashCacheMap.put(key, result);
                        offset += resultSize;
                    }
                    int cfCount = (int)Bytes.readVLong(hashCacheByteArray, offset);
                    if (cfCount == 0) {
                        cfs = null;
                        tableName = null;
                    } else {
                        offset += WritableUtils.decodeVIntSize(hashCacheByteArray[offset]);
                        byte[][] tableInfo = ByteUtil.toByteArrays(hashCacheByteArray, offset, cfCount);
                        tableName = tableInfo[0];
                        cfs = new byte[cfCount - 1][];
                        for (int i = 1; i < cfCount; i++) {
                            cfs[i-1] = tableInfo[i];
                        }
                    }
                    this.hashCache = Collections.unmodifiableMap(hashCacheMap);
                } catch (IOException e) { // Not possible with ByteArrayInputStream
                    throw new RuntimeException(e);
                }
            }
            
            private ImmutableBytesPtr getHashKey(Tuple result, List<Expression> expressions) throws IOException {
                ImmutableBytesPtr value = new ImmutableBytesPtr(ByteUtil.EMPTY_BYTE_ARRAY);
                Expression expression = expressions.get(0);
                boolean evaluated = expression.evaluate(result, value);
                
                if (expressions.size() == 1) {
                    if (!evaluated) {
                        value.set(ByteUtil.EMPTY_BYTE_ARRAY);
                    }
                    return value;
                } else {
                    TrustedByteArrayOutputStream output = new TrustedByteArrayOutputStream(value.getLength() * expressions.size());
                    try {
                        if (evaluated) {
                            output.write(value.get(), value.getOffset(), value.getLength());
                        }
                        for (int i = 1; i < expressions.size(); i++) {
                            if (!expression.getDataType().isFixedWidth()) {
                                output.write(QueryConstants.SEPARATOR_BYTE);
                            }
                            expression = expressions.get(i);
                            // TODO: should we track trailing null values and omit the separator bytes?
                            if (expression.evaluate(result, value)) {
                                output.write(value.get(), value.getOffset(), value.getLength());
                            } else if (i < expressions.size()-1 && expression.getDataType().isFixedWidth()) {
                                // This should never happen, because any non terminating nullable fixed width type (i.e. INT or LONG) is
                                // converted to a variable length type (i.e. DECIMAL) to allow an empty byte array to represent null.
                                throw new DoNotRetryIOException("Non terminating null value found for fixed width ON expression (" + expression + ") in row: " + result);
                            }
                        }
                        byte[] outputBytes = output.getBuffer();
                        value.set(outputBytes, 0, output.size());
                        return value;
                    } finally {
                        output.close();
                    }
                }
            }
    
            @Override
            public void close() {
                memoryChunk.close();
            }
    
            @Override
            public byte[] getTableName() {
                return tableName;
            }
            
            @Override
            public byte[][] getColumnFamilies() {
                return cfs;
            }
            
            @Override
            public Tuple get(ImmutableBytesWritable hashKey) {
                lastAccessTime = System.currentTimeMillis();
                return hashCache.get(new ImmutableBytesPtr(hashKey));
            }
        }
    }
}
