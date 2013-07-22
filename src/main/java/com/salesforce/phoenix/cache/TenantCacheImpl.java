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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.http.annotation.Immutable;
import org.xerial.snappy.Snappy;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.ExpressionType;
import com.salesforce.phoenix.memory.MemoryManager;
import com.salesforce.phoenix.memory.MemoryManager.MemoryChunk;
import com.salesforce.phoenix.schema.tuple.ResultTuple;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.ImmutableBytesPtr;
import com.salesforce.phoenix.util.SQLCloseable;
import com.salesforce.phoenix.util.ServerUtil;
import com.salesforce.phoenix.util.SizedUtil;
import com.salesforce.phoenix.util.TupleUtil;

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
            throw ServerUtil.parseServerException(e);
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
            private final Map<ImmutableBytesPtr,List<Tuple>> hashCache;
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
                    HashMap<ImmutableBytesPtr,List<Tuple>> hashCacheMap = new HashMap<ImmutableBytesPtr,List<Tuple>>(nRows * 5 / 4);
                    offset += Bytes.SIZEOF_INT;
                    // Build Map with evaluated hash key as key and row as value
                    for (int i = 0; i < nRows; i++) {
                        int resultSize = (int)Bytes.readVLong(hashCacheByteArray, offset);
                        offset += WritableUtils.decodeVIntSize(hashCacheByteArray[offset]);
                        ImmutableBytesWritable value = new ImmutableBytesWritable(hashCacheByteArray,offset,resultSize);
                        Tuple result = new ResultTuple(new Result(value));
                        ImmutableBytesPtr key = new ImmutableBytesPtr(TupleUtil.getConcatenatedValue(result, onExpressions));
                        List<Tuple> tuples = hashCacheMap.get(key);
                        if (tuples == null) {
                            tuples = new ArrayList<Tuple>(1);
                            hashCacheMap.put(key, tuples);
                        }
                        tuples.add(result);
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
            public List<Tuple> get(ImmutableBytesWritable hashKey) {
                lastAccessTime = System.currentTimeMillis();
                return hashCache.get(new ImmutableBytesPtr(hashKey));
            }
        }
    }
}
