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

import java.io.Closeable;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.cache.*;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.coprocessor.ServerCachingProtocol.ServerCacheFactory;
import com.salesforce.phoenix.memory.MemoryManager;
import com.salesforce.phoenix.memory.MemoryManager.MemoryChunk;
import com.salesforce.phoenix.util.Closeables;

/**
 * 
 * Cache per tenant on server side.  Tracks memory usage for each
 * tenat as well and rolling up usage to global memory manager.
 * 
 * @author jtaylor
 * @since 0.1
 */
public class TenantCacheImpl implements TenantCache {
    private final int maxTimeToLiveMs;
    private final MemoryManager memoryManager;
    private volatile Cache<ImmutableBytesPtr, Closeable> serverCaches;

    public TenantCacheImpl(MemoryManager memoryManager, int maxTimeToLiveMs) {
        this.memoryManager = memoryManager;
        this.maxTimeToLiveMs = maxTimeToLiveMs;
    }
    
    @Override
    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    private Cache<ImmutableBytesPtr,Closeable> getServerCaches() {
        /* Delay creation of this map until it's needed */
        if (serverCaches == null) {
            synchronized(this) {
                if (serverCaches == null) {
                    serverCaches = CacheBuilder.newBuilder()
                        .expireAfterAccess(maxTimeToLiveMs, TimeUnit.MILLISECONDS)
                        .removalListener(new RemovalListener<ImmutableBytesPtr, Closeable>(){
                            @Override
                            public void onRemoval(RemovalNotification<ImmutableBytesPtr, Closeable> notification) {
                                Closeables.closeAllQuietly(Collections.singletonList(notification.getValue()));
                            }
                        })
                        .build();
                }
            }
        }
        return serverCaches;
    }
    
    @Override
    public Closeable getServerCache(ImmutableBytesPtr cacheId) {
        return getServerCaches().getIfPresent(cacheId);
    }
    
    @Override
    public Closeable addServerCache(ImmutableBytesPtr cacheId, ImmutableBytesWritable cachePtr, ServerCacheFactory cacheFactory) throws SQLException {
        MemoryChunk chunk = this.getMemoryManager().allocate(cachePtr.getLength());
        Closeable element = cacheFactory.newCache(cachePtr, chunk);
        getServerCaches().put(cacheId, element);
        return element;
    }
    
    @Override
    public void removeServerCache(ImmutableBytesPtr cacheId) throws SQLException {
        getServerCaches().invalidate(cacheId);
    }
}
