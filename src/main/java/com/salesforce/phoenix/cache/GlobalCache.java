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

import static com.salesforce.phoenix.query.QueryServices.*;

import java.util.concurrent.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.memory.ChildMemoryManager;
import com.salesforce.phoenix.memory.GlobalMemoryManager;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.query.QueryServicesOptions;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.util.ImmutableBytesPtr;


/**
 * 
 * Global root cache for the server. Each tenant is managed as a child tenant cache of this one. Queries
 * not associated with a particular tenant use this as their tenant cache.
 *
 * @author jtaylor
 * @since 0.1
 */
public class GlobalCache extends TenantCacheImpl {
    public static final String HASH_CACHE_AGE_OUT_THREAD_NAME = "PhoenixHashCacheAgeOutThread";
    private static volatile GlobalCache INSTANCE = null; 
    
    private final Configuration config;
    private final ConcurrentMap<ImmutableBytesWritable,TenantCache> perTenantCacheMap = new ConcurrentHashMap<ImmutableBytesWritable,TenantCache>();
    // Cache for lastest PTable for a given Phoenix table
    private final ConcurrentHashMap<ImmutableBytesPtr,PTable> metaDataCacheMap = new ConcurrentHashMap<ImmutableBytesPtr,PTable>();
    
    public static GlobalCache getInstance(Configuration config) {
        if (INSTANCE == null) {
            synchronized(GlobalCache.class) {
                if (INSTANCE == null) {
                    INSTANCE = new GlobalCache(config);
                }
            }
        }
        return INSTANCE;
    }
    
    public ConcurrentHashMap<ImmutableBytesPtr,PTable> getMetaDataCache() {
        return metaDataCacheMap;
    }
    
    /**
     * Get the tenant cache associated with the tenantId. If tenantId is not applicable, null may be
     * used in which case a global tenant cache is returned.
     * @param config the HBase configuration
     * @param tenantId the tenant ID or null if not applicable.
     * @return TenantCache
     */
    public static TenantCache getTenantCache(Configuration config, ImmutableBytesWritable tenantId) {
        GlobalCache globalCache = GlobalCache.getInstance(config);
        TenantCache tenantCache = tenantId == null ? globalCache : globalCache.getChildTenantCache(tenantId);      
        return tenantCache;
    }
    
    private GlobalCache(Configuration config) {
        super(Executors.newSingleThreadScheduledExecutor(),
              new GlobalMemoryManager(Runtime.getRuntime().totalMemory() * 
                                          config.getInt(MAX_MEMORY_PERC_ATTRIB, QueryServicesOptions.DEFAULT_MAX_MEMORY_PERC) / 100,
                                      config.getInt(MAX_MEMORY_WAIT_MS_ATTRIB, QueryServicesOptions.DEFAULT_MAX_MEMORY_WAIT_MS)),
              config.getInt(QueryServices.MAX_HASH_CACHE_TIME_TO_LIVE_MS, QueryServicesOptions.DEFAULT_MAX_HASH_CACHE_TIME_TO_LIVE_MS));
        this.config = config;
    }
    
    public Configuration getConfig() {
        return config;
    }
    
    /**
     * Retrieve the tenant cache given an tenantId.
     * @param tenantId the ID that identifies the tenant
     * @return the existing or newly created TenantCache
     */
    public TenantCache getChildTenantCache(ImmutableBytesWritable tenantId) {
        TenantCache tenantCache = perTenantCacheMap.get(tenantId);
        if (tenantCache == null) {
            int maxTenantMemoryPerc = config.getInt(MAX_TENANT_MEMORY_PERC_ATTRIB, QueryServicesOptions.DEFAULT_MAX_TENANT_MEMORY_PERC);
            int maxHashCacheTimeToLive = config.getInt(QueryServices.MAX_HASH_CACHE_TIME_TO_LIVE_MS, QueryServicesOptions.DEFAULT_MAX_HASH_CACHE_TIME_TO_LIVE_MS);
            TenantCacheImpl newTenantCache = new TenantCacheImpl(getTimerExecutor(), new ChildMemoryManager(getMemoryManager(), maxTenantMemoryPerc), maxHashCacheTimeToLive);
            tenantCache = perTenantCacheMap.putIfAbsent(tenantId, newTenantCache);
            if (tenantCache == null) {
                tenantCache = newTenantCache;
            }
        }
        return tenantCache;
    }
}
