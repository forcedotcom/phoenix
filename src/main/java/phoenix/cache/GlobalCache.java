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
package phoenix.cache;

import static phoenix.query.QueryServices.*;

import java.util.concurrent.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import phoenix.memory.ChildMemoryManager;
import phoenix.memory.GlobalMemoryManager;
import phoenix.schema.PTable;
import phoenix.util.ImmutableBytesPtr;

/**
 * 
 * Global root cache for the server. Each tenant is managed as a child tenant cache of this one. Queries
 * not associated with a particular tenant use this as their tenant cache.
 *
 * @author jtaylor
 * @since 0.1
 */
public class GlobalCache extends TenantCacheImpl {
    public static final String MAX_CP_ORG_MEMORY_PERC_ATTRIB = "phoenix.coprocessor.maxOrgMemoryPercentage";
    public static final String MAX_CP_MEMORY_WAIT_MS_ATTRIB = "phoenix.coprocessor.maxGlobalMemoryWaitMs";
    public static final String MAX_CP_MEMORY_BYTES_ATTRIB = "phoenix.coprocessor.maxGlobalMemoryBytes";
    public static final String MAX_HASH_CACHE_TIME_TO_LIVE_MS = "phoenix.coprocessor.maxHashCacheTimeToLiveMs";
    public static final String HASH_CACHE_AGE_OUT_THREAD_NAME = "PhoenixHashCacheAgeOutThread";
    
    public static final int MAX_COPROCESSOR_MEMORY_BYTES = 1024 * 1024 * 500; // 500 M
    public static final int MAX_COPROCESSOR_MEMORY_WAIT_MS = 1000; // 1 sec
    public static final int MAX_ORG_MEMORY_PERCENTAGE = 30; // 30%
    public static final int DEFAULT_MAX_HASH_CACHE_TIME_TO_LIVE_MS = 30000; // 30 sec (with no activity)
    
    private static volatile GlobalCache INSTANCE = null; 
    
    private final Configuration config;
    private final ConcurrentMap<ImmutableBytesWritable,TenantCache> perOrgCacheMap = new ConcurrentHashMap<ImmutableBytesWritable,TenantCache>();
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
     * @return
     */
    public static TenantCache getTenantCache(Configuration config, ImmutableBytesWritable tenantId) {
        GlobalCache globalCache = GlobalCache.getInstance(config);
        TenantCache tenantCache = tenantId == null ? globalCache : globalCache.getChildTenantCache(tenantId);      
        return tenantCache;
    }
    
    private GlobalCache(Configuration config) {
        super(Executors.newSingleThreadScheduledExecutor(),
              new GlobalMemoryManager(config.getLong(GlobalCache.MAX_CP_MEMORY_BYTES_ATTRIB, config.getInt(MAX_MEMORY_BYTES_ATTRIB, MAX_COPROCESSOR_MEMORY_BYTES)),
                                      config.getInt(GlobalCache.MAX_CP_MEMORY_WAIT_MS_ATTRIB, config.getInt(MAX_MEMORY_WAIT_MS_ATTRIB, MAX_COPROCESSOR_MEMORY_BYTES))),
              config.getInt(GlobalCache.MAX_HASH_CACHE_TIME_TO_LIVE_MS, config.getInt(MAX_HASH_CACHE_TIME_TO_LIVE_MS, MAX_ORG_MEMORY_PERCENTAGE)));
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
        TenantCache tenantCache = perOrgCacheMap.get(tenantId);
        if (tenantCache == null) {
            int maxOrgMemoryPerc = config.getInt(GlobalCache.MAX_CP_ORG_MEMORY_PERC_ATTRIB, config.getInt(MAX_ORG_MEMORY_PERC_ATTRIB, MAX_ORG_MEMORY_PERCENTAGE));
            int maxHashCacheTimeToLive = config.getInt(GlobalCache.MAX_HASH_CACHE_TIME_TO_LIVE_MS, config.getInt(MAX_HASH_CACHE_TIME_TO_LIVE_MS, MAX_ORG_MEMORY_PERCENTAGE));
            TenantCacheImpl newTenantCache = new TenantCacheImpl(getTimerExecutor(), new ChildMemoryManager(getMemoryManager(), maxOrgMemoryPerc), maxHashCacheTimeToLive);
            tenantCache = perOrgCacheMap.putIfAbsent(tenantId, newTenantCache);
            if (tenantCache == null) {
                tenantCache = newTenantCache;
            }
        }
        return tenantCache;
    }
}
