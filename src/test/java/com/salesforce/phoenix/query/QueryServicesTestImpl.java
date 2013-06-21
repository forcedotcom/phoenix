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
package com.salesforce.phoenix.query;

import static com.salesforce.phoenix.query.QueryServicesOptions.withDefaults;

import com.salesforce.phoenix.util.ReadOnlyProps;


/**
 * QueryServices implementation to use for tests that do not execute queries
 *
 * @author jtaylor
 * @since 0.1
 */
public final class QueryServicesTestImpl extends BaseQueryServicesImpl {

    private static final int DEFAULT_THREAD_POOL_SIZE = 8;
    private static final int DEFAULT_QUEUE_SIZE = 0;
    // TODO: setting this down to 5mb causes insufficient memory exceptions. Need to investigate why
    private static final int DEFAULT_MAX_MEMORY_PERC = 50; // 50% of heap
    private static final int DEFAULT_THREAD_TIMEOUT_MS = 60000*5; //5min
    private static final int DEFAULT_SPOOL_THRESHOLD_BYTES = 1024 * 1024; // 1m
    private static final int DEFAULT_MAX_MEMORY_WAIT_MS = 0;
    private static final int DEFAULT_MAX_TENANT_MEMORY_PERC = 100;
    private static final int DEFAULT_MAX_HASH_CACHE_TIME_TO_LIVE_MS = 60000 * 10; // 10min (to prevent age-out of hash cache during debugging)
    private static final long DEFAULT_MAX_HASH_CACHE_SIZE = 1024*1024*10;  // 10 Mb
    private static final int DEFAULT_TARGET_QUERY_CONCURRENCY = 4;
    private static final int DEFAULT_MAX_QUERY_CONCURRENCY = 8;
    
    private static final int DEFAULT_MASTER_INFO_PORT = -1;
    private static final int DEFAULT_REGIONSERVER_INFO_PORT = -1;
    private static final int DEFAULT_REGIONSERVER_LEASE_PERIOD_MS = 9000000;
    private static final int DEFAULT_RPC_TIMEOUT_MS = 9000000;
    
    public QueryServicesTestImpl() {
        this(ReadOnlyProps.EMPTY_PROPS);
    }
    
    public QueryServicesTestImpl(ReadOnlyProps overrideProps) {
        super(withDefaults()
                .setThreadPoolSize(DEFAULT_THREAD_POOL_SIZE)
                .setQueueSize(DEFAULT_QUEUE_SIZE)
                .setMaxMemoryPerc(DEFAULT_MAX_MEMORY_PERC)
                .setThreadTimeoutMs(DEFAULT_THREAD_TIMEOUT_MS)
                .setSpoolThresholdBytes(DEFAULT_SPOOL_THRESHOLD_BYTES)
                .setMaxMemoryWaitMs(DEFAULT_MAX_MEMORY_WAIT_MS)
                .setMaxTenantMemoryPerc(DEFAULT_MAX_TENANT_MEMORY_PERC)
                .setMaxHashCacheSize(DEFAULT_MAX_HASH_CACHE_SIZE)
                .setTargetQueryConcurrency(DEFAULT_TARGET_QUERY_CONCURRENCY)
                .setMaxQueryConcurrency(DEFAULT_MAX_QUERY_CONCURRENCY)
                .setRowKeyOrderSaltedTable(true)
                .setMaxHashCacheTTLMs(DEFAULT_MAX_HASH_CACHE_TIME_TO_LIVE_MS)
                .setMasterInfoPort(DEFAULT_MASTER_INFO_PORT)
                .setRegionServerInfoPort(DEFAULT_REGIONSERVER_INFO_PORT)
                .setRegionServerLeasePeriodMs(DEFAULT_REGIONSERVER_LEASE_PERIOD_MS)
                .setRpcTimeoutMs(DEFAULT_RPC_TIMEOUT_MS)
                .setAll(overrideProps)
        );
    }    
}
