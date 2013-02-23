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

import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.annotation.Immutable;

import com.salesforce.phoenix.memory.MemoryManager;
import com.salesforce.phoenix.util.SQLCloseable;



/**
 * 
 * Interface to group together services needed during querying.  The
 * following parameters may be set in
 * {@link org.apache.hadoop.conf.Configuration}:
 * <ul>
 *   <li><strong>phoenix.query.timeoutMs</strong>: number of milliseconds
 *   after which a query will timeout on the client. Defaults to
 *   {@link com.salesforce.phoenix.query.QueryServicesOptions#DEFAULT_THREAD_TIMEOUT_MS}.</li>
 *   <li><strong>phoenix.query.keepAliveMs</strong>: when the number of
 *     threads is greater than the core in the client side thread pool
 *     executor, this is the maximum time in milliseconds that excess idle
 *     threads will wait for a new tasks before terminating. Defaults to
 *     {@link com.salesforce.phoenix.query.QueryServicesOptions#DEFAULT_KEEP_ALIVE_MS}.</li>
 *   <li><strong>phoenix.query.threadPoolSize</strong>: number of threads
 *     in client side thread pool executor. As the number of machines/cores
 *     in the cluster grows, this value should be increased. Defaults to
 *     {@link com.salesforce.phoenix.query.QueryServicesOptions#DEFAULT_THREAD_POOL_SIZE}.</li>
 *   <li><strong>phoenix.query.queueSize</strong>: max queue depth of the
 *     bounded round robin backing the client side thread pool executor,
 *     beyond which attempts to queue additional work cause the client to
 *     block. If zero, a SynchronousQueue is used of the bounded round
 *     robin queue. Defaults to 
 *     {@link com.salesforce.phoenix.query.QueryServicesOptions#DEFAULT_QUEUE_SIZE}.</li>
 *   <li><strong>phoenix.query.spoolThresholdBytes</strong>: threshold
 *     size in bytes after which results from parallel executed aggregate
 *     query results are spooled to disk. Defaults to
 *     {@link com.salesforce.phoenix.query.QueryServicesOptions#DEFAULT_SPOOL_THRESHOLD_BYTES}.</li>
 *   <li><strong>phoenix.query.maxGlobalMemoryPercentage</strong>: percentage of total 
 *     memory ({@link java.lang.Runtime.getRuntime()#totalMemory}) that all threads
 *     may use. Only course grain memory usage is tracked, mainly accounting for memory
 *     usage in the intermediate map built during group by aggregation.  When this limit
 *     is reached the clients block attempting to get more memory, essentially throttling 
 *     memory usage. Defaults to
 *     {@link com.salesforce.phoenix.query.QueryServicesOptions#DEFAULT_MAX_MEMORY_PERC}.</li>
 *   <li><strong>phoenix.query.maxGlobalMemoryWaitMs</strong>: maximum
 *     amount of time that a client will block while waiting for more memory
 *     to become available.  After this amount of time, a
 *     {@link com.salesforce.phoenix.memory.InsufficientMemoryException} is
 *     thrown. Defaults to
 *     {@link com.salesforce.phoenix.query.QueryServicesOptions#DEFAULT_MAX_MEMORY_WAIT_MS}.</li>
 *   <li><strong>phoenix.query.maxTenantMemoryPercentage</strong>: maximum
 *     percentage of phoenix.query.maxGlobalMemoryPercentage that any one tenant
 *     is allowed to consume. After this percentage, a
 *     {@link com.salesforce.phoenix.memory.InsufficientMemoryException} is
 *     thrown. Defaults to
 *     {@link com.salesforce.phoenix.query.QueryServicesOptions#DEFAULT_MAX_TENANT_MEMORY_PERC}.</li>
 *   <li><strong>phoenix.query.targetConcurrency</strong>: target concurrent
 *     threads to use for a query. It serves as a soft limit on the number of
 *     scans into which a query may be split. A hard limit is imposed by
 *     phoenix.query.maxConcurrency (which should not be exceeded by this
 *     value). Defaults to
 *     {@link com.salesforce.phoenix.query.QueryServicesOptions#DEFAULT_TARGET_QUERY_CONCURRENCY}.</li>
 *   <li><strong>phoenix.query.maxConcurrency</strong>: maximum concurrent
 *     threads to use for a query. It servers as a hard limit on the number
 *     of scans into which a query may be split. A soft limit is imposed by
 *     phoenix.query.targetConcurrency. Defaults to
 *     {@link com.salesforce.phoenix.query.QueryServicesOptions#DEFAULT_MAX_QUERY_CONCURRENCY}.</li>
 *   <li><strong>phoenix.query.dateFormat</strong>: default pattern to use
 *     for convertion of a date to/from a string, whether through the
 *     TO_CHAR(<date>) or TO_DATE(<dateAsString>) functions, or through
 *     resultSet.getString(<date-column>). Defaults to
 *     {@link com.salesforce.phoenix.query.QueryServicesOptions#DEFAULT_DATE_FORMAT}.</li>
 *   <li><strong>phoenix.query.statsUpdateFrequency</strong>: the frequency
 *     in milliseconds at which each table stats will be updated. Defaults to
 *     {@link com.salesforce.phoenix.query.QueryServicesOptions#DEFAULT_STATS_UPDATE_FREQ_MS}.</li>
 *   <li><strong>phoenix.query.maxStatsAge</strong>: the maximum age of
 *     stats in milliseconds after which they no longer will be used (i.e.
 *     if the stats could not be updated for this length of time, the stats
 *     are considered too old and thus no longer accurate enough to use).
 *     Defaults to {@link com.salesforce.phoenix.query.QueryServicesOptions#DEFAULT_MAX_STATS_AGE_MS}.</li>
 *   <li><strong>phoenix.mutate.maxSize</strong>: the maximum number of rows
 *     that may be collected in {@link com.salesforce.phoenix.execute.MutationState}
 *     before a commit or rollback must be called. For better performance and to
 *     circumvent this limit, set {@link java.sql.Connection#setAutoCommit(boolean)}
 *     to TRUE, in which case, mutations (upserts and deletes) are performed
 *     on the server side without returning data back to the client. Defaults
 *     to {@link com.salesforce.phoenix.query.QueryServicesOptions#DEFAULT_MAX_MUTATION_SIZE}.</li>
 *   <li><strong><del>phoenix.mutate.upsertBatchSize</del></strong>: deprecated - use strong>phoenix.mutate.batchSize</strong>
 *     instead.</li>
 *   <li><strong>phoenix.mutate.batchSize</strong>: the number of rows
 *     that are batched together and automatically committed during the execution
 *     of an UPSERT SELECT or DELETE statement. This property may be overridden at
 *     connection time by specifying a {@link com.salesforce.phoenix.util.PhoenixRuntime#UPSERT_BATCH_SIZE_ATTRIB}
 *     property value. Note that the connection property value does not affect the
 *     batch size used by the coprocessor when these statements are executed
 *     completely on the server side. Defaults to
 *     {@link com.salesforce.phoenix.query.QueryServicesOptions#DEFAULT_MUTATE_BATCH_SIZE}.</li>
 *   <li><strong>phoenix.query.regionBoundaryCacheTTL</strong>: the time-to-live
 *     in milliseconds of the region boundary cache used to guide the split
 *     points for query parallelization. Defaults to
 *     {@link com.salesforce.phoenix.query.QueryServicesOptions#DEFAULT_REGION_BOUNDARY_CACHE_TTL_MS}.</li>
 *   </ul>
 *     
 * @author jtaylor
 * @since 0.1
 */
@Immutable
public interface QueryServices extends SQLCloseable {
    public static final String KEEP_ALIVE_MS_ATTRIB = "phoenix.query.keepAliveMs";
    public static final String THREAD_POOL_SIZE_ATTRIB = "phoenix.query.threadPoolSize";
    public static final String QUEUE_SIZE_ATTRIB = "phoenix.query.queueSize";
    public static final String THREAD_TIMEOUT_MS_ATTRIB = "phoenix.query.timeoutMs";
    public static final String SPOOL_THRESHOLD_BYTES_ATTRIB = "phoenix.query.spoolThresholdBytes";
    
    public static final String MAX_MEMORY_PERC_ATTRIB = "phoenix.query.maxGlobalMemoryPercentage";
    public static final String MAX_MEMORY_WAIT_MS_ATTRIB = "phoenix.query.maxGlobalMemoryWaitMs";
    public static final String MAX_TENANT_MEMORY_PERC_ATTRIB = "phoenix.query.maxTenantMemoryPercentage";
    public static final String MAX_HASH_CACHE_SIZE_ATTRIB = "phoenix.query.maxHashCacheBytes";
    public static final String TARGET_QUERY_CONCURRENCY_ATTRIB = "phoenix.query.targetConcurrency";
    public static final String MAX_QUERY_CONCURRENCY_ATTRIB = "phoenix.query.maxConcurrency";
    public static final String DATE_FORMAT_ATTRIB = "phoenix.query.dateFormat";
    public static final String STATS_UPDATE_FREQ_MS_ATTRIB = "phoenix.query.statsUpdateFrequency";
    public static final String MAX_STATS_AGE_MS_ATTRIB = "phoenix.query.maxStatsAge";
    public static final String CALL_QUEUE_ROUND_ROBIN_ATTRIB = "ipc.server.callqueue.roundrobin";
    public static final String SCAN_CACHE_SIZE_ATTRIB = "hbase.client.scanner.caching";
    public static final String MAX_MUTATION_SIZE_ATTRIB = "phoenix.mutate.maxSize";
    /**
     * Use {@link #MUTATE_BATCH_SIZE_ATTRIB} instead
     * @deprecated
     */
    @Deprecated
    public static final String UPSERT_BATCH_SIZE_ATTRIB = "phoenix.mutate.upsertBatchSize";
    public static final String MUTATE_BATCH_SIZE_ATTRIB = "phoenix.mutate.batchSize";
    public static final String REGION_BOUNDARY_CACHE_TTL_MS_ATTRIB = "phoenix.query.regionBoundaryCacheTTL";
    public static final String MAX_HASH_CACHE_TIME_TO_LIVE_MS = "phoenix.coprocessor.maxHashCacheTimeToLiveMs";

    public static final String CALL_QUEUE_PRODUCER_ATTRIB_NAME = "CALL_QUEUE_PRODUCER";
    
    /**
     * Get executor service used for parallel scans
     */
    public ExecutorService getExecutor();
    /**
     * Get the memory manager used to track memory usage
     */
    public MemoryManager getMemoryManager();
    
    /**
     * Get the configuration
     */
    public Configuration getConfig();
}
