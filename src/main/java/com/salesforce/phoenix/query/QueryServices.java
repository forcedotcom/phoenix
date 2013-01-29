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
 * following parameters may be set in {@link org.apache.hadoop.conf.Configuration}:
 *
 *   - phoenix.query.maxGlobalMemoryBytes: total amount of memory that all threads
 *     may use.  Only course grain memory usage is tracked, mainly accounting for
 *     memory usage in the hash join cache and the intermediate map for group by
 *     aggregation.  When this limit is reached clients block when attempting to
 *     get more memory, essentially throttling memory usage.  One way to calculate
 *     what it should be set to is to take a percentage of the maximum heap size.
 *   - phoenix.query.maxGlobalMemoryWaitMs: maximum amount of time that a client
 *     will block while waiting for more memory to become available.  After this
 *     amount of time, a memory.InsufficientMemoryException is thrown.
 *   - phoenix.query.maxOrgMemoryPercentage: maximum percentage of 
 *     phoenix.query.maxGlobalMemoryBytes that any one org is allowed to consume.
 *     After this percentage, a memory.InsufficientMemoryException is thrown.
 *   - phoenix.coprocessor.maxGlobalMemoryBytes: similar to and defaulted to
 *     phoenix.query.maxGlobalMemoryBytes but for memory consumption by coprocessors.
 *   - phoenix.coprocessor.maxGlobalMemoryWaitMs: similar to and defaulted to
 *     phoenix.query.maxGlobalMemoryBytes but for memory consumption by coprocessors.
 *   - phoenix.coprocessor.maxOrgMemoryPercentage: similar and defaulted to
 *     phoenix.query.maxGlobalMemoryBytes but for memory consumption by coprocessors.
 *   - phoenix.coprocessor.maxHashCacheTimeToLiveMs:  maximum amount of time in
 *     milliseconds that a hash cache will be available during coprocessing.
 *   - phoenix.query.maxHashCacheBytes: max size in bytes of a hash cache.
 *   - phoenix.query.targetConcurrency: target concurrent threads to use for a query.
 *     It serves as a soft limit on how many scans a query can split into. A hard
 *     limit is imposed by phoenix.query.maxConcurrency.  
 *     This value should not exceed phoenix.query.maxConcurrency.
 *   - phoenix.query.maxConcurrency: maximum concurrent threads to use for a query.
 *     See phoenix.query.targetConcurrency for more details.
 *   - phoenix.query.dateFormat: to specify the default pattern to use to convert
 *     a date to/from a string, whether through the TO_CHAR(<date>) or TO_DATE(<dateAsString>) functions, or through
 *     resultSet.getString("DATE_COLUMN"). The default is {@link com.salesforce.phoenix.util.DateUtil#DEFAULT_DATE_FORMAT}
 *   - phoenix.query.statsUpdateFrequency: the frequency in milliseconds at which each table stats
 *     will be updated.
 *   - phoenix.query.maxStatsAge: the maximum age of stats in milliseconds after which they no longer will
 *     be used (i.e. the stats could not be updated at phoenix.query.statsUpdateFrequency for this length of
 *     time, so the stats are considered no longer accurate enough to use).
 *   - phoenix.mutate.maxSize: the maximum number of rows that may be collected in {@link com.salesforce.phoenix.execute.MutationState}
 *     before a commit or rollback are called. The default is {@link com.salesforce.phoenix.query.QueryServicesOptions#DEFAULT_MAX_MUTATION_SIZE}.
 *     For better performance and to circumvent this limit, set {@link java.sql.Connection#setAutoCommit(boolean)} to TRUE.
 *     In this case, mutations (upserts and deletes) will be performed on the server side without returning data back
 *     to the client.
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
    public static final String MAX_HTABLE_POOL_SIZE_ATTRIB = "phoenix.query.htablePoolSize";
    public static final String MAX_MEMORY_BYTES_ATTRIB = "phoenix.query.maxGlobalMemoryBytes";
    public static final String MAX_MEMORY_WAIT_MS_ATTRIB = "phoenix.query.maxGlobalMemoryWaitMs";
    public static final String MAX_ORG_MEMORY_PERC_ATTRIB = "phoenix.query.maxOrgMemoryPercentage";
    public static final String MAX_HASH_CACHE_SIZE_ATTRIB = "phoenix.query.maxHashCacheBytes";
    public static final String TARGET_QUERY_CONCURRENCY_ATTRIB = "phoenix.query.targetConcurrency";
    public static final String MAX_QUERY_CONCURRENCY_ATTRIB = "phoenix.query.maxConcurrency";
    public static final String DATE_FORMAT_ATTRIB = "phoenix.query.dateFormat";
    public static final String STATS_UPDATE_FREQ_MS_ATTRIB = "phoenix.query.statsUpdateFrequency";
    public static final String CALL_QUEUE_PRODUCER_ATTRIB_NAME = "CALL_QUEUE_PRODUCER";
    public static final String MAX_STATS_AGE_MS_ATTRIB = "phoenix.query.maxStatsAge";
    public static final String CALL_QUEUE_ROUND_ROBIN_ATTRIB = "ipc.server.callqueue.roundrobin";
    public static final String SCAN_CACHE_SIZE_ATTRIB = "hbase.client.scanner.caching";
    public static final String MAX_MUTATION_SIZE_ATTRIB = "phoenix.mutate.maxSize";
    public static final String UPSERT_BATCH_SIZE_ATTRIB = "phoenix.mutate.upsertBatchSize";

    public static final int DEFAULT_SCAN_CACHE_SIZE = 1000;
    
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
