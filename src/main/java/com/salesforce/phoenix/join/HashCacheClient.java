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
import java.util.concurrent.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.xerial.snappy.Snappy;

import com.google.common.collect.ImmutableSet;
import com.salesforce.phoenix.exception.*;
import com.salesforce.phoenix.iterate.ResultIterator;
import com.salesforce.phoenix.job.JobManager.JobCallable;
import com.salesforce.phoenix.memory.MemoryManager.MemoryChunk;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.query.Scanner;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.*;

/**
 * 
 * Client for adding cache of one side of a join to region servers
 *
 * @author jtaylor
 * @since 0.1
 */
public class HashCacheClient {
    public static final int DEFAULT_HASH_CACHE_SIZE = 1024*1024*20;  // 20 Mb
    private static final int DEFAULT_THREAD_TIMEOUT_MS = 60000; // 1min
    private static final int DEFAULT_MAX_HASH_CACHE_SIZE = 1024*1024*100;  // 100 Mb
    
    private static final Log LOG = LogFactory.getLog(HashCacheClient.class);
    private static final String JOIN_KEY_PREFIX = "joinKey";
    private static int JOIN_KEY_ID = 0;
    private final byte[] iterateOverTableName;
    private final byte[] tenantId;
    private final ConnectionQueryServices services;

    /**
     * Construct client used to create a serialized cached snapshot of a table and send it to each region server
     * for caching during hash join processing.
     * @param services the global services
     * @param iterateOverTableName table name
     * @param tenantId the tenantId or null if not applicable
     */
    public HashCacheClient(ConnectionQueryServices services, byte[] iterateOverTableName, byte[] tenantId) {
        this.services = services;
        this.iterateOverTableName = iterateOverTableName;
        this.tenantId = tenantId;
    }

    /**
     * Client-side representation of a hash cache.  Call {@link #close()} when scan doing join
     * is complete to free cache up on region server
     *
     * @author jtaylor
     * @since 0.1
     */
    public class HashCache implements SQLCloseable {
        private final int size;
        private final byte[] joinId;
        private final ImmutableSet<ServerName> servers;
        
        public HashCache(byte[] joinId, Set<ServerName> servers, int size) {
            this.joinId = joinId;
            this.servers = ImmutableSet.copyOf(servers);
            this.size = size;
        }

        /**
         * Gets the size in bytes of hash cache
         */
        public int getSize() {
            return size;
        }

        /**
         * Gets the unique identifier for this hash cache
         */
        public byte[] getJoinId() {
            return joinId;
        }

        /**
         * Call to free up cache on region servers when no longer needed
         */
        @Override
        public void close() throws SQLException {
            removeHashCache(joinId, servers);
        }

    }
    
    /**
     * Send the results of scanning the hashCacheTable (using the hashCacheScan) to all
     * region servers for the table being iterated over (i.e. the other table involved in
     * the join that is not being hashed).
     * @param scanner scanner for the table or intermediate results being cached
     * @param tableName table name being scanned or null when hash cache will not be used
     * in an outer join. TODO: switch to List<byte[]> for multi-table case if we go with
     * single column layout
     * @param cfs column families for table being scanned or null when hash cache will not
     * be used in an outer join.  TODO: switch to List<byte[][]> for multi-table case if
     * we go with single column layout
     * @return client-side {@link HashCache} representing the added hash cache
     * @throws SQLException 
     * @throws MaxHashCacheSizeExceededException if size of hash cache exceeds max allowed
     * size
     */
    public HashCache addHashCache(Scanner scanner, byte[] tableName, byte[][] cfs) throws SQLException {
        final byte[] joinId = nextJoinId();
        
        /**
         * Serialize and compress hashCacheTable
         */
        HashCache hashCacheSpec = null;
        SQLException firstException = null;
        ResultIterator iterator = null;
        ImmutableBytesWritable hashCache = null;
        Configuration config = services.getConfig();
        List<Closeable> closeables = new ArrayList<Closeable>();
        MemoryChunk chunk = services.getMemoryManager().allocate(scanner.getEstimatedSize());
        closeables.add(chunk);
        try {
            iterator = scanner.iterator();        
            hashCache = serialize(iterator, tableName, cfs, chunk);
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
        
        /**
         * Execute EndPoint in parallel on each server to send compressed hash cache 
         */
        // TODO: generalize and package as a per region server EndPoint caller
        // (ideally this would be functionality provided by the coprocessor framework)
        final ImmutableBytesWritable theHashCache = hashCache;
        boolean success = false;
        ExecutorService executor = services.getExecutor();
        List<Future<Boolean>> futures = Collections.emptyList();
        try {
            NavigableMap<HRegionInfo, ServerName> locations = MetaScanner.allTableRegions(services.getConfig(), iterateOverTableName, false);
            int nRegions = locations.size();
            // Size these based on worst case
            futures = new ArrayList<Future<Boolean>>(nRegions);
            Set<ServerName> servers = new HashSet<ServerName>(nRegions);
            for (Map.Entry<HRegionInfo, ServerName> entry : locations.entrySet()) {
                // Keep track of servers we've sent to and only send once
                if (!servers.contains(entry.getValue())) {  // Call RPC once per server
                    servers.add(entry.getValue());
                    final byte[] key = entry.getKey().getStartKey();
                    final HTableInterface iterateOverTable = services.getTable(iterateOverTableName);
                    closeables.add(iterateOverTable);
                    futures.add(executor.submit(new JobCallable<Boolean>() {
                        
                        @Override
                        public Boolean call() throws Exception {
                            HashCacheProtocol protocol = iterateOverTable.coprocessorProxy(HashCacheProtocol.class, key);
                            return protocol.addHashCache(tenantId, joinId, theHashCache);
                        }

                        /**
                         * Defines the grouping for round robin behavior.  All threads spawned to process
                         * this scan will be grouped together and time sliced with other simultaneously
                         * executing parallel scans.
                         */
                        @Override
                        public Object getJobId() {
                            return HashCacheClient.this;
                        }
                    }));
                }
            }
            
            hashCacheSpec = new HashCache(joinId,servers,theHashCache.getSize());
            // Execute in parallel
            int timeoutMs = config.getInt(QueryServices.THREAD_TIMEOUT_MS_ATTRIB, DEFAULT_THREAD_TIMEOUT_MS);
            for (Future<Boolean> future : futures) {
                future.get(timeoutMs, TimeUnit.MILLISECONDS);
            }
            
            success = true;
        } catch (SQLException e) {
            firstException = e;
        } catch (Exception e) {
            firstException = new SQLException(e);
        } finally {
            if (!success) {
                SQLCloseables.closeAllQuietly(Collections.singletonList(hashCacheSpec));
                for (Future<Boolean> future : futures) {
                    future.cancel(true);
                }
            }
            try {
                Closeables.closeAll(closeables);
            } catch (IOException e) {
                if (firstException == null) {
                    firstException = new SQLException(e);
                }
            } finally {
                if (firstException != null) {
                    throw firstException;
                }
            }
        }
        return hashCacheSpec;
    }

    /**
     * Remove the cached table from all region servers
     * @param joinId unique identifier for the hash join (returned from {@link #addHashCache(HTable, Scan, Set)})
     * @param servers list of servers upon which table was cached (filled in by {@link #addHashCache(HTable, Scan, Set)})
     * @throws SQLException
     * @throws IllegalStateException if hashed table cannot be removed on any region server on which it was added
     */
    private void removeHashCache(byte[] joinId, Set<ServerName> servers) throws SQLException {
        Throwable lastThrowable = null;
        HTableInterface iterateOverTable = services.getTable(iterateOverTableName);
        NavigableMap<HRegionInfo, ServerName> locations;
        try {
            locations = MetaScanner.allTableRegions(services.getConfig(), iterateOverTableName, false);
        } catch (IOException e) {
            throw new PhoenixIOException(e);
        }
        Set<ServerName> remainingOnServers = new HashSet<ServerName>(servers); 
        for (Map.Entry<HRegionInfo, ServerName> entry : locations.entrySet()) {
            if (remainingOnServers.contains(entry.getValue())) {  // Call once per server
                try {
                    byte[] key = entry.getKey().getStartKey();
                    HashCacheProtocol protocol = iterateOverTable.coprocessorProxy(HashCacheProtocol.class, key);
                    protocol.removeHashCache(tenantId, joinId);
                    remainingOnServers.remove(entry.getValue());
                } catch (Throwable t) {
                    lastThrowable = t;
                    LOG.error("Error trying to remove hash cache for " + entry.getValue(), t);
                }
            }
        }
        if (!remainingOnServers.isEmpty()) {
            LOG.warn("Unable to remove hash cache for " + remainingOnServers, lastThrowable);
        }
    }

    /**
     * Create a join ID to keep the cached information across other joins independent.
     * TODO: Use HBase counter instead here once this is real
     */
    private static synchronized byte[] nextJoinId() {
        return Bytes.toBytes(JOIN_KEY_PREFIX + ++JOIN_KEY_ID);
    }
 
    // package private for testing
    ImmutableBytesWritable serialize(ResultIterator scanner, byte[] tableName, byte[][] cfs, MemoryChunk chunk) throws SQLException {
        try {
            long maxSize = services.getConfig().getLong(QueryServices.MAX_HASH_CACHE_SIZE_ATTRIB, DEFAULT_MAX_HASH_CACHE_SIZE);
            long estimatedSize = Math.min(chunk.getSize(), maxSize);
            if (estimatedSize > Integer.MAX_VALUE) {
                throw new IllegalStateException("Estimated size(" + estimatedSize + ") must not be greater than Integer.MAX_VALUE(" + Integer.MAX_VALUE + ")");
            }
            TrustedByteArrayOutputStream baOut = new TrustedByteArrayOutputStream((int)estimatedSize);
            DataOutputStream out = new DataOutputStream(baOut);
            int nRows = 0;
            out.writeInt(nRows); // In the end will be replaced with total number of rows
            for (Tuple result = scanner.next(); result != null; result = scanner.next()) {
                TupleUtil.write(result, out);
                if (baOut.size() > estimatedSize) {
                    if (baOut.size() > maxSize) {
                        throw new MaxHashCacheSizeExceededException("Size of hash cache (" + baOut.size() + " bytes) exceeds the maximum allowed size (" + maxSize + " bytes)");
                    }
                    estimatedSize *= 3/2;
                    chunk.resize(estimatedSize);
                }
                nRows++;
            }
            if (cfs == null) {
                WritableUtils.writeVInt(out, 0);
            } else {
                WritableUtils.writeVInt(out, cfs.length + 1);
                Bytes.writeByteArray(out, tableName);
                out.write(ByteUtil.toBytes(cfs));
            }
            TrustedByteArrayOutputStream sizeOut = new TrustedByteArrayOutputStream(Bytes.SIZEOF_INT);
            DataOutputStream dataOut = new DataOutputStream(sizeOut);
            dataOut.writeInt(nRows);
            dataOut.flush();
            byte[] cache = baOut.getBuffer();
            // Replace number of rows written above with the correct value.
            System.arraycopy(sizeOut.getBuffer(), 0, cache, 0, sizeOut.size());
            // Reallocate to actual size plus compressed buffer size (which is allocated below)
            int maxCompressedSize = Snappy.maxCompressedLength(baOut.size());
            chunk.resize(baOut.size() + maxCompressedSize);
            byte[] compressed = new byte[maxCompressedSize]; // size for worst case
            int compressedSize = Snappy.compress(baOut.getBuffer(), 0, baOut.size(), compressed, 0);
            // Last realloc to size of compressed buffer.
            chunk.resize(compressedSize);
            return new ImmutableBytesWritable(compressed,0,compressedSize);
        } catch (IOException e) {
            throw new PhoenixIOException(e);
        }
    }
}
