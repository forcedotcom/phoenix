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
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.ImmutableSet;
import com.salesforce.phoenix.compile.ScanRanges;
import com.salesforce.phoenix.coprocessor.ServerCachingProtocol;
import com.salesforce.phoenix.coprocessor.ServerCachingProtocol.ServerCacheFactory;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.job.JobManager.JobCallable;
import com.salesforce.phoenix.memory.MemoryManager.MemoryChunk;
import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.query.QueryServicesOptions;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.Closeables;
import com.salesforce.phoenix.util.SQLCloseable;
import com.salesforce.phoenix.util.SQLCloseables;

/**
 * 
 * Client for sending cache to each region server
 * 
 * @author jtaylor
 * @since 0.1
 */
public class ServerCacheClient {
    public static final int UUID_LENGTH = Bytes.SIZEOF_LONG;
    private static final Log LOG = LogFactory.getLog(ServerCacheClient.class);
    private static final Random RANDOM = new Random();
    private final PhoenixConnection connection;
    private final Map<Integer, TableRef> cacheUsingTableRefMap = new HashMap<Integer, TableRef>();

    /**
     * Construct client used to create a serialized cached snapshot of a table and send it to each region server
     * for caching during hash join processing.
     * @param connection the client connection
     * @param cacheUsingTableRef table name
     * @param minMaxKeyRange KeyRange specifying the min and max keys for the operation involving cacheUsingTableRef
     * 
     * TODO: instead of minMaxKeyRange, have an interface for iterating through ranges as we may be sending to
     * servers when we don't have to if the min is in first region and max is in last region, especially for point queries.
     */
    public ServerCacheClient(PhoenixConnection connection) {
        this.connection = connection;
    }

    public PhoenixConnection getConnection() {
        return connection;
    }
    
    /**
     * Client-side representation of a server cache.  Call {@link #close()} when usage
     * is complete to free cache up on region server
     *
     * @author jtaylor
     * @since 0.1
     */
    public class ServerCache implements SQLCloseable {
        private final int size;
        private final byte[] id;
        private final ImmutableSet<HRegionLocation> servers;
        
        public ServerCache(byte[] id, Set<HRegionLocation> servers, int size) {
            this.id = id;
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
        public byte[] getId() {
            return id;
        }

        /**
         * Call to free up cache on region servers when no longer needed
         */
        @Override
        public void close() throws SQLException {
            removeServerCache(id, servers);
        }

    }
    
    public ServerCache addServerCache(ScanRanges keyRanges, final ImmutableBytesWritable cachePtr, final ServerCacheFactory cacheFactory, final TableRef cacheUsingTableRef) throws SQLException {
        ConnectionQueryServices services = connection.getQueryServices();
        MemoryChunk chunk = services.getMemoryManager().allocate(cachePtr.getLength());
        List<Closeable> closeables = new ArrayList<Closeable>();
        closeables.add(chunk);
        ServerCache hashCacheSpec = null;
        SQLException firstException = null;
        final byte[] cacheId = generateId();
        /**
         * Execute EndPoint in parallel on each server to send compressed hash cache 
         */
        // TODO: generalize and package as a per region server EndPoint caller
        // (ideally this would be functionality provided by the coprocessor framework)
        boolean success = false;
        ExecutorService executor = services.getExecutor();
        List<Future<Boolean>> futures = Collections.emptyList();
        try {
            List<HRegionLocation> locations = services.getAllTableRegions(cacheUsingTableRef.getTable().getName().getBytes());
            int nRegions = locations.size();
            // Size these based on worst case
            futures = new ArrayList<Future<Boolean>>(nRegions);
            Set<HRegionLocation> servers = new HashSet<HRegionLocation>(nRegions);
            for (HRegionLocation entry : locations) {
                // Keep track of servers we've sent to and only send once
                if ( ! servers.contains(entry) && 
                        keyRanges.intersect(entry.getRegionInfo().getStartKey(), entry.getRegionInfo().getEndKey())) {  // Call RPC once per server
                    servers.add(entry);
                    final byte[] key = entry.getRegionInfo().getStartKey();
                    final HTableInterface htable = services.getTable(cacheUsingTableRef.getTable().getName().getBytes());
                    closeables.add(htable);
                    futures.add(executor.submit(new JobCallable<Boolean>() {
                        
                        @Override
                        public Boolean call() throws Exception {
                            ServerCachingProtocol protocol = htable.coprocessorProxy(ServerCachingProtocol.class, key);
                            return protocol.addServerCache(connection.getTenantId(), cacheId, cachePtr, cacheFactory);
                        }

                        /**
                         * Defines the grouping for round robin behavior.  All threads spawned to process
                         * this scan will be grouped together and time sliced with other simultaneously
                         * executing parallel scans.
                         */
                        @Override
                        public Object getJobId() {
                            return ServerCacheClient.this;
                        }
                    }));
                }
            }
            
            hashCacheSpec = new ServerCache(cacheId,servers,cachePtr.getLength());
            // Execute in parallel
            int timeoutMs = services.getProps().getInt(QueryServices.THREAD_TIMEOUT_MS_ATTRIB, QueryServicesOptions.DEFAULT_THREAD_TIMEOUT_MS);
            for (Future<Boolean> future : futures) {
                future.get(timeoutMs, TimeUnit.MILLISECONDS);
            }
            
            cacheUsingTableRefMap.put(Bytes.mapKey(cacheId), cacheUsingTableRef);
            success = true;
        } catch (SQLException e) {
            firstException = e;
        } catch (Exception e) {
            firstException = new SQLException(e);
        } finally {
            try {
                if (!success) {
                    SQLCloseables.closeAllQuietly(Collections.singletonList(hashCacheSpec));
                    for (Future<Boolean> future : futures) {
                        future.cancel(true);
                    }
                }
            } finally {
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
        }
        return hashCacheSpec;
    }
    
    /**
     * Remove the cached table from all region servers
     * @param cacheId unique identifier for the hash join (returned from {@link #addHashCache(HTable, Scan, Set)})
     * @param servers list of servers upon which table was cached (filled in by {@link #addHashCache(HTable, Scan, Set)})
     * @throws SQLException
     * @throws IllegalStateException if hashed table cannot be removed on any region server on which it was added
     */
    private void removeServerCache(byte[] cacheId, Set<HRegionLocation> servers) throws SQLException {
        ConnectionQueryServices services = connection.getQueryServices();
        Throwable lastThrowable = null;
        TableRef cacheUsingTableRef = cacheUsingTableRefMap.get(Bytes.mapKey(cacheId));
        byte[] tableName = cacheUsingTableRef.getTable().getName().getBytes();
        HTableInterface iterateOverTable = services.getTable(tableName);
        List<HRegionLocation> locations = services.getAllTableRegions(tableName);
        Set<HRegionLocation> remainingOnServers = new HashSet<HRegionLocation>(servers);
        /**
         * Allow for the possibility that the region we based where to send our cache has split and been
         * relocated to another region server *after* we sent it, but before we removed it. To accommodate
         * this, we iterate through the current metadata boundaries and remove the cache once for each
         * server that we originally sent to.
         */
        for (HRegionLocation entry : locations) {
            if (remainingOnServers.contains(entry)) {  // Call once per server
                try {
                    byte[] key = entry.getRegionInfo().getStartKey();
                    ServerCachingProtocol protocol = iterateOverTable.coprocessorProxy(ServerCachingProtocol.class, key);
                    protocol.removeServerCache(connection.getTenantId(), cacheId);
                    remainingOnServers.remove(entry);
                } catch (Throwable t) {
                    lastThrowable = t;
                    LOG.error("Error trying to remove hash cache for " + entry, t);
                }
            }
        }
        if (!remainingOnServers.isEmpty()) {
            LOG.warn("Unable to remove hash cache for " + remainingOnServers, lastThrowable);
        }
    }

    /**
     * Create an ID to keep the cached information across other operations independent.
     * Using simple long random number, since the length of time we need this to be unique
     * is very limited. 
     */
    public static byte[] generateId() {
        long rand = RANDOM.nextLong();
        return Bytes.toBytes(rand);
    }
}