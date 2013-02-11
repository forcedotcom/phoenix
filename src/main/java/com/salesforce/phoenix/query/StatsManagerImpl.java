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

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.*;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;

import com.salesforce.phoenix.exception.*;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.SchemaUtil;


/**
 * 
 * Implementation of StatsManager. Table stats are updated asynchronously when they're
 * accessed and past time-to-live. In this case, future calls (after the asynchronous
 * call has completed), will have the updated stats.
 * 
 * All tables share the same HBase connection for a given connection and each connection
 * will have it's own cache for these stats. This isn't ideal and will get reworked when
 * the schema is kept on the server side. It's ok for now because:
 * 1) we only ask the server for these stats when the start/end region is queried against
 * 2) the query to get the stats pulls a single row so it's very cheap
 * 3) it's async and if it takes too long it won't lead to anything except less optimal
 *  parallelization.
 *
 * @author jtaylor
 * @since 0.1
 */
public class StatsManagerImpl implements StatsManager {
    private final ConnectionQueryServices services;
    private final int statsUpdateFrequencyMs;
    private final int maxStatsAgeMs;
    private final TimeKeeper timeKeeper;
    private final ConcurrentMap<TableRef,PTableStats> tableStatsMap = new ConcurrentHashMap<TableRef,PTableStats>();

    public StatsManagerImpl(ConnectionQueryServices services, int statsUpdateFrequencyMs, int maxStatsAgeMs) {
        this(services, statsUpdateFrequencyMs, maxStatsAgeMs, TimeKeeper.SYSTEM);
    }
    
    public StatsManagerImpl(ConnectionQueryServices services, int statsUpdateFrequencyMs, int maxStatsAgeMs, TimeKeeper timeKeeper) {
        this.services = services;
        this.statsUpdateFrequencyMs = statsUpdateFrequencyMs;
        this.maxStatsAgeMs = maxStatsAgeMs;
        this.timeKeeper = timeKeeper;
    }
    
    public static interface TimeKeeper {
        static final TimeKeeper SYSTEM = new TimeKeeper() {
            @Override
            public long currentTimeMillis() {
                return System.currentTimeMillis();
            }
        };
        
        long currentTimeMillis();
    }
    public long getStatsUpdateFrequency() {
        return statsUpdateFrequencyMs;
    }
    
    @Override
    public void updateStats(TableRef table) throws SQLException {
        SQLException sqlE = null;
        HTableInterface hTable = services.getTable(table.getTableName());
        try {
            byte[] minKey = null, maxKey = null;
            // Do a key-only scan to get the first row of a table. This is the min
            // key for the table.
            Scan scan = new Scan(HConstants.EMPTY_START_ROW, new KeyOnlyFilter());
            ResultScanner scanner = hTable.getScanner(scan);
            try {
                Result r = scanner.next(); 
                if (r != null) {
                    minKey = r.getRow();
                }
            } finally {
                scanner.close();
            }
            int maxPossibleKeyLength = SchemaUtil.estimateKeyLength(table.getTable());
            byte[] maxPossibleKey = new byte[maxPossibleKeyLength];
            Arrays.fill(maxPossibleKey, (byte)255);
            // Use this deprecated method to get the key "before" the max possible key value,
            // which is the max key for a table.
            @SuppressWarnings("deprecation")
            Result r = hTable.getRowOrBefore(maxPossibleKey, table.getTable().getColumnFamilies().iterator().next().getName().getBytes());
            if (r != null) {
                maxKey = r.getRow();
            }
            tableStatsMap.put(table, new PTableStats(timeKeeper.currentTimeMillis(),minKey,maxKey));
        } catch (IOException e) {
            sqlE = new PhoenixIOException(e);
        } finally {
            try {
                hTable.close();
            } catch (IOException e) {
                if (sqlE == null) {
                    sqlE = new PhoenixIOException(e);
                } else {
                    sqlE.setNextException(new PhoenixIOException(e));
                }
            } finally {
                if (sqlE != null) {
                    throw sqlE;
                }
            }
        }
    }
    
    private PTableStats getStats(final TableRef table) {
        PTableStats stats = tableStatsMap.get(table);
        if (stats == null) {
            PTableStats newStats = new PTableStats();
            stats = tableStatsMap.putIfAbsent(table, newStats);
            stats = stats == null ? newStats : stats;
        }
        // Synchronize on the current stats for a table to prevent
        // multiple attempts to update the stats.
        synchronized (stats) {
            long initiatedTime = stats.getInitiatedTime();
            long currentTime = timeKeeper.currentTimeMillis();
            // Update stats asynchronously if they haven't been updated within the specified frequency.
            // We update asynchronously because we don't ever want to block the caller - instead we'll continue
            // to use the old one.
            if ( currentTime - initiatedTime >= getStatsUpdateFrequency()) {
                stats.setInitiatedTime(currentTime);
                services.getExecutor().submit(new Callable<Void>() {

                    @Override
                    public Void call() throws Exception { // TODO: will exceptions be logged?
                        updateStats(table);
                        return null;
                    }
                    
                });
            }
            // If the stats are older than the max age, use an empty stats
            if (currentTime - stats.getCompletedTime() >= maxStatsAgeMs) {
                return PTableStats.NO_STATS;
            }
        }
        return stats;
    }
    
    @Override
    public byte[] getMinKey(TableRef table) {
        PTableStats stats = getStats(table);
        return stats.getMinKey();
    }

    @Override
    public byte[] getMaxKey(TableRef table) {
        PTableStats stats = getStats(table);
        return stats.getMaxKey();
    }

    private static class PTableStats {
        private static final PTableStats NO_STATS = new PTableStats();
        private long initiatedTime;
        private final long completedTime;
        private final byte[] minKey;
        private final byte[] maxKey;
        
        public PTableStats() {
            this(-1,null,null);
        }
        public PTableStats(long completedTime, byte[] minKey, byte[] maxKey) {
            this.minKey = minKey;
            this.maxKey = maxKey;
            this.completedTime = this.initiatedTime = completedTime;
        }

        private byte[] getMinKey() {
            return minKey;
        }

        private byte[] getMaxKey() {
            return maxKey;
        }

        private long getCompletedTime() {
            return completedTime;
        }

        private void setInitiatedTime(long initiatedTime) {
            this.initiatedTime = initiatedTime;
        }

        private long getInitiatedTime() {
            return initiatedTime;
        }
    }
}
