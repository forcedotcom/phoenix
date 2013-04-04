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
package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.salesforce.phoenix.compile.StatementContext;
import com.salesforce.phoenix.iterate.DefaultParallelIteratorRegionSplitter;
import com.salesforce.phoenix.iterate.ParallelIterators;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.query.StatsManagerImpl.TimeKeeper;
import com.salesforce.phoenix.schema.PSchema;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.PhoenixRuntime;


/**
 * Tests for {@link DefaultParallelIteratorRegionSplitter}.
 * 
 * @author syyang
 * @since 0.1
 */
public class DefaultParallelIteratorsTest extends BaseClientMangedTimeTest {

    private static final byte[] KMIN  = new byte[] {'!'};
    private static final byte[] KMIN2  = new byte[] {'.'};
    private static final byte[] K1  = new byte[] {'a'};
    private static final byte[] K3  = new byte[] {'c'};
    private static final byte[] K4  = new byte[] {'d'};
    private static final byte[] K5  = new byte[] {'e'};
    private static final byte[] K6  = new byte[] {'f'};
    private static final byte[] K9  = new byte[] {'i'};
    private static final byte[] K11 = new byte[] {'k'};
    private static final byte[] K12 = new byte[] {'l'};
    private static final byte[] KMAX  = new byte[] {'~'};
    private static final byte[] KMAX2  = new byte[] {'z'};

    protected static TableRef initTableValues(long ts, int targetQueryConcurrency, int maxQueryConcurrency) throws Exception {
        byte[][] splits = new byte[][] {K3,K4,K9,K11};
        Configuration config = driver.getQueryServices().getConfig();
        config.setInt(QueryServices.MAX_QUERY_CONCURRENCY_ATTRIB, maxQueryConcurrency);
        config.setInt(QueryServices.TARGET_QUERY_CONCURRENCY_ATTRIB, targetQueryConcurrency);
        ensureTableCreated(getUrl(),STABLE_NAME,splits, ts-2);
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " + STABLE_NAME + " VALUES (?, ?)");
        stmt.setString(1, new String(KMIN));
        stmt.setInt(2, 1);
        stmt.execute();
        stmt.setString(1, new String(KMAX));
        stmt.setInt(2, 2);
        stmt.execute();
        conn.commit();
        conn.close();
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        PSchema schema = pconn.getPMetaData().getSchemas().get(STABLE_SCHEMA_NAME);
        return new TableRef(null,schema.getTable(STABLE_NAME),schema, ts);
    }

    private static NavigableMap<HRegionInfo, ServerName> getRegions(TableRef table) throws IOException {
        return MetaScanner.allTableRegions(driver.getQueryServices().getConfig(), table.getTableName(), false);
    }

    private static List<KeyRange> getSplits(TableRef table, final Scan scan, final NavigableMap<HRegionInfo, ServerName> regions) throws SQLException {
        PhoenixConnection connection = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        StatementContext context = new StatementContext(connection, null, Collections.emptyList(), 0, scan);
        DefaultParallelIteratorRegionSplitter splitter = new DefaultParallelIteratorRegionSplitter(context, table) {
            @Override
            protected List<Map.Entry<HRegionInfo, ServerName>> getAllRegions() throws SQLException {
                return ParallelIterators.filterRegions(regions, scan.getStartRow(), scan.getStopRow());
            }
        };
        List<KeyRange> keyRanges = splitter.getSplits();
        Collections.sort(keyRanges, new Comparator<KeyRange>() {
            @Override
            public int compare(KeyRange o1, KeyRange o2) {
                return Bytes.compareTo(o1.getLowerRange(),o2.getLowerRange());
            }
        });
        return keyRanges;
    }

    @Test
    public void testGetSplits() throws Exception {
        long ts = nextTimestamp();
        TableRef table = initTableValues(ts, 3, 5);
        Scan scan = new Scan();
        
        // number of regions > target query concurrency
        scan.setStartRow(K1);
        scan.setStopRow(K12);
        NavigableMap<HRegionInfo, ServerName> regions = getRegions(table);
        List<KeyRange> keyRanges = getSplits(table, scan, regions);
        assertEquals("Unexpected number of splits: " + keyRanges, 5, keyRanges.size());
        assertEquals(newKeyRange(HConstants.EMPTY_START_ROW, K3), keyRanges.get(0));
        assertEquals(newKeyRange(K3, K4), keyRanges.get(1));
        assertEquals(newKeyRange(K4, K9), keyRanges.get(2));
        assertEquals(newKeyRange(K9, K11), keyRanges.get(3));
        assertEquals(newKeyRange(K11, HConstants.EMPTY_END_ROW), keyRanges.get(4));
        
        // (number of regions / 2) > target query concurrency
        scan.setStartRow(K3);
        scan.setStopRow(K6);
        keyRanges = getSplits(table, scan, regions);
        assertEquals("Unexpected number of splits: " + keyRanges, 3, keyRanges.size());
        // note that we get a single split from R2 due to small key space
        assertEquals(newKeyRange(K3, K4), keyRanges.get(0));
        assertEquals(newKeyRange(K4, K6), keyRanges.get(1));
        assertEquals(newKeyRange(K6, K9), keyRanges.get(2));
        
        // (number of regions / 2) <= target query concurrency
        scan.setStartRow(K5);
        scan.setStopRow(K6);
        keyRanges = getSplits(table, scan, regions);
        assertEquals("Unexpected number of splits: " + keyRanges, 3, keyRanges.size());
        assertEquals(newKeyRange(K4, K5), keyRanges.get(0));
        assertEquals(newKeyRange(K5, K6), keyRanges.get(1));
        assertEquals(newKeyRange(K6, K9), keyRanges.get(2));
    }

    @Test
    public void testGetLowerUnboundSplits() throws Exception {
        long ts = nextTimestamp();
        TableRef table = initTableValues(ts, 3, 5);
        Scan scan = new Scan();
        
        ConnectionQueryServices services = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES);
        services.getStatsManager().updateStats(table);
        scan.setStartRow(HConstants.EMPTY_START_ROW);
        scan.setStopRow(K1);
        NavigableMap<HRegionInfo, ServerName> regions = getRegions(table);
        List<KeyRange> keyRanges = getSplits(table, scan, regions);
        assertEquals("Unexpected number of splits: " + keyRanges, 3, keyRanges.size());
        assertEquals(newKeyRange(KeyRange.UNBOUND_LOWER, new byte[] {'7'}), keyRanges.get(0));
        assertEquals(newKeyRange(new byte[] {'7'}, new byte[] {'M'}), keyRanges.get(1));
        assertEquals(newKeyRange(new byte[] {'M'}, K3), keyRanges.get(2));
    }

    private static class ManualTimeKeeper implements TimeKeeper {
        private long currentTime = 0;
        @Override
        public long currentTimeMillis() {
            return currentTime;
        }
        
        public void setCurrentTimeMillis(long currentTime) {
            this.currentTime = currentTime;
        }
    }

    private static interface ChangeDetector {
        boolean isChanged();
    }

    private boolean waitForAsyncChange(ChangeDetector detector, long maxWaitTimeMs) throws Exception {
        long startTime = System.currentTimeMillis();
        do {
            if (detector.isChanged()) {
                return true;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw e;
            }
        } while (System.currentTimeMillis() - startTime < maxWaitTimeMs);
        return false;
    }

    private static class MinKeyChange implements ChangeDetector {
        private byte[] value;
        private StatsManager stats;
        private TableRef table;
        
        public MinKeyChange(StatsManager stats, TableRef table) {
            this.value = stats.getMinKey(table);
            this.stats = stats;
            this.table = table;
        }
        @Override
        public boolean isChanged() {
            return value != stats.getMinKey(table);
        }
    }

    private static class MaxKeyChange implements ChangeDetector {
        private byte[] value;
        private StatsManager stats;
        private TableRef table;
        
        public MaxKeyChange(StatsManager stats, TableRef table) {
            this.value = stats.getMaxKey(table);
            this.stats = stats;
            this.table = table;
        }
        @Override
        public boolean isChanged() {
            return value != stats.getMaxKey(table);
        }
    }

    @Test
    public void testStatsManagerImpl() throws Exception {
        long ts = nextTimestamp();
        TableRef table = initTableValues(ts, 3, 5);
        int updateFreq = 5;
        int maxAge = 10;
        int startTime = 100;
        long waitTime = 3000;
        
        ManualTimeKeeper timeKeeper = new ManualTimeKeeper();
        timeKeeper.setCurrentTimeMillis(startTime);
        ConnectionQueryServices services = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES);
        StatsManager stats = new StatsManagerImpl(services, updateFreq, maxAge, timeKeeper);
        MinKeyChange minKeyChange = new MinKeyChange(stats, table);
        MaxKeyChange maxKeyChange = new MaxKeyChange(stats, table);
        
        byte[] minKey = stats.getMinKey(table);
        assertTrue(minKey == null);
        assertTrue(waitForAsyncChange(minKeyChange,waitTime));
        assertArrayEquals(KMIN, stats.getMinKey(table));
        assertArrayEquals(KMAX, stats.getMaxKey(table));
        minKeyChange = new MinKeyChange(stats, table);
        
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts + 2;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement delStmt = conn.prepareStatement("delete from " + STABLE_NAME + " where id=?");
        delStmt.setString(1, new String(KMIN));
        delStmt.execute();
        PreparedStatement upsertStmt = conn.prepareStatement("upsert into " + STABLE_NAME + " VALUES (?, ?)");
        upsertStmt.setString(1, new String(KMIN2));
        upsertStmt.setInt(2, 1);
        upsertStmt.execute();
        conn.commit();

        assertFalse(waitForAsyncChange(minKeyChange,waitTime)); // Stats won't change until they're attempted to be retrieved again
        timeKeeper.setCurrentTimeMillis(timeKeeper.currentTimeMillis() + updateFreq);
        minKeyChange = new MinKeyChange(stats, table); // Will kick off change, but will upate asynchronously
        assertArrayEquals(KMIN, minKeyChange.value);
        assertTrue(waitForAsyncChange(minKeyChange,waitTime));
        assertArrayEquals(KMIN2, stats.getMinKey(table));
        assertArrayEquals(KMAX, stats.getMaxKey(table));
        minKeyChange = new MinKeyChange(stats, table);
        
        timeKeeper.setCurrentTimeMillis(timeKeeper.currentTimeMillis() + maxAge);
        minKeyChange = new MinKeyChange(stats, table); // Will kick off change, but will upate asynchronously
        assertTrue(null == minKeyChange.value);
        assertTrue(waitForAsyncChange(minKeyChange,waitTime));
        assertArrayEquals(KMIN2, stats.getMinKey(table));
        assertArrayEquals(KMAX, stats.getMaxKey(table));
        minKeyChange = new MinKeyChange(stats, table);
        maxKeyChange = new MaxKeyChange(stats, table);
        
        delStmt.setString(1, new String(KMAX));
        delStmt.execute();
        upsertStmt.setString(1, new String(KMAX2));
        upsertStmt.setInt(2, 1);
        upsertStmt.execute();
        conn.commit();
        conn.close();

        assertFalse(waitForAsyncChange(maxKeyChange,waitTime)); // Stats won't change until they're attempted to be retrieved again
        timeKeeper.setCurrentTimeMillis(timeKeeper.currentTimeMillis() + updateFreq);
        maxKeyChange = new MaxKeyChange(stats, table); // Will kick off change, but will upate asynchronously
        assertArrayEquals(KMAX, maxKeyChange.value);
        assertTrue(waitForAsyncChange(maxKeyChange,waitTime));
        assertArrayEquals(KMAX2, stats.getMaxKey(table));
        assertArrayEquals(KMIN2, stats.getMinKey(table));
        maxKeyChange = new MaxKeyChange(stats, table);
        
        timeKeeper.setCurrentTimeMillis(timeKeeper.currentTimeMillis() + maxAge);
        maxKeyChange = new MaxKeyChange(stats, table); // Will kick off change, but will upate asynchronously
        assertTrue(null == maxKeyChange.value);
        assertTrue(waitForAsyncChange(maxKeyChange,waitTime));
        assertArrayEquals(KMIN2, stats.getMinKey(table));
        assertArrayEquals(KMAX2, stats.getMaxKey(table));
    }

    private static KeyRange newKeyRange(byte[] lowerRange, byte[] upperRange) {
        return KeyRange.getKeyRange(lowerRange, true, upperRange, false);
    }
}
