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
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.salesforce.phoenix.compile.ScanRanges;
import com.salesforce.phoenix.compile.StatementContext;
import com.salesforce.phoenix.filter.SkipScanFilter;
import com.salesforce.phoenix.iterate.*;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import com.salesforce.phoenix.util.PhoenixRuntime;


/**
 * Tests for {@link SkipRangeParallelIteratorRegionSplitter}.
 */
@RunWith(Parameterized.class)
public class SkipRangeParallelIteratorRegionSplitterTest extends BaseClientMangedTimeTest {

    private static final String TABLE_NAME = "TEST_SKIP_RANGE_PARALLEL_ITERATOR";
    private static final String DDL = "CREATE TABLE " + TABLE_NAME + " (id char(3) NOT NULL PRIMARY KEY, value integer)";
    private static final byte[] Ka1A = Bytes.toBytes("a1A");
    private static final byte[] Ka1E = Bytes.toBytes("a1E");
    private static final byte[] Ka2A = Bytes.toBytes("a2A");
    private static final byte[] Ka2Z = Bytes.toBytes("a2Z");
    private static final byte[] Kb1D = Bytes.toBytes("b1D");
    private static final byte[] Kz9Z = Bytes.toBytes("z9Z");

    private final Scan scan;
    private final SkipScanFilter filter;
    private final ScanRanges scanRanges;
    private final List<KeyRange> expectedSplits;

    public SkipRangeParallelIteratorRegionSplitterTest(List<List<KeyRange>> slots, int[] widths, List<KeyRange> expectedSplits) {
        RowKeySchemaBuilder builder = new RowKeySchemaBuilder().setMinNullable(10);
        for (final int width : widths) {
            builder.addField(new PDatum() {
                @Override
                public boolean isNullable() {
                    return false;
                }
                @Override
                public PDataType getDataType() {
                    return PDataType.CHAR;
                }
                @Override
                public Integer getByteSize() {
                    return width;
                }
                @Override
                public Integer getMaxLength() {
                    return width;
                }
                @Override
                public Integer getScale() {
                    return null;
                }
            });
        }
        this.filter = new SkipScanFilter(slots, builder.build());
        // Always set start and stop key to max to verify we are using the information in skipscan
        // filter over the scan's KMIN and KMAX.
        this.scan = new Scan().setFilter(filter).setStartRow(KeyRange.UNBOUND_LOWER).setStopRow(KeyRange.UNBOUND_UPPER);
        this.expectedSplits = expectedSplits;
        this.scanRanges = ScanRanges.create(slots, builder.build());
    }

    @Test
    public void testGetSplitsWithSkipScanFilter() throws Exception {
        long ts = nextTimestamp();
        TableRef table = initTableValues(ts, 3, 5);
        NavigableMap<HRegionInfo, ServerName> regions = getRegions(table);
        List<KeyRange> splits = getSplits(table, scan, regions, scanRanges);
        assertEquals("Unexpected number of splits: " + splits.size(), expectedSplits.size(), splits.size());
        for (int i=0; i<expectedSplits.size(); i++) {
            assertEquals(expectedSplits.get(i), splits.get(i));
        }
    }

    @Parameters(name="{0} {1} {2}")
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        KeyRange.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true)},{
                        KeyRange.getKeyRange(Bytes.toBytes("0"), true, Bytes.toBytes("0"), true)},{
                        KeyRange.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("Z"), true)}},
                    new int[] {1,1,1},
                    new KeyRange[] {
                        KeyRange.getKeyRange(HConstants.EMPTY_START_ROW, true, Ka1A, false)
                }));
        return testCases;
    }

    private static Collection<?> foreach(KeyRange[][] ranges, int[] widths, KeyRange[] expectedSplits) {
        List<List<KeyRange>> slots = Lists.transform(Lists.newArrayList(ranges), ARRAY_TO_LIST);
        RowKeySchemaBuilder builder = new RowKeySchemaBuilder().setMinNullable(10);
        for (final int width : widths) {
            builder.addField(new PDatum() {
                @Override
                public boolean isNullable() {
                    return false;
                }
                @Override
                public PDataType getDataType() {
                    return PDataType.CHAR;
                }
                @Override
                public Integer getByteSize() {
                    return width;
                }
                @Override
                public Integer getMaxLength() {
                    return width;
                }
                @Override
                public Integer getScale() {
                    return null;
                }
            });
        }
        List<Object> ret = Lists.newArrayList();
        ret.add(new Object[] {slots, widths, Arrays.<KeyRange>asList(expectedSplits)});
        return ret;
    }

    private static final Function<KeyRange[], List<KeyRange>> ARRAY_TO_LIST = 
            new Function<KeyRange[], List<KeyRange>>() {
                @Override 
                public List<KeyRange> apply(KeyRange[] input) {
                    return Lists.newArrayList(input);
                }
    };

    private static TableRef initTableValues(long ts, int targetQueryConcurrency, int maxQueryConcurrency) throws Exception {
        byte[][] splits = new byte[][] {Ka1A, Ka1E, Ka2A, Ka2Z, Kb1D, Kz9Z};
        Configuration config = driver.getQueryServices().getConfig();
        config.setInt(QueryServices.MAX_QUERY_CONCURRENCY_ATTRIB, maxQueryConcurrency);
        config.setInt(QueryServices.TARGET_QUERY_CONCURRENCY_ATTRIB, targetQueryConcurrency);
        createTestTable(getUrl(),DDL,splits, ts-2);
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " + TABLE_NAME + " VALUES (?, ?)");
        stmt.setString(1, new String("a1B"));
        stmt.setInt(2, 1);
        stmt.execute();
        stmt.setString(1, new String("a1Z"));
        stmt.setInt(2, 2);
        stmt.execute();
        conn.commit();
        conn.close();
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        PSchema schema = pconn.getPMetaData().getSchemas().get("");
        return new TableRef(null,schema.getTable(TABLE_NAME),schema, ts);
    }

    private static NavigableMap<HRegionInfo, ServerName> getRegions(TableRef table) throws IOException {
        return MetaScanner.allTableRegions(driver.getQueryServices().getConfig(), table.getTableName(), false);
    }

    private static List<KeyRange> getSplits(TableRef table, final Scan scan, final NavigableMap<HRegionInfo, ServerName> regions,
            final ScanRanges scanRanges) 
            throws SQLException {
        PhoenixConnection connection = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        StatementContext context = new StatementContext(connection, null, Collections.emptyList(), 0, scan);
        context.setScanRanges(scanRanges);
        SkipRangeParallelIteratorRegionSplitter splitter = SkipRangeParallelIteratorRegionSplitter.getInstance(context, table);
        List<KeyRange> keyRanges = splitter.getSplits();
        Collections.sort(keyRanges, new Comparator<KeyRange>() {
            @Override
            public int compare(KeyRange o1, KeyRange o2) {
                return Bytes.compareTo(o1.getLowerRange(),o2.getLowerRange());
            }
        });
        return keyRanges;
    }
}
