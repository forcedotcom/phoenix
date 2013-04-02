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
package com.salesforce.phoenix.filter;

import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.salesforce.phoenix.end2end.BaseClientMangedTimeTest;
import com.salesforce.phoenix.filter.SkipScanFilter;
import com.salesforce.phoenix.iterate.SkipRangeParallelIteratorRegionSplitter;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.query.KeyRange.Bound;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import com.salesforce.phoenix.util.TestUtil;

/**
 * Test for {@link SkipRangeParallelIteratorRegionSplitter}.
 *
 * @author zhuang
 */
@RunWith(Parameterized.class)
public class SkipScanParallelIteratorsTest extends BaseTest {

    private final ConnectionQueryServices services;
    private final Scan scan;
    private final Expectation expectation;

    public SkipScanParallelIteratorsTest(List<List<KeyRange>> slots, int[] widths, Expectation expectation) throws SQLException {
        this.services = mock(ConnectionQueryServices.class);
        Configuration config = mock(Configuration.class);
        when(services.getConfig()).thenReturn(config);
        when(config.getInt(QueryServices.TARGET_QUERY_CONCURRENCY_ATTRIB, QueryServicesOptions.DEFAULT_TARGET_QUERY_CONCURRENCY)).thenReturn(3);
        when(config.getInt(QueryServices.MAX_QUERY_CONCURRENCY_ATTRIB, QueryServicesOptions.DEFAULT_MAX_QUERY_CONCURRENCY)).thenReturn(5);
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
        this.scan = new Scan();
        this.scan.setFilter(new SkipScanFilter(slots, builder.build()));
        this.expectation = expectation;
    }

    @Test
    public void test() {
        expectation.examine(services, scan);
    }

    @Parameters(name="{0} {1} {2}")
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        // All ranges are single keys.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("aaa"), true, Bytes.toBytes("aaa"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("bbb"), true, Bytes.toBytes("bbb"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("ccc"), true, Bytes.toBytes("ccc"), true),
                }},
                new int[] {3},
                new KeyRange[] {
                    KeyRange.getKeyRange(Bytes.toBytes("aaa"), true, Bytes.toBytes("aab"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("bbb"), true, Bytes.toBytes("bbc"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("ccc"), true, Bytes.toBytes("ccd"), false),
                }));
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("aaa"), true, Bytes.toBytes("aaa"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("bbb"), true, Bytes.toBytes("bbb"), true),
                }, {
                    KeyRange.getKeyRange(Bytes.toBytes("ccc"), true, Bytes.toBytes("ccc"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("ddd"), true, Bytes.toBytes("ddd"), true),
                }},
                new int[] {3,3},
                new KeyRange[] {
                    KeyRange.getKeyRange(Bytes.toBytes("aaaccc"), true, Bytes.toBytes("aaaccd"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("aaaddd"), true, Bytes.toBytes("aaadde"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("bbbccc"), true, Bytes.toBytes("bbbccd"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("bbbddd"), true, Bytes.toBytes("bbbdde"), false),
                }));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        KeyRange.getKeyRange(Bytes.toBytes("aaa"), true, Bytes.toBytes("aaa"), true),
                        KeyRange.getKeyRange(Bytes.toBytes("bbb"), true, Bytes.toBytes("bbb"), true),
                        KeyRange.getKeyRange(Bytes.toBytes("ccc"), true, Bytes.toBytes("ccc"), true),
                    }, {
                        KeyRange.getKeyRange(Bytes.toBytes("ddd"), true, Bytes.toBytes("ddd"), true),
                        KeyRange.getKeyRange(Bytes.toBytes("eee"), true, Bytes.toBytes("eee"), true),
                        KeyRange.getKeyRange(Bytes.toBytes("fff"), true, Bytes.toBytes("fff"), true),
                    }},
                    new int[] {3,3},
                    new KeyRange[] {
                        KeyRange.getKeyRange(Bytes.toBytes("aaaddd"), true, Bytes.toBytes("aaaeed"), false),
                        KeyRange.getKeyRange(Bytes.toBytes("aaafff"), true, Bytes.toBytes("bbbdde"), false),
                        KeyRange.getKeyRange(Bytes.toBytes("bbbeee"), true, Bytes.toBytes("bbbffg"), false),
                        KeyRange.getKeyRange(Bytes.toBytes("cccddd"), true, Bytes.toBytes("ccceef"), false),
                        KeyRange.getKeyRange(Bytes.toBytes("cccfff"), true, Bytes.toBytes("cccffg"), false),
                    }));
        // Some slots contains range keys.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        
                    }},
                    new int[] {3},
                    new KeyRange[] {
                        
                    }
                    ));
        // r/2 > t
        // split each key range into s splits such that:
        // s = max(x) where s * x < m
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        
                    }},
                    new int[] {3},
                    new KeyRange[] {
                        
                    }
                    ));
        return testCases;
    }

    private static Collection<?> foreach(KeyRange[][] ranges, int[] widths, KeyRange[] expectedSplits) {
        List<List<KeyRange>> slots = Lists.transform(Lists.newArrayList(ranges), ARRAY_TO_LIST);
        List<Object> ret = Lists.newArrayList();
        ret.add(new Object[] {slots, widths, new SplitRanges(expectedSplits)});
        return ret;
    }

    private static final Function<KeyRange[], List<KeyRange>> ARRAY_TO_LIST = 
            new Function<KeyRange[], List<KeyRange>>() {
                @Override 
                public List<KeyRange> apply(KeyRange[] input) {
                    return Lists.newArrayList(input);
                }
    };

    private static interface Expectation {
        void examine(ConnectionQueryServices services, Scan scan);
    }

    private static final class SplitRanges implements Expectation {
        private final List<KeyRange> expectedSplits;
        
        public SplitRanges(KeyRange[] expectedSplits) {
            List<KeyRange> splits = Arrays.<KeyRange>asList(expectedSplits);
            Collections.sort(splits, new Comparator<KeyRange>() {
                @Override
                public int compare(KeyRange o1, KeyRange o2) {
                    return Bytes.compareTo(o1.getLowerRange(),o2.getLowerRange());
                }
            });
            this.expectedSplits = splits;
        }
        
        @Override
        public void examine(ConnectionQueryServices services, Scan scan) {
            // table and alltableRegions not used for SkipScanParallelIterator.
            List<KeyRange> keyRanges = SkipRangeParallelIteratorRegionSplitter.getInstance().getSplits(services, null, scan, null);
            Collections.sort(keyRanges, new Comparator<KeyRange>() {
                @Override
                public int compare(KeyRange o1, KeyRange o2) {
                    return Bytes.compareTo(o1.getLowerRange(),o2.getLowerRange());
                }
            });
            assertEquals("Unexpected number of splits: " + keyRanges, expectedSplits.size(), keyRanges.size());
            for (int i=0; i<expectedSplits.size(); i++) {
                System.out.println(expectedSplits.get(i));
                System.out.println(keyRanges.get(i));
                System.out.println(keyRanges.get(i).isInclusive(Bound.LOWER));
                System.out.println(keyRanges.get(i).isInclusive(Bound.UPPER));
//                assertEquals(expectedSplits.get(i), keyRanges.get(i));
            }
        }
    }
}
