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

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.salesforce.phoenix.filter.SkipScanFilter;
import com.salesforce.phoenix.iterate.SkipRangeParallelIteratorRegionSplitter;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;


/**
 * Test for {@link SkipRangeParallelIteratorRegionSplitter}.
 */
@RunWith(Parameterized.class)
public class SkipScanFilterRangeSplitTest extends BaseTest {

    private final static int MAX_CONCURRENCY = 5;
    private final SkipScanFilter filter;
    private final List<KeyRange> expectedSplits;

    public SkipScanFilterRangeSplitTest(List<List<KeyRange>> slots, int[] widths, KeyRange[] expectedSplits) throws SQLException {
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
        List<KeyRange> splits = Arrays.<KeyRange>asList(expectedSplits);
        Collections.sort(splits, new Comparator<KeyRange>() {
            @Override
            public int compare(KeyRange o1, KeyRange o2) {
                return Bytes.compareTo(o1.getLowerRange(),o2.getLowerRange());
            }
        });
        this.expectedSplits = splits;
    }

    @Test
    public void test() {
        // table and alltableRegions not used for SkipScanParallelIterator.
        List<KeyRange> keyRanges = filter.generateSplitRanges(MAX_CONCURRENCY);
        Collections.sort(keyRanges, new Comparator<KeyRange>() {
            @Override
            public int compare(KeyRange o1, KeyRange o2) {
                return Bytes.compareTo(o1.getLowerRange(),o2.getLowerRange());
            }
        });
        assertEquals("Unexpected number of splits: " + keyRanges, expectedSplits.size(), keyRanges.size());
        for (int i=0; i<keyRanges.size(); i++) {
            assertEquals("Expecting: " + expectedSplits.get(i), expectedSplits.get(i), keyRanges.get(i));
        }
    }

    @Parameters(name="{0} {1} {2}")
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        // All ranges are single keys.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("c"), true),
                }},
                new int[] {1},
                new KeyRange[] {
                    KeyRange.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("c"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("d"), false),
                }));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        KeyRange.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),
                        KeyRange.getKeyRange(Bytes.toBytes("k"), true, Bytes.toBytes("k"), true),
                        KeyRange.getKeyRange(Bytes.toBytes("t"), true, Bytes.toBytes("t"), true),
                    }},
                    new int[] {1},
                    new KeyRange[] {
                        KeyRange.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), false),
                        KeyRange.getKeyRange(Bytes.toBytes("k"), true, Bytes.toBytes("l"), false),
                        KeyRange.getKeyRange(Bytes.toBytes("t"), true, Bytes.toBytes("u"), false),
                    }));
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),
                }, {
                    KeyRange.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("c"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("d"), true, Bytes.toBytes("d"), true),
                }},
                new int[] {1,1},
                new KeyRange[] {
                    KeyRange.getKeyRange(Bytes.toBytes("ac"), true, Bytes.toBytes("ad"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("ad"), true, Bytes.toBytes("ae"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("bc"), true, Bytes.toBytes("bd"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("bd"), true, Bytes.toBytes("be"), false),
                }));
        // chunks 2 ranges into one.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("c"), true),
                }, {
                    KeyRange.getKeyRange(Bytes.toBytes("d"), true, Bytes.toBytes("d"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("e"), true, Bytes.toBytes("e"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("f"), true, Bytes.toBytes("f"), true),
                }},
                new int[] {1,1},
                new KeyRange[] {
                    KeyRange.getKeyRange(Bytes.toBytes("ad"), true, Bytes.toBytes("af"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("af"), true, Bytes.toBytes("be"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("be"), true, Bytes.toBytes("bg"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("cd"), true, Bytes.toBytes("cf"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("cf"), true, Bytes.toBytes("cg"), false),
                }));
        // Some slots contains range keys.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("c"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("d"), false),
                }},
                new int[] {1},
                new KeyRange[] {
                    KeyRange.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("c"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("d"), false),
                }));
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("d"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("e"), true, Bytes.toBytes("f"), true),
                },{
                    KeyRange.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("2"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("3"), true, Bytes.toBytes("3"), true),
                }},
                new int[] {1,1},
                new KeyRange[] {
                    KeyRange.getKeyRange(Bytes.toBytes("a1"), true, Bytes.toBytes("b4"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("c1"), true, Bytes.toBytes("d4"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("e1"), true, Bytes.toBytes("f4"), false),
                }));
        // 2 ranges in one chunk.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("c"), true),
                },{
                    KeyRange.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("3"), true, Bytes.toBytes("4"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("5"), true, Bytes.toBytes("6"), true),
                },{
                    KeyRange.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("A"), true),
                }},
                new int[] {1,1,1},
                new KeyRange[] {
                    KeyRange.getKeyRange(Bytes.toBytes("a1A"), true, Bytes.toBytes("a4B"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("a5A"), true, Bytes.toBytes("b2B"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("b3A"), true, Bytes.toBytes("b6B"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("c1A"), true, Bytes.toBytes("c4B"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("c5A"), true, Bytes.toBytes("c6B"), false),
                }));
        // Combination of cases. 19 ranges, 4 ranges in each chunk, 3 ranges in last chunk.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("c"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("d"), true, Bytes.toBytes("d"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("e"), true, Bytes.toBytes("e"), true),
                },{
                    KeyRange.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("3"), true, Bytes.toBytes("3"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("4"), true, Bytes.toBytes("4"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("5"), true, Bytes.toBytes("6"), true),
                },{
                    KeyRange.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("A"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                }},
                new int[] {1,1,1},
                new KeyRange[] {
                    KeyRange.getKeyRange(Bytes.toBytes("a1A"), true, Bytes.toBytes("a4B"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("a4B"), true, Bytes.toBytes("d2C"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("d3B"), true, Bytes.toBytes("d6C"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("e1A"), true, Bytes.toBytes("e4B"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("e4B"), true, Bytes.toBytes("e6C"), false),
                }));
        return testCases;
    }

    private static Collection<?> foreach(KeyRange[][] ranges, int[] widths, KeyRange[] expectedSplits) {
        List<List<KeyRange>> slots = Lists.transform(Lists.newArrayList(ranges), ARRAY_TO_LIST);
        List<Object> ret = Lists.newArrayList();
        ret.add(new Object[] {slots, widths, expectedSplits});
        return ret;
    }

    private static final Function<KeyRange[], List<KeyRange>> ARRAY_TO_LIST = 
            new Function<KeyRange[], List<KeyRange>>() {
                @Override 
                public List<KeyRange> apply(KeyRange[] input) {
                    return Lists.newArrayList(input);
                }
    };
}
