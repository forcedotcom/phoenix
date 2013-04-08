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
package com.salesforce.phoenix.compile;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;


/**
 * Test for intersect method in {@link ScanRanges}
 */
@RunWith(Parameterized.class)
public class ScanRangesTest {

    private final ScanRanges scanRanges;
    private final KeyRange keyRange;
    private final boolean expectedResult;

    public ScanRangesTest(ScanRanges scanRanges, int[] widths,
            KeyRange keyRange, boolean expectedResult) {
        this.keyRange = keyRange;
        this.scanRanges = scanRanges;
        this.expectedResult = expectedResult;
    }

    @Test
    public void test() {
        assertEquals(expectedResult, scanRanges.intersect(keyRange));
    }

    private static KeyRange getKeyRange(byte[] lowerRange, boolean lowerInclusive, byte[] upperRange, boolean upperInclusive) {
        return KeyRange.getKeyRange(lowerRange, lowerInclusive, upperRange, upperInclusive, true);
    }
    
    private static KeyRange getKeyRange(byte[] lowerRange, boolean lowerInclusive, byte[] upperRange, boolean upperInclusive, boolean isFixedWidth) {
        return KeyRange.getKeyRange(lowerRange, lowerInclusive, upperRange, upperInclusive, isFixedWidth);
    }
    
    @Parameters(name="{0} {1} {2} {3} {4}")
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        // variable length test that demonstrates that null byte
        // must be added at end
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        getKeyRange(Bytes.toBytes("b"), false, Bytes.toBytes("c"), true, false),}},
                    new int[] {0}, getKeyRange(Bytes.toBytes("ba"), true, Bytes.toBytes("bb"), true),
                    true));
        // KeyRange covers the first scan range.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),},{
                        getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),},{
                        getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("D"), true),}},
                    new int[] {1,1,1}, getKeyRange(Bytes.toBytes("a9Z"), true, Bytes.toBytes("c0A"), true),
                    true));
        // KeyRange that requires a fixed width  exclusive lower bound to be bumped up
        // and made inclusive. Otherwise, the comparison thinks its bigger than it really is.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),},{
                        getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),},{
                        getKeyRange(Bytes.toBytes("A"), false, Bytes.toBytes("B"), true),}},
                    new int[] {1,1,1}, getKeyRange(Bytes.toBytes("b1A"), true, Bytes.toBytes("b1A"), true),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),},{
                        getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),},{
                        getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("D"), true),}},
                    new int[] {1,1,1}, getKeyRange(Bytes.toBytes("b0A"), true, Bytes.toBytes("b1C"), true),
                    true));
        // KeyRange intersect with the first scan range on range's upper end.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),},{
                        getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),},{
                        getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),}},
                    new int[] {1,1,1}, getKeyRange(Bytes.toBytes("b0A"), true, Bytes.toBytes("b1B"), true),
                    true));
         // ScanRanges is everything.
        testCases.addAll(
                foreach(ScanRanges.EVERYTHING, null, getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    true));
        // ScanRanges is nothing.
        testCases.addAll(
                foreach(ScanRanges.NOTHING, null, getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    false));
        // KeyRange below the first scan range.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),}},
                    new int[] {1}, getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),},{
                        getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),},{
                        getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),}},
                    new int[] {1,1,1}, getKeyRange(Bytes.toBytes("b1A"), true, Bytes.toBytes("b1A"), true),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("c"), true),},{
                        getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true),},{
                        getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("C"), true),}},
                    new int[] {1,1,1}, getKeyRange(Bytes.toBytes("a1A"), true, Bytes.toBytes("b1B"), false),
                    false));
        // KeyRange intersects with the first scan range on range's lower end.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),},{
                        getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),},{
                        getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("D"), true),}},
                    new int[] {1,1,1}, getKeyRange(Bytes.toBytes("b1C"), true, Bytes.toBytes("b2E"), true),
                    true));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),},{
                        getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),},{
                        getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("D"), true),}},
                    new int[] {1,1,1}, getKeyRange(Bytes.toBytes("b1D"), true, Bytes.toBytes("b2E"), true),
                    true));
        // KeyRange above the first scan range, no intersect.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),},{
                        getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),},{
                        getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("D"), true),
                        getKeyRange(Bytes.toBytes("G"), true, Bytes.toBytes("H"), true),}},
                    new int[] {1,1,1}, getKeyRange(Bytes.toBytes("b1E"), true, Bytes.toBytes("b1F"), true),
                    false));
        // KeyRange above the first scan range, with intersects.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),},{
                        getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),},{
                        getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("D"), true),
                        getKeyRange(Bytes.toBytes("G"), true, Bytes.toBytes("I"), true),}},
                    new int[] {1,1,1}, getKeyRange(Bytes.toBytes("b1E"), true, Bytes.toBytes("b1H"), true),
                    true));
        // KeyRange above the last scan range.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),},{
                        getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),},{
                        getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),}},
                    new int[] {1,1,1}, getKeyRange(Bytes.toBytes("b1B"), false, Bytes.toBytes("b2A"), true),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), false),},{
                        getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), false),},{
                        getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), false),}},
                    new int[] {1,1,1}, getKeyRange(Bytes.toBytes("b2A"), true, Bytes.toBytes("b2A"), true),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),},{
                        getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),},{
                        getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),}},
                    new int[] {1,1,1}, getKeyRange(Bytes.toBytes("c1A"), false, Bytes.toBytes("c9Z"), true),
                    false));
        // KeyRange contains unbound lower bound.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                        getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),},{
                        getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), true),}},
                    new int[] {1,1,1}, getKeyRange(KeyRange.UNBOUND_LOWER, false, Bytes.toBytes("a0Z"), true),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                        getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),},{
                        getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), true),}},
                    new int[] {1,1,1}, getKeyRange(KeyRange.UNBOUND_LOWER, false, Bytes.toBytes("a1C"), true),
                    true));
        return testCases;
    }

    private static Collection<?> foreach(ScanRanges ranges, int[] widths, KeyRange keyRange,
            boolean expectedResult) {
        List<Object> ret = Lists.newArrayList();
        ret.add(new Object[] {ranges, widths, keyRange, expectedResult});
        return ret;
    }

    private static Collection<?> foreach(KeyRange[][] ranges, int[] widths, KeyRange keyRange,
            boolean expectedResult) {
        List<List<KeyRange>> slots = Lists.transform(Lists.newArrayList(ranges), ARRAY_TO_LIST);
        RowKeySchemaBuilder builder = new RowKeySchemaBuilder().setMinNullable(10);
        for (final int width : widths) {
            if (width > 0) {
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
            } else {
                builder.addField(new PDatum() {
                    @Override
                    public boolean isNullable() {
                        return false;
                    }
                    @Override
                    public PDataType getDataType() {
                        return PDataType.VARCHAR;
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
        }
        ScanRanges scanRanges = ScanRanges.create(slots, builder.build());
        return foreach(scanRanges, widths, keyRange, expectedResult);
    }

    private static final Function<KeyRange[], List<KeyRange>> ARRAY_TO_LIST = 
            new Function<KeyRange[], List<KeyRange>>() {
                @Override 
                public List<KeyRange> apply(KeyRange[] input) {
                    return Lists.newArrayList(input);
                }
    };
}
