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
package com.salesforce.phoenix.util;

import static org.junit.Assert.assertArrayEquals;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.query.KeyRange.Bound;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;


/**
 * Test the SetKey method in ScanUtil.
 */
@RunWith(Parameterized.class)
public class ScanUtilTest {

    private final List<List<KeyRange>> slots;
    private final byte[] expectedKey;
    private final RowKeySchema schema;
    private final Bound bound;

    public ScanUtilTest(List<List<KeyRange>> slots, int[] widths, byte[] expectedKey, Bound bound) throws Exception {
        RowKeySchemaBuilder builder = new RowKeySchemaBuilder().setMinNullable(widths.length);
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
                    @Override
                    public ColumnModifier getColumnModifier() {
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
                        return null;
                    }
                    @Override
                    public Integer getMaxLength() {
                        return null;
                    }
                    @Override
                    public Integer getScale() {
                        return null;
                    }
                    @Override
                    public ColumnModifier getColumnModifier() {
                        return null;
                    }
                });
            }
        }
        this.schema = builder.build();
        this.slots = slots;
        this.expectedKey = expectedKey;
        this.bound = bound;
    }

    @Test
    public void test() {
        byte[] key = new byte[1024];
        int[] position = new int[slots.size()];
        int offset = ScanUtil.setKey(schema, slots, position, bound, key, 0, 0, slots.size());
        byte[] actualKey = new byte[offset];
        System.arraycopy(key, 0, actualKey, 0, offset);
        assertArrayEquals(expectedKey, actualKey);
    }

    @Parameters(name="{0} {1} {2} {3} {4}")
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        // 1, Lower bound, all single keys, all inclusive.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("A"), true),}},
                new int[] {1,1,1},
                ByteUtil.fillKey(PDataType.VARCHAR.toBytes("a1A"), 3),
                Bound.LOWER
                ));
        // 2, Lower bound, all range keys, all inclusive.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), true),}},
                new int[] {1,1,1},
                ByteUtil.fillKey(PDataType.VARCHAR.toBytes("a1A"), 3),
                Bound.LOWER
                ));
        // 3, Lower bound, mixed single and range keys, all inclusive.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("A"), true),}},
                new int[] {1,1,1},
                ByteUtil.fillKey(PDataType.VARCHAR.toBytes("a1A"), 3),
                Bound.LOWER
                ));
        // 4, Lower bound, all range key, all exclusive on lower bound.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), false, Bytes.toBytes("b"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), false, Bytes.toBytes("2"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("A"), false, Bytes.toBytes("B"), true),}},
                new int[] {1,1,1},
                ByteUtil.fillKey(PDataType.VARCHAR.toBytes("b2B"), 3),
                Bound.LOWER
                ));
        // 5, Lower bound, all range key, some exclusive.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), false, Bytes.toBytes("b"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("A"), false, Bytes.toBytes("B"), true),}},
                new int[] {1,1,1},
                ByteUtil.fillKey(PDataType.VARCHAR.toBytes("b1B"), 3),
                Bound.LOWER
                ));
        // 6, Lower bound, mixed single and range key, mixed inclusive and exclusive.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("A"), false, Bytes.toBytes("B"), true),}},
                new int[] {1,1,1},
                ByteUtil.fillKey(PDataType.VARCHAR.toBytes("a1B"), 3),
                Bound.LOWER
                ));
        // 7, Lower bound, unbound key in the middle, fixed length.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    PDataType.CHAR.getKeyRange(KeyRange.UNBOUND, true, KeyRange.UNBOUND, true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("A"), false, Bytes.toBytes("B"), true),}},
                new int[] {1,1,1},
                ByteUtil.fillKey(PDataType.VARCHAR.toBytes("a"), 1),
                Bound.LOWER
                ));
        // 8, Lower bound, unbound key in the middle, variable length.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                        PDataType.CHAR.getKeyRange(KeyRange.UNBOUND, true, KeyRange.UNBOUND, true),}},
                    new int[] {1,1},
                    ByteUtil.concat(PDataType.VARCHAR.toBytes("a")),
                    Bound.LOWER
                    ));
        // 9, Lower bound, unbound key at end, variable length.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    PDataType.CHAR.getKeyRange(KeyRange.UNBOUND, true, KeyRange.UNBOUND, true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), true),}},
                new int[] {1,1,1},
                ByteUtil.concat(PDataType.VARCHAR.toBytes("a")),
                Bound.LOWER
                ));
        // 10, Upper bound, all single keys, all inclusive, increment at end.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("A"), true),}},
                new int[] {1,1,1},
                ByteUtil.fillKey(PDataType.VARCHAR.toBytes("a1B"), 3),
                Bound.UPPER
                ));
        // 11, Upper bound, all range keys, all inclusive, increment at end.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), true),}},
                new int[] {1,1,1},
                ByteUtil.fillKey(PDataType.VARCHAR.toBytes("b2C"), 3),
                Bound.UPPER
                ));
        // 12, Upper bound, all range keys, all exclusive, no increment at end.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), false),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), false),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), false),}},
                new int[] {1,1,1},
                ByteUtil.fillKey(PDataType.VARCHAR.toBytes("b2B"), 3),
                Bound.UPPER
                ));
        // 13, Upper bound, single inclusive, range inclusive, increment at end.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true),}},
                new int[] {1,1},
                ByteUtil.fillKey(PDataType.VARCHAR.toBytes("a3"), 2),
                Bound.UPPER
                ));
        // 14, Upper bound, range exclusive, single inclusive, increment at end.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), false),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),}},
                new int[] {1,1},
                ByteUtil.fillKey(PDataType.VARCHAR.toBytes("b2"), 2),
                Bound.UPPER
                ));
        // 15, Upper bound, range inclusive, single inclusive, increment at end.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),}},
                new int[] {1,1},
                ByteUtil.fillKey(PDataType.VARCHAR.toBytes("b2"), 2),
                Bound.UPPER
                ));
        // 16, Upper bound, single inclusive, range exclusive, no increment at end.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), false),}},
                new int[] {1,1},
                ByteUtil.fillKey(PDataType.VARCHAR.toBytes("a2"), 2),
                Bound.UPPER
                ));
        // 17, Upper bound, unbound key, fixed length;
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    PDataType.CHAR.getKeyRange(KeyRange.UNBOUND, true, KeyRange.UNBOUND, true),}},
                new int[] {1,1},
                ByteUtil.fillKey(PDataType.VARCHAR.toBytes("b"), 1),
                Bound.UPPER
                ));
        // 18, Upper bound, unbound key, variable length;
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    PDataType.CHAR.getKeyRange(KeyRange.UNBOUND, true, KeyRange.UNBOUND, true),}},
                new int[] {1,1},
                ByteUtil.concat(PDataType.VARCHAR.toBytes("b")),
                Bound.UPPER
                ));
        // 19, Upper bound, keys wrapped around when incrementing.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                PDataType.CHAR.getKeyRange(new byte[] {-1}, true, new byte[] {-1}, true)},{
                PDataType.CHAR.getKeyRange(new byte[] {-1}, true, new byte[] {-1}, true)}},
                new int[] {1, 1},
                ByteUtil.EMPTY_BYTE_ARRAY,
                Bound.UPPER
                ));
        // 20, Variable length
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    PDataType.VARCHAR.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), true),}},
                new int[] {1,0},
                ByteUtil.nextKey(ByteUtil.concat(PDataType.VARCHAR.toBytes("aB"), QueryConstants.SEPARATOR_BYTE_ARRAY)),
                Bound.UPPER
                ));
        return testCases;
    }

    private static Collection<?> foreach(KeyRange[][] ranges, int[] widths, byte[] expectedKey,
            Bound bound) {
        List<List<KeyRange>> slots = Lists.transform(Lists.newArrayList(ranges), ARRAY_TO_LIST);
        List<Object> ret = Lists.newArrayList();
        ret.add(new Object[] {slots, widths, expectedKey, bound});
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
