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
 * Test for intersect method in {@link SkipScanFilter}
 */
@RunWith(Parameterized.class)
public class SkipScanFilterIntersectTest {

    private final SkipScanFilter filter;
    private final byte[] lowerInclusiveKey;
    private final byte[] upperExclusiveKey;
    private final List<List<KeyRange>> expectedNewSlots;

    public SkipScanFilterIntersectTest(List<List<KeyRange>> slots, RowKeySchema schema, byte[] lowerInclusiveKey,
            byte[] upperExclusiveKey, List<List<KeyRange>> expectedNewSlots) {
        this.filter = new SkipScanFilter(slots, schema);
        this.lowerInclusiveKey = lowerInclusiveKey;
        this.upperExclusiveKey = upperExclusiveKey;
        this.expectedNewSlots = expectedNewSlots;
    }

    @Test
    public void test() {
        SkipScanFilter intersectedFilter = filter.intersect(lowerInclusiveKey, upperExclusiveKey);
        if (expectedNewSlots == null && intersectedFilter == null) {
            return;
        }
        assertNotNull("Intersected filter should not be null", intersectedFilter);
        List<List<KeyRange>> newSlots = intersectedFilter.getSlots();
        assertSameSlots(expectedNewSlots, newSlots);
    }

    private void assertSameSlots(List<List<KeyRange>> expectedSlots, List<List<KeyRange>> slots) {
        assertEquals(expectedSlots.size(), slots.size());
        for (int i=0; i<expectedSlots.size(); i++) {
            List<KeyRange> expectedSlot = expectedSlots.get(i);
            List<KeyRange> slot = slots.get(i);
            assertEquals("index: " + i, expectedSlot.size(), slot.size());
            for (int j=0; j<expectedSlot.size(); j++) {
                KeyRange expectedRange = expectedSlot.get(j);
                KeyRange range = slot.get(j);
                assertArrayEquals(expectedRange.getLowerRange(), range.getLowerRange());
                assertArrayEquals(expectedRange.getUpperRange(), range.getUpperRange());
                assertEquals(expectedRange.isLowerInclusive(), range.isLowerInclusive());
                assertEquals(expectedRange.isUpperInclusive(), range.isUpperInclusive());
            }
        }
    }

    @Parameters(name="{0} {1} {2} {3} {4}")
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        // Causes increment of slot 2 to increment slot 1
        testCases.addAll(foreach(
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("e"), false),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("j"), true, Bytes.toBytes("m"), false),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                }},
                new int[] {1,1,1},
                Bytes.toBytes("j3A"),
                Bytes.toBytes("k4C"),
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("j"), true, Bytes.toBytes("m"), false),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                }}));
        // Single matching in the first 2 slots.
        testCases.addAll(foreach(
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true), 
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("d"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("3"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"), true),
                }},
                new int[] {1,1,1},
                Bytes.toBytes("b1B"),
                Bytes.toBytes("b1C"),
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                }}));
        // Single matching in the first slot.
        testCases.addAll(foreach(
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true), 
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"), true),
                }},
                new int[] {1,1,1},
                Bytes.toBytes("b1Z"),
                Bytes.toBytes("b3Z"),
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"), true),
                }}));
        // No overlap
        testCases.addAll(foreach(
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true), 
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("d"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("3"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"), true),
                }},
                new int[] {1,1,1},
                Bytes.toBytes("a0A"),
                Bytes.toBytes("b1B"),
                null));
        testCases.addAll(foreach(
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true), 
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("d"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("3"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"), true),
                }},
                new int[] {1,1,1},
                Bytes.toBytes("a0A"),
                Bytes.toBytes("b1C"),
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                }}));
        testCases.addAll(foreach(
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true), 
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("d"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("3"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"), true),
                }},
                new int[] {1,1,1},
                Bytes.toBytes("a0A"),
                Bytes.toBytes("b1D"),
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"), true),
                }}));
        testCases.addAll(foreach(
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true), 
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("d"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("3"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"), true),
                }},
                new int[] {1,1,1},
                Bytes.toBytes("a0A"),
                Bytes.toBytes("b1D"),
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"), true),
                }}));
        testCases.addAll(foreach(
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true), 
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("d"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("3"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"), true),
                }},
                new int[] {1,1,1},
                Bytes.toBytes("b1B"),
                Bytes.toBytes("b1D"),
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"), true),
                }}));
        testCases.addAll(foreach(
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true), 
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("d"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("3"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"), true),
                }},
                new int[] {1,1,1},
                Bytes.toBytes("a0A"),
                Bytes.toBytes("b1F"),
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"), true),
                }}));
        testCases.addAll(foreach(
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true), 
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"), true),
                }},
                new int[] {1,1,1},
                Bytes.toBytes("a0Z"),
                Bytes.toBytes("b3Z"),
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"), true),
                }}));
        testCases.addAll(foreach(
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true), 
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"), true),
                }},
                new int[] {1,1,1},
                Bytes.toBytes("a0Z"),
                Bytes.toBytes("b9Z"),
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"), true),
                }}));
        // Multiple matching in all slot.
        testCases.addAll(foreach(
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true), 
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"), true),
                }},
                new int[] {1,1,1},
                Bytes.toBytes("a0Z"),
                Bytes.toBytes("c3Z"),
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true), 
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"), true),
                }}));
        testCases.addAll(foreach(
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true), 
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"), true),
                }},
                new int[] {1,1,1},
                Bytes.toBytes("a0A"),
                Bytes.toBytes("f4F"),
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true), 
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"), true),
                }}));
        // VARCHAR as the last column, various cases.
        testCases.addAll(foreach(
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true), 
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"), true),
                }},
                new int[] {1,1,-1},
                Bytes.toBytes("d3AA"),
                Bytes.toBytes("d4FF"),
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"), true),
                }}));
        testCases.addAll(foreach(
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true), 
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"), true),
                }},
                new int[] {1,1,-1},
                Bytes.toBytes("d0AA"),
                Bytes.toBytes("d4FF"),
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"), true),
                }}));
        testCases.addAll(foreach(
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true), 
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"), true),
                }},
                new int[] {1,1,-1},
                Bytes.toBytes("a0AA"),
                Bytes.toBytes("f4FF"),
                new KeyRange[][] {{
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true), 
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true),
                    }, {
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true),
                    PDataType.CHAR.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"), true),
                }}));
        return testCases;
    }

    private static Collection<?> foreach(KeyRange[][] ranges, int[] widths, byte[] lowerInclusive,
            byte[] upperExclusive, KeyRange[][] expectedRanges) {
        List<List<KeyRange>> slots = Lists.transform(Lists.newArrayList(ranges), ARRAY_TO_LIST);
        List<List<KeyRange>> expectedSlots = expectedRanges == null ? null : Lists.transform(Lists.newArrayList(expectedRanges), ARRAY_TO_LIST);
        RowKeySchemaBuilder builder = new RowKeySchemaBuilder().setMinNullable(10);
        for (final int width: widths) {
            builder.addField(
                    new PDatum() {
                        @Override
                        public boolean isNullable() {
                            return width <= 0;
                        }
                        @Override
                        public PDataType getDataType() {
                            return width <= 0 ? PDataType.VARCHAR : PDataType.CHAR;
                        }
                        @Override
                        public Integer getByteSize() {
                            return width <= 0 ? null : width;
                        }
                        @Override
                        public Integer getMaxLength() {
                            return getByteSize();
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
        List<Object> ret = Lists.newArrayList();
        ret.add(new Object[] {slots, builder.build(), lowerInclusive, upperExclusive, expectedSlots});
        return ret;
    }

    private static final Function<KeyRange[], List<KeyRange>> ARRAY_TO_LIST = new Function<KeyRange[], List<KeyRange>>() {
        @Override public List<KeyRange> apply(KeyRange[] input) {
            return Lists.newArrayList(input);
        }
    };
}
