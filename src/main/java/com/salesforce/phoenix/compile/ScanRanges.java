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

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.schema.RowKeySchema;
import com.salesforce.phoenix.schema.ValueBitSet;
import com.salesforce.phoenix.util.ScanUtil;


public class ScanRanges {
    private static final List<List<KeyRange>> EVERYTHING_RANGES = Collections.<List<KeyRange>>emptyList();
    private static final List<List<KeyRange>> NOTHING_RANGES = Collections.<List<KeyRange>>singletonList(Collections.<KeyRange>singletonList(KeyRange.EMPTY_RANGE));
    public static final ScanRanges EVERYTHING = new ScanRanges(EVERYTHING_RANGES,null);
    public static final ScanRanges NOTHING = new ScanRanges(NOTHING_RANGES,null);

    public static ScanRanges create(List<List<KeyRange>> ranges, RowKeySchema schema) {
        if (ranges.isEmpty()) {
            return EVERYTHING;
        } else if (ranges.size() == 1 && ranges.get(0).size() == 1 && ranges.get(0).get(0) == KeyRange.EMPTY_RANGE) {
            return NOTHING;
        }
        return new ScanRanges(ranges, schema);
    }

    private final List<List<KeyRange>> ranges;
    private final RowKeySchema schema;

    private ScanRanges (List<List<KeyRange>> ranges, RowKeySchema schema) {
        this.ranges = ranges;
        this.schema = schema;
    }

    public List<List<KeyRange>> getRanges() {
        return ranges;
    }

    public RowKeySchema getSchema() {
        return schema;
    }

    public boolean isEverything() {
        return this == EVERYTHING;
    }

    public boolean isDegenerate() {
        return this == NOTHING;
    }
    
    /**
     * Use SkipScanFilter under two circumstances:
     * 1) If we have multiple ranges for a given key slot (use of IN)
     * 2) If we have a range (i.e. not a single/point key) that is
     *    not the last key slot
     */
    public boolean useSkipScanFilter() {
        boolean hasRangeKey = false, useSkipScan = false;
        for (List<KeyRange> orRanges : ranges) {
            useSkipScan |= orRanges.size() > 1 | hasRangeKey;
            if (useSkipScan) {
                return true;
            }
            for (KeyRange range : orRanges) {
                hasRangeKey |= !range.isSingleKey();
            }
        }
        return false;
    }

    /**
     * @return true if this represents the full key to a single row
     */
    public boolean isSingleRowScan() {
        if (schema == null || ranges.size() < schema.getMaxFields()) {
            return false;
        }
        boolean isSingleKey = true;
        for (List<KeyRange> orRanges : ranges) {
            if (orRanges.size() > 1) {
                return false;
            }
            isSingleKey &= orRanges.get(0).isSingleKey();
        }
        return isSingleKey;
    }

    public void setScanStartStopRow(Scan scan) {
        if (isEverything()) {
            return;
        }
        if (isDegenerate()) {
            scan.setStartRow(KeyRange.EMPTY_RANGE.getLowerRange());
            scan.setStopRow(KeyRange.EMPTY_RANGE.getUpperRange());
            return;
        }
        
        byte[] expectedKey;
        expectedKey = ScanUtil.getMinKey(schema, ranges);
        if (expectedKey != null) {
            scan.setStartRow(expectedKey);
        }
        expectedKey = ScanUtil.getMaxKey(schema, ranges);
        if (expectedKey != null) {
            scan.setStopRow(expectedKey);
        }
    }

    public static final ImmutableBytesWritable UNBOUND = new ImmutableBytesWritable(KeyRange.UNBOUND);

    /**
     * Return true if the range formed by the lowerInclusiveKey and upperExclusiveKey
     * intersects with any of the scan ranges and false otherwise. We cannot pass in
     * a KeyRange here, because the underlying compare functions expect lower inclusive
     * and upper exclusive keys. We cannot get their next key because the key must
     * conform to the row key schema and if a null byte is added to a lower inclusive
     * key, it's no longer a valid, real key.
     * @param lowerInclusiveKey lower inclusive key
     * @param upperExclusiveKey upper exclusive key
     * @return true if the scan range intersects with the specified lower/upper key
     * range
     */
    public boolean intersect(byte[] lowerInclusiveKey, byte[] upperExclusiveKey) {
        if (isEverything()) {
            return true;
        }
        if (isDegenerate()) {
            return false;
        }
        int i = 0;
        int[] position = new int[ranges.size()];
        
        ImmutableBytesWritable lowerPtr = new ImmutableBytesWritable();
        ImmutableBytesWritable upperPtr = new ImmutableBytesWritable();
        ImmutableBytesWritable lower = lowerPtr, upper = upperPtr;
        int nSlots = ranges.size();
        
        lowerPtr.set(lowerInclusiveKey, 0, lowerInclusiveKey.length);
        if (schema.first(lowerPtr, i, ValueBitSet.EMPTY_VALUE_BITSET) == null) {
            lower = UNBOUND;
        }
        upperPtr.set(upperExclusiveKey, 0, upperExclusiveKey.length);
        if (schema.first(upperPtr, i, ValueBitSet.EMPTY_VALUE_BITSET) == null) {
            upper = UNBOUND;
        }
        
        int cmpLower=0,cmpUpper=0;
        while (true) {
            // Search to the slot whose upper bound of is closest bigger or equal to our lower bound.
            position[i] = ScanUtil.searchClosestKeyRangeWithUpperHigherThanLowerPtr(ranges.get(i), lower);
            if (position[i] == ranges.get(i).size()) {
                // The return value indicates that no upper bound can be found to be bigger or equals
                // to the lower bound, therefore the cmpLower would always be -1.
                cmpLower = -1;
            } else {
                cmpLower = ranges.get(i).get(position[i]).compareUpperToLowerBound(lower, true);
            }
            if (position[i] >= ranges.get(i).size()) {
                // Our current key is bigger than the last range of the current slot.
                return false;
            } else if ((cmpUpper=ranges.get(i).get(position[i]).compareLowerToUpperBound(upper, i < nSlots-1)) > 0) {
                // Our upper bound is less than the lower range of the current position in the current slot.
                return false;
            } else { // We're in range, check the next slot
                i++;
                // Stop if no more slots or the range we have completely encompasses our key
                if (i >= nSlots || (cmpLower > 0 && cmpUpper < 0)) {
                    break;
                }
                
                // Move to the next part of the key
                if (lower != UNBOUND) {
                    if (schema.next(lowerPtr, i, lowerInclusiveKey.length, ValueBitSet.EMPTY_VALUE_BITSET) == null) {
                        // If no more lower key parts, then we have no constraint for that part of the key,
                        // so we use unbound lower from here on out.
                        lower = UNBOUND;
                    } else {
                        lower = lowerPtr;
                    }
                }
                if (upper != UNBOUND) {
                    if (schema.next(upperPtr, i, upperExclusiveKey.length, ValueBitSet.EMPTY_VALUE_BITSET) == null) {
                        // If no more upper key parts, then we have no constraint for that part of the key,
                        // so we use unbound upper from here on out.
                        upper = UNBOUND;
                    } else {
                        upper = upperPtr;
                    }
                }
            }
        }

        // We're in range for all slots and can include this row plus all rows
        // up to the upper range of our last slot
        return true;
   }

    @Override
    public String toString() {
        return "ScanRanges[" + ranges.toString() + "]";
    }

}
