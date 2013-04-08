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
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.schema.RowKeySchema;
import com.salesforce.phoenix.schema.ValueBitSet;
import com.salesforce.phoenix.util.ByteUtil;
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

    private static final ImmutableBytesWritable UNBOUND_LOWER = new ImmutableBytesWritable(KeyRange.UNBOUND_LOWER);
    private static final ImmutableBytesWritable UNBOUND_UPPER = new ImmutableBytesWritable(KeyRange.UNBOUND_UPPER);

    public boolean intersect(KeyRange keyRange) {
        if (isEverything()) {
            return true;
        }
        if (isDegenerate()) {
            return false;
        }
        byte[] lowerInclusiveKey = keyRange.getLowerRange();
        if (!keyRange.isLowerInclusive() && !Bytes.equals(lowerInclusiveKey, KeyRange.UNBOUND_LOWER)) {
            lowerInclusiveKey = ByteUtil.nextKey(lowerInclusiveKey);
        }
        byte[] upperExclusiveKey = keyRange.getUpperRange();
        if (keyRange.isUpperInclusive()) {
            upperExclusiveKey = ByteUtil.nextKey(upperExclusiveKey);
        }
        int i = 0;
        int[] position = new int[ranges.size()];
        
        ImmutableBytesWritable lowerPtr = new ImmutableBytesWritable();
        ImmutableBytesWritable upperPtr = new ImmutableBytesWritable();
        ImmutableBytesWritable lower = lowerPtr, upper = upperPtr;
        int nSlots = ranges.size();
        
        lowerPtr.set(lowerInclusiveKey, 0, lowerInclusiveKey.length);
        schema.first(lowerPtr, i, ValueBitSet.EMPTY_VALUE_BITSET);
        upperPtr.set(upperExclusiveKey, 0, upperExclusiveKey.length);
        schema.first(upperPtr, i, ValueBitSet.EMPTY_VALUE_BITSET);
        
        int cmpLower=0,cmpUpper=0;
        
        while (true) {
            // Increment to the next range while the upper bound of our current slot is less than our lower bound
            while (position[i] < ranges.get(i).size() && 
                    (cmpLower=ranges.get(i).get(position[i]).compareUpperToLowerBound(lower, true)) < 0) {
                position[i]++;
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
                if (schema.next(lowerPtr, i, lowerInclusiveKey.length, ValueBitSet.EMPTY_VALUE_BITSET) == null) {
                    // If no more lower key parts, then we have no constraint for that part of the key,
                    // so we use unbound lower from here on out.
                    lower = UNBOUND_LOWER;
                } else {
                    lower = lowerPtr;
                }
                if (schema.next(upperPtr, i, upperExclusiveKey.length, ValueBitSet.EMPTY_VALUE_BITSET) == null) {
                    // If no more upper key parts, then we have no constraint for that part of the key,
                    // so we use unbound upper from here on out.
                    upper = UNBOUND_UPPER;
                } else {
                    upper = upperPtr;
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
