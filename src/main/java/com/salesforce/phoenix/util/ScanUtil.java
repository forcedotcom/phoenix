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

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.coprocessor.MetaDataProtocol;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.query.KeyRange.Bound;
import com.salesforce.phoenix.schema.RowKeySchema;


/**
 * 
 * Various utilities for scans
 *
 * @author jtaylor
 * @since 0.1
 */
public class ScanUtil {
    
    private ScanUtil() {
    }

    public static void setTenantId(Scan scan, byte[] tenantId) {
        scan.setAttribute(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
    }

    // Use getTenantId and pass in column name to match against
    // in as PSchema attribute. If column name matches in 
    // KeyExpressions, set on scan as attribute
    public static ImmutableBytesWritable getTenantId(Scan scan) {
        // Create Scan with special aggregation column over which to aggregate
        byte[] tenantId = scan.getAttribute(PhoenixRuntime.TENANT_ID_ATTRIB);
        if (tenantId == null) {
            return null;
        }
        return new ImmutableBytesWritable(tenantId);
    }

    /**
     * Intersects the scan start/stop row with the startKey and stopKey
     * @param scan
     * @param startKey
     * @param stopKey
     * @return false if the Scan cannot possibly return rows and true otherwise
     */
    public static boolean intersectScanRange(Scan scan, byte[] startKey, byte[] stopKey) {
        boolean mayHaveRows = false;
        byte[] existingStartKey = scan.getStartRow();
        byte[] existingStopKey = scan.getStopRow();
        if (existingStartKey.length > 0) {
            if (startKey.length == 0 || Bytes.compareTo(existingStartKey, startKey) > 0) {
                startKey = existingStartKey;
            }
        } else {
            mayHaveRows = true;
        }
        if (existingStopKey.length > 0) {
            if (stopKey.length == 0 || Bytes.compareTo(existingStopKey, stopKey) < 0) {
                stopKey = existingStopKey;
            }
        } else {
            mayHaveRows = true;
        }
        scan.setStartRow(startKey);
        scan.setStopRow(stopKey);
        return mayHaveRows || Bytes.compareTo(scan.getStartRow(), scan.getStopRow()) < 0;
    }

    public static void andFilter(Scan scan, Filter andWithFilter) {
        Filter filter = scan.getFilter();
        if (filter == null) {
            scan.setFilter(andWithFilter); 
        } else if (filter instanceof FilterList && ((FilterList)filter).getOperator() == FilterList.Operator.MUST_PASS_ALL) {
            FilterList filterList = (FilterList)filter;
            List<Filter> allFilters = new ArrayList<Filter>(filterList.getFilters().size() + 1);
            allFilters.add(andWithFilter);
            allFilters.addAll(filterList.getFilters());
            scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,allFilters));
        } else {
            scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,Arrays.asList(andWithFilter, filter)));
        }
    }

    public static void setTimeRange(Scan scan, long ts) {
        try {
            scan.setTimeRange(MetaDataProtocol.MIN_TABLE_TIMESTAMP, ts);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] getMinKey(RowKeySchema schema, List<List<KeyRange>> slots) {
        return getKey(schema, slots, Bound.LOWER);
    }

    public static byte[] getMaxKey(RowKeySchema schema, List<List<KeyRange>> slots) {
        return getKey(schema, slots, Bound.UPPER);
    }

    private static byte[] getKey(RowKeySchema schema, List<List<KeyRange>> slots, Bound bound) {
        if (slots.isEmpty()) {
            return null;
        }
        int[] position = new int[slots.size()];
        int maxLength = 0;
        for (int i = 0; i < position.length; i++) {
            position[i] = bound == Bound.LOWER ? 0 : slots.get(i).size()-1;
            KeyRange range = slots.get(i).get(position[i]);
            maxLength += range.getRange(bound).length + (schema.getField(i).getType().isFixedWidth() ? 0 : 1);
        }
        byte[] key = new byte[maxLength];
        int length = setKey(schema, slots, position, bound, key, 0, 0, position.length);
        if (length == 0) {
            return null;
        }
        if (length == maxLength) {
            return key;
        }
        byte[] keyCopy = new byte[length];
        System.arraycopy(key, 0, keyCopy, 0, length);
        return keyCopy;
    }

    /*
     * Set the key by appending the keyRanges inside slots at positions as specified by the position array.
     * 
     * We need to increment part of the key range, or increment the whole key at the end, depending on the
     * bound we are setting and whether the key range is inclusive or exclusive. The logic for determining
     * whether to increment or not is:
     * range/single    boundary       bound      increment
     *  range          inclusive      lower         no
     *  range          inclusive      upper         yes, at the end if occurs at any slots.
     *  range          exclusive      lower         yes
     *  range          exclusive      upper         no
     *  single         inclusive      lower         no
     *  single         inclusive      upper         yes, at the end if it is the last slots.
     */
    public static int setKey(RowKeySchema schema, List<List<KeyRange>> slots, int[] position, Bound bound,
            byte[] key, int byteOffset, int slotStartIndex, int slotEndIndex) {
        int offset = byteOffset;
        boolean singleInclusiveUpperKeyAtEnd = false;
        boolean rangeExclusiveUpperKey = false;
        boolean isFixedWidth = false;
        for (int i = slotStartIndex; i < slotEndIndex; i++) {
            // Build up the key by appending the bound of each key range
            // from the current position of each slot. 
            KeyRange range = slots.get(i).get(position[i]);
            isFixedWidth = schema.getField(i).getType().isFixedWidth();
            /*
             * If the current slot is unbound then stop if:
             * 1) setting the upper bound. There's no value in
             *    continuing because nothing will be filtered.
             * 2) setting the lower bound when the type is fixed length
             *    for the same reason. However, if the type is variable width
             *    continue building the key because null values will be filtered
             *    since our separator byte will be appended and increment.
             */
            if (  range.isUnbound(bound) &&
                ( bound == Bound.UPPER || isFixedWidth) ){
                break;
            }
            // If we are setting the upper bound of using inclusive single key, we remember 
            // to increment the key if we exit the loop after this iteration.
            // 
            // We remember to increment the last slot if we are setting the upper bound with an
            // inclusive range key.
            //
            // We cannot combine the two flags together in case for single-inclusive key followed
            // by the range-exclusive key. In that case, we do not need to increment the end at the
            // end. But if we combine the two flag, the single inclusive key in the middle of the
            // key slots would cause the flag to become true.
            singleInclusiveUpperKeyAtEnd = range.isSingleKey() && range.isInclusive(bound) && bound == Bound.UPPER;
            rangeExclusiveUpperKey = rangeExclusiveUpperKey ||
                    (!range.isSingleKey() && range.isInclusive(bound) && bound == Bound.UPPER);
            byte[] bytes = range.getRange(bound);
            System.arraycopy(bytes, 0, key, offset, bytes.length);
            offset += bytes.length;
            if (i < schema.getMaxFields()-1 && !isFixedWidth) {
                key[offset++] = QueryConstants.SEPARATOR_BYTE;
            }
            // If we are setting the lower bound with an exclusive range key, we need to bump the
            // slot up;
            if (!range.isSingleKey() && !range.isInclusive(bound) && bound == Bound.LOWER) {
                if (!ByteUtil.nextKey(key, offset)) {
                    // Special case for not being able to increment.
                    // In this case we return a negative byteOffset to
                    // remove this part from the key being formed. Since the
                    // key has overflowed, this means that we should not
                    // have an end key specified.
                    return -byteOffset;
                }
            }
        }
        if (singleInclusiveUpperKeyAtEnd || rangeExclusiveUpperKey) {
            if (!ByteUtil.nextKey(key, offset)) {
                return -byteOffset;
            }
        }
        return offset - byteOffset;
    }

    public static boolean isKeyInclusive(Bound bound, int[] position, List<List<KeyRange>> slots) {
        // We declare the key as exclusive only when all the parts make up of it are exclusive.
        boolean inclusive = false;
        for (int i=0; i<slots.size(); i++) {
            if (slots.get(i).get(position[i]).isInclusive(bound)) {
                inclusive = true;
            }
        }
        return inclusive;
    }

    public static boolean incrementKey(List<List<KeyRange>> slots, int[] position, int steps, Bound bound) {
        for (int i=0; i<steps; i++) {
            if (!incrementKey(slots, position)) {
                return false;
            }
        }
        setBoundSlotPosition(bound, slots, position);
        return true;
    }

    public static boolean incrementKey(List<List<KeyRange>> slots, int[] position) {
        // Find first index on the current position that is a range slot.
        int idx;
        for (idx = 0; idx < slots.size(); idx++) {
            if (!slots.get(idx).get(position[idx]).isSingleKey()) {
                break;
            }
        }
        // No slot on the current position is a range.
        if (idx == slots.size()) {
            idx = slots.size() - 1;
        }
        while (idx >= 0 && (position[idx] = (position[idx] + 1) % slots.get(idx).size()) == 0) {
            idx--;
        }
        return idx >= 0;
    }

    private static void setBoundSlotPosition(Bound bound, List<List<KeyRange>> slots, int[] position) {
        // Find first index on the current position that is a range slot.
        int idx;
        for (idx = 0; idx < slots.size(); idx++) {
            if (!slots.get(idx).get(position[idx]).isSingleKey()) {
                break;
            }
        }
        // If the idx is not the last position, reset the slots beyond to become 0th position. If
        // we are setting the position for a lower bound, reset all of them to 0. If we are setting
        // the position for an uppser bound, reset all of them to the last index. If the bound is
        // not specified, set all positions to 0.
        if (idx < slots.size() - 1) {
            if (bound == Bound.LOWER) {
                for (int i = idx + 1; i < slots.size(); i++) {
                    position[i] = 0;
                }
            } else {
                for (int i = idx + 1; i < slots.size(); i++) {
                    position[i] = slots.get(i).size() - 1;
                }
            }
        }
    }
}
