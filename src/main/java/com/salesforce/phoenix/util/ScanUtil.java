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

    public static int setKey(RowKeySchema schema, List<List<KeyRange>> slots, int[] position, Bound bound,
            byte[] key, int byteOffset, int slotStartIndex, int slotEndIndex) {
        int offset = byteOffset;
        boolean incrementKey = bound == Bound.UPPER;
        boolean incrementAtEnd = false;
        for (int i = slotStartIndex; i < slotEndIndex; i++) {
            // Build up the key by appending the bound of each key range
            // from the current position of each slot. 
            KeyRange range = slots.get(i).get(position[i]);
            boolean isFixedWidth = schema.getField(i).getType().isFixedWidth();
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
            // We increment the key at the current slot if it's an exclusive range key and we are
            // setting the lower bound.
            // We also use this argument to remember if this is a single inclusive upper bound key.
            // But we only increment the key if this is the last slot.
            incrementKey = (!range.isInclusive(bound) && bound == Bound.LOWER) || 
                    (range.isSingleKey() && range.isInclusive(bound) && bound == Bound.UPPER);
            // We remember to increment the last slot if this is an inclusive key, we are setting 
            // the upper bound and it's a range key;
            incrementAtEnd = incrementAtEnd ||
                    (!range.isSingleKey() && range.isInclusive(bound) && bound == Bound.UPPER);
            byte[] bytes = range.getRange(bound);
            System.arraycopy(bytes, 0, key, offset, bytes.length);
            offset += bytes.length;
            if (i < schema.getMaxFields()-1 && !isFixedWidth) {
                key[offset++] = QueryConstants.SEPARATOR_BYTE;
            }
            
            if (!range.isSingleKey() && incrementKey) {
                if (!ByteUtil.nextKey(key, offset)) {
                    // Special case for not being able to increment
                    return -byteOffset;
                }
                incrementKey = false;
            }
        }
        if (incrementAtEnd || incrementKey) {
            ByteUtil.nextKey(key, offset);
        }
        return offset - byteOffset;
    }

    public static int setKeyOld(RowKeySchema schema, List<List<KeyRange>> slots, int[] position, Bound bound,
            byte[] key, int byteOffset, int slotStartIndex, int slotEndIndex) {
        int offset = byteOffset;
        // Increment the key if we're setting an upper range by default
        boolean incrementKey = bound == Bound.UPPER;
        for (int i = slotStartIndex; i < slotEndIndex; i++) {
            // Build up the key by appending the bound of each key range
            // from the current position of each slot. 
            KeyRange range = slots.get(i).get(position[i]);
            boolean isFixedWidth = schema.getField(i).getType().isFixedWidth();
            
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
            incrementKey = range.isInclusive(bound) ^ bound == Bound.LOWER;

            byte[] bytes = range.getRange(bound);
            System.arraycopy(bytes, 0, key, offset, bytes.length);
            offset += bytes.length;
            if (i < schema.getMaxFields()-1 && !isFixedWidth) {
                key[offset++] = QueryConstants.SEPARATOR_BYTE;
            }
            
            if (!range.isSingleKey() && incrementKey) {
                if (!ByteUtil.nextKey(key, offset)) {
                    // Special case for not being able to increment
                    return -byteOffset;
                }
                incrementKey = false;
            }
        }
        
        if (incrementKey) {
            ByteUtil.nextKey(key, offset);
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

    public static void setBoundSlotPosition(Bound bound, List<List<KeyRange>> slots, int[] position) {
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
