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

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Objects;
import com.google.common.hash.*;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.query.KeyRange.Bound;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.schema.ValueSchema.Field;
import com.salesforce.phoenix.util.ByteUtil;

public class SkipScanFilter extends FilterBase {
    private List<List<KeyRange>> slots;
    private RowKeySchema schema;
    private int[] position; // current position for each slot
    private byte[] startKey; // buffer used for skip hint
    private int startKeyLength;
    private byte[] endKey; // buffer used for current end key after which we need to increment the position
    private int endKeyLength;
    private int maxKeyLength;
    private boolean includeUntilEndKey;
    private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();

    /**
     * We know that initially the first row will be positioned at or 
after the first possible key.
     */
    public SkipScanFilter() {
    }
    
    public SkipScanFilter(List<List<KeyRange>> slots, RowKeySchema schema) {
        int maxKeyLength = getTerminatorCount(schema);
        for (List<KeyRange> slot : slots) {
            int maxSlotLength = 0;
            for (KeyRange range : slot) {
                int maxRangeLength = Math.max(range.getLowerRange().length, range.getUpperRange().length);
                if (maxSlotLength < maxRangeLength) {
                    maxSlotLength = maxRangeLength;
                }
            }
            maxKeyLength += maxSlotLength;
        }
        init(slots, schema, maxKeyLength);
    }

    private void init(List<List<KeyRange>> slots, RowKeySchema schema, int maxKeyLength) {
        this.slots = slots;
        // TODO: add back precondition checks
        this.schema = schema;
        this.maxKeyLength = maxKeyLength;
        this.position = new int[slots.size()];
        startKey = new byte[maxKeyLength];
        endKey = new byte[maxKeyLength];
        // Start key for the scan will initially be set to start at the right place
        // We just need to set the end key for when we need to calculate the next skip hint
        // TODO: shouldn't be necessary, since the start key of the scan should be set to this
        // or a higher value
        setStartKey();
        setEndKey();
    }

    @Override
    public boolean filterAllRemaining() {
        return startKey == null;
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue kv) {
        return navigate(kv.getBuffer(), kv.getRowOffset(), 
kv.getRowLength());
    }

    @Override
    public KeyValue getNextKeyHint(KeyValue kv) {
        // TODO: don't allocate new key value every time here
        return endKey == null ? null : KeyValue.createFirstOnRow(endKey);
    }

    private ReturnCode navigate(final byte[] currentKey, final int offset, final int length) {
        if (startKey == null) { // TODO: verify not necessary and remove
            return ReturnCode.NEXT_ROW;
        }
        // TODO: remove this assert eventually
        assert(Bytes.compareTo(currentKey, offset, length, startKey, 0, startKeyLength) >= 0);
        if (Bytes.compareTo(currentKey, offset, length, endKey, 0, endKeyLength) <= 0) {
            if (includeUntilEndKey) {
                return ReturnCode.INCLUDE;
            }
            int i;
            int nSlots = slots.size();
            for (i = 0; i < nSlots; i++) {
                if (!slots.get(i).get(position[i]).isSingleKey()) {
                    i++;
                    break;
                }
            }
            if (i < nSlots) {
                ptr.set(currentKey, offset, length);
                for (   Boolean hasValue = schema.first(ptr, i, ValueBitSet.EMPTY_VALUE_BITSET); 
                        hasValue != null; 
                        hasValue = ++i == nSlots ? null : schema.next(ptr, i, ValueBitSet.EMPTY_VALUE_BITSET)) {
                    KeyRange range = slots.get(i).get(position[i]);
                    // TODO: can't the iterator set the length to zero in this case?
                    // TODO: if no value, then this is null and most likely shouldn't be included.
                    // We probably want to use {1} as our byte value instead of leaving it empty
                    // when a range is unbound, so that null is filtered
                    if (!hasValue || !range.isInRange(ptr.get(), ptr.getOffset(), ptr.getLength())) {
                        break;
                    }
                }
                int partialLength = ptr.getOffset() - offset;
                int maxLength = partialLength + this.maxKeyLength;
                if (i < nSlots) {
                    setStartKey(maxLength, currentKey, offset, partialLength);
                    appendToStartKey(i, partialLength);
                    return ReturnCode.SEEK_NEXT_USING_HINT;
                } else if (!slots.get(nSlots-1).get(position[i]).isSingleKey()) {
                    // Optimization: Last slot is range - we can include until we reach
                    // end range for this slot now that we know we're in range.
                    setEndKey(maxLength, currentKey, offset, partialLength);
                    appendToEndKey(nSlots-1, partialLength);
                    includeUntilEndKey = true;
                }
            }
            if (i == nSlots) { // Include this row, since we're in range for all slots
                // If we haven't set includeUntilEndKey, it means that we're currently at a single key.
                // In this case, we can do the same optimization as above, basically including all key
                // values with this row key.
                if (!includeUntilEndKey) {
                    setEndKey(length, currentKey, offset, length);
                    includeUntilEndKey = true;
                }
                return ReturnCode.INCLUDE;
            }
        }
        includeUntilEndKey = false;
        if (incrementKey()) {
            setStartKey();
            setEndKey();
            return ReturnCode.SEEK_NEXT_USING_HINT;
        }
        startKey = null;
        return ReturnCode.NEXT_ROW;
    }

    private boolean incrementKey() {
        int i = slots.size() - 1;
        // Starting at last slot, increment it's current position, modded with size.
        // Continue moving to the left when we've wrapped the current slot position.
        // Stop when we're at the beginning (and we're done)
        while (i > 0 && (position[i] = (position[i] + 1) % slots.get(i).size()) == 0) {
            i--;
        }
        return i >= 0;
    }

    private static byte[] copyKey(byte[] targetKey, int targetLength, byte[] sourceKey, int offset, int length) {
        if (targetLength > targetKey.length) {
            targetKey = new byte[targetLength];
        }
        System.arraycopy(sourceKey, offset, targetKey, 0, length);
        return targetKey;
    }
    
    private void setStartKey(int maxLength, byte[] sourceKey, int offset, int length) {
        startKey = copyKey(startKey, maxLength, sourceKey, offset, length);
        startKeyLength = length;
    }
    
    private void setEndKey(int maxLength, byte[] sourceKey, int offset, int length) {
        endKey = copyKey(endKey, maxLength, sourceKey, offset, length);
        endKeyLength = length;
    }
    
    private int setKey(Bound bound, byte[] key, int slotIndex, int byteOffset) {
        int offset = byteOffset;
        int nSlots = slots.size();
        boolean incremented = false;
        for (int i = slotIndex; i < nSlots; i++) {
            // build up the start key by appending lower bound of key range
            // from the current position of each slot append to startKey buffer 
            KeyRange range = slots.get(i).get(position[i]);
            if (range.isUnbound(bound)) {
                break;
            }
            byte[] bytes = range.getRange(bound);
            System.arraycopy(bytes, 0, key, offset, bytes.length);
            offset += bytes.length;
            if (i < nSlots-1 && !schema.getField(i).getType().isFixedWidth()) {
                key[offset++] = QueryConstants.SEPARATOR_BYTE;
            }
            if (!incremented && !range.isSingleKey() && (bound == Bound.UPPER) == range.isInclusive(bound)) {
                incremented = true;
                ByteUtil.nextKey(key, offset);
            }
        }
        return offset - byteOffset;
    }

    private void setStartKey() {
        startKeyLength = setKey(Bound.LOWER, startKey, 0, 0);
    }

    private void appendToStartKey(int slotIndex, int byteOffset) {
        startKeyLength += setKey(Bound.LOWER, startKey, slotIndex, byteOffset);
    }

    private void setEndKey() {
        endKeyLength = setKey(Bound.UPPER, endKey, 0, 0);
    }

    private void appendToEndKey(int slotIndex, int byteOffset) {
        endKeyLength += setKey(Bound.UPPER, endKey, slotIndex, byteOffset);
    }

    private int getTerminatorCount(RowKeySchema schema) {
        int nTerminators = 0;
        for (int i = 0; i < schema.getFieldCount() - 1; i++) {
            Field field = schema.getField(i);
            if (!field.getType().isFixedWidth()) {
                nTerminators++;
            }
        }
        return nTerminators;
    }
    
    @Override public void readFields(DataInput in) throws IOException {
        RowKeySchema schema = new RowKeySchema();
        schema.readFields(in);
        int maxLength = getTerminatorCount(schema);
        int n = in.readInt();
        List<List<KeyRange>> slots = new ArrayList<List<KeyRange>>();
        for (int i=0; i<n; i++) {
            int orlen = in.readInt();
            List<KeyRange> orclause = new ArrayList<KeyRange>();
            slots.add(orclause);
            int maxSlotLength = 0;
            for (int j=0; j<orlen; j++) {
                byte[] lower = WritableUtils.readCompressedByteArray(in);
                if (lower.length > maxSlotLength) {
                    maxSlotLength = lower.length;
                }
                boolean lowerInclusive = in.readBoolean();
                byte[] upper = WritableUtils.readCompressedByteArray(in);
                if (upper.length > maxSlotLength) {
                    maxSlotLength = upper.length;
                }
                boolean upperInclusive = in.readBoolean();
                orclause.add(
                    KeyRange.getKeyRange(lower, lowerInclusive,
                            upper, upperInclusive));
            }
            maxLength += maxSlotLength;
        }
        this.init(slots, schema, maxLength);
    }

    @Override public void write(DataOutput out) throws IOException {
        schema.write(out);
        out.writeInt(slots.size());
        for (List<KeyRange> orclause : slots) {
            out.writeInt(orclause.size());
            for (KeyRange arr : orclause) {
                WritableUtils.writeCompressedByteArray(out, arr.getLowerRange());
                out.writeBoolean(arr.isLowerInclusive());
                WritableUtils.writeCompressedByteArray(out, arr.getUpperRange());
                out.writeBoolean(arr.isUpperInclusive());
            }
        }
    }

    @Override public int hashCode() {
        HashFunction hf = Hashing.goodFastHash(32);
        Hasher h = hf.newHasher();
        h.putInt(slots.size());
        for (int i=0; i<slots.size(); i++) {
            h.putInt(slots.get(i).size());
            for (int j=0; j<slots.size(); j++) {
                h.putBytes(slots.get(i).get(j).getLowerRange());
                h.putBytes(slots.get(i).get(j).getUpperRange());
            }
        }
        return h.hash().asInt();
    }

    @Override public boolean equals(Object obj) {
        if (!(obj instanceof SkipScanFilter)) return false;
        SkipScanFilter other = (SkipScanFilter)obj;
        return Objects.equal(slots, other.slots) && Objects.equal(schema, other.schema);
    }

    @Override public String toString() {
        // TODO: make static util methods in ExplainTable that use type to print
        // key ranges
        return "SkipScanFilter "+ slots.toString() ;
    }
}
