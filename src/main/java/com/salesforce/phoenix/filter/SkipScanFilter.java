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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.hash.*;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.query.KeyRange.Bound;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.schema.ValueSchema.Field;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.ScanUtil;


/**
 * 
 * Filter that seeks based on CNF containing anded and ored key ranges
 * 
 * TODO: figure out when to reset/not reset position array
 *
 * @author ryang, jtaylor
 * @since 0.1
 */
public class SkipScanFilter extends FilterBase {
    // Conjunctive normal form of or-ed ranges or point lookups
    private List<List<KeyRange>> slots;
    // schema of the row key
    private RowKeySchema schema;
    // current position for each slot
    private int[] position;
    // buffer used for skip hint
    private int maxKeyLength;
    private byte[] startKey;
    private int startKeyLength;
    private byte[] endKey; 
    private int endKeyLength;

    private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();

    /**
     * We know that initially the first row will be positioned at or 
     * after the first possible key.
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
        for (List<KeyRange> ranges : slots) {
            if (ranges.isEmpty()) {
                throw new IllegalStateException();
            }
        }
        this.slots = slots;
        this.schema = schema;
        this.maxKeyLength = maxKeyLength;
        this.position = new int[slots.size()];
        startKey = new byte[maxKeyLength];
        endKey = new byte[maxKeyLength];
        endKeyLength = 0;
    }

    // Exposed for testing.
    List<List<KeyRange>> getSlots() {
        return slots;
    }

    @Override
    public boolean filterAllRemaining() {
        return startKey == null && endKeyLength == 0;
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue kv) {
        return navigate(kv.getBuffer(), kv.getRowOffset(),kv.getRowLength());
    }

    @Override
    public KeyValue getNextKeyHint(KeyValue kv) {
        // TODO: don't allocate new key value every time here if possible
        return startKey == null ? null : new KeyValue(startKey, 0, startKeyLength,
                null, 0, 0, null, 0, 0, HConstants.LATEST_TIMESTAMP, Type.Maximum, null, 0, 0);
    }

    /**
     * Intersect the ranges of this filter with the ranges form by lowerInclusive and upperInclusive
     * key and filter out the ones that are not included in the region. Return the new slots.
     */
    public SkipScanFilter intersect(byte[] lowerInclusiveKey, byte[] upperExclusiveKey) {
        boolean lowerUnbound = (lowerInclusiveKey.length == 0);
        Arrays.fill(position, 0);
        int firstSlotStartPos = 0;
        int lastSlot = slots.size()-1;
        if (!lowerUnbound) {
            // Find the position of the first slot of the lower range
            ptr.set(lowerInclusiveKey);
            schema.first(ptr, 0, ValueBitSet.EMPTY_VALUE_BITSET);
            firstSlotStartPos = ScanUtil.searchClosestKeyRangeWithUpperHigherThanLowerPtr(slots.get(0), ptr, 0);
            // Lower range is past last upper range of first slot
            if (firstSlotStartPos >= slots.get(0).size()) {
                return null;            
            }
        }
        boolean upperUnbound = (upperExclusiveKey.length == 0);
        int firstSlotEndPos = slots.get(0).size()-1;
        if (!upperUnbound) {
            // Find the position of the first slot of the upper range
            ptr.set(upperExclusiveKey);
            schema.first(ptr, 0, ValueBitSet.EMPTY_VALUE_BITSET);
            firstSlotEndPos = ScanUtil.searchClosestKeyRangeWithUpperHigherThanLowerPtr(slots.get(0), ptr, firstSlotStartPos);
            // Upper range lower than first lower range of first slot
            if (firstSlotEndPos == 0 && Bytes.compareTo(upperExclusiveKey, slots.get(0).get(0).getLowerRange()) <= 0) {
                return null;            
            }
            // Past last position, so we can include everything from the start position
            if (firstSlotEndPos >= slots.get(0).size()) {
                upperUnbound = true;
                firstSlotEndPos = slots.get(0).size()-1;
            }
        }
        if (firstSlotEndPos > firstSlotStartPos || upperUnbound) {
            List<List<KeyRange>> newSlots = Lists.newArrayListWithCapacity(slots.size());
            newSlots.add(slots.get(0).subList(firstSlotStartPos, firstSlotEndPos+1));
            newSlots.addAll(slots.subList(1, slots.size()));
            return new SkipScanFilter(newSlots, schema);
        }
        if (!lowerUnbound) {
            position[0] = firstSlotStartPos;
            navigate(lowerInclusiveKey, 0, lowerInclusiveKey.length);
            if (filterAllRemaining()) {
                return null;            
            }
        }
        int[] startPosition = Arrays.copyOf(position, position.length);
        boolean excludeLastKey = false;
        position[0] = firstSlotEndPos;
        ReturnCode endCode = navigate(upperExclusiveKey, 0, upperExclusiveKey.length);
        // Exclude the last key if the upperExclusiveKey needs to be seeked (since it is then outside of the
        // range of that key) or is included and is a single key (since it's upper exclusive)
        excludeLastKey = (
                 endCode == ReturnCode.SEEK_NEXT_USING_HINT ||
                (endCode == ReturnCode.INCLUDE && slots.get(lastSlot).get(position[lastSlot]).isSingleKey()));
        if (filterAllRemaining()) { // Include everything up to the last position
            for (int i = 0; i < slots.size(); i++) {
                position[i] = slots.get(i).size()-1;
            }
        }
        List<List<KeyRange>> newSlots = Lists.newArrayListWithCapacity(slots.size());
        // Copy inclusive all positions 
        for (int i = 0; i < lastSlot; i++) {
            List<KeyRange> newRanges = slots.get(i).subList(startPosition[i], position[i] + 1);
            newSlots.add(newRanges);
            if (newRanges.size() > 1) {
                newSlots.addAll(slots.subList(i+1, slots.size()));
                return new SkipScanFilter(newSlots, schema);
            }
        }
        List<KeyRange> newRanges = slots.get(lastSlot).subList(startPosition[lastSlot], position[lastSlot] + (excludeLastKey ? 0 : 1));
        if (newRanges.isEmpty()) {
            return null;
        }
        newSlots.add(newRanges);
        return new SkipScanFilter(newSlots, schema);
    }

    private ReturnCode navigate(final byte[] currentKey, int offset, int length) {
        int nSlots = slots.size();
        // First check to see if we're in-range until we reach our end key
        if (endKeyLength > 0) {
            if (Bytes.compareTo(currentKey, offset, length, endKey, 0, endKeyLength) < 0) {
                return ReturnCode.INCLUDE;
            }

            // If key range of last slot is a single key, we can increment our position
            // since we know we'll be past the current row after including it.
            if (slots.get(nSlots-1).get(position[nSlots-1]).isSingleKey()) {
                if (incrementKey(nSlots-1) < 0) {
                    // Current row will be included, but we have no more
                    startKey = null;
                }
            }
            else {
                // Reset the positions to zero from the next slot after the earliest ranged slot, since the
                // next key could be bigger at this ranged slot, and smaller than the current position of
                // less significant slots.
                int earliestRangeIndex = nSlots-1;
                for (int i = 0; i < nSlots; i++) {
                    if (!slots.get(i).get(position[i]).isSingleKey()) {
                        earliestRangeIndex = i;
                        break;
                    }
                }
                Arrays.fill(position, earliestRangeIndex+1, position.length, 0);
            }
        }
        endKeyLength = 0;
        
        // We could have included the previous
        if (startKey == null) {
            return ReturnCode.NEXT_ROW;
        }

        int i = 0;
        boolean seek = false;
        int earliestRangeIndex = nSlots-1;
        ptr.set(currentKey, offset, length);
        schema.first(ptr, i, ValueBitSet.EMPTY_VALUE_BITSET);
        while (true) {
            // Increment to the next range while the upper bound of our current slot is less than our current key
            while (position[i] < slots.get(i).size() && slots.get(i).get(position[i]).compareUpperToLowerBound(ptr) < 0) {
                position[i]++;
            }
            Arrays.fill(position, i+1, position.length, 0);
            if (position[i] >= slots.get(i).size()) {
                // Our current key is bigger than the last range of the current slot.
                // Backtrack and increment the key of the previous slot values.
                if (i == 0) {
                    startKey = null;
                    return ReturnCode.NEXT_ROW;
                }
                // Increment key and backtrack until in range. We know at this point that we'll be
                // issuing a seek next hint.
                seek = true;
                Arrays.fill(position, i, position.length, 0);
                i--;
                // If we're positioned at a single key, no need to copy the current key and get the next key .
                // Instead, just increment to the next key and continue.
                boolean incremented = false;
                while (i >= 0 && slots.get(i).get(position[i]).isSingleKey() && (incremented=true) && (position[i] = (position[i] + 1) % slots.get(i).size()) == 0) {
                    i--;
                    incremented = false;
                }
                if (i < 0) {
                    startKey = null;
                    return ReturnCode.NEXT_ROW;
                }
                if (incremented) {
                    // Continue the loop after setting the start key, because our start key maybe smaller than
                    // the current key, so we'll end up incrementing the start key until it's bigger than the
                    // current key.
                    setStartKey();
                    ptr.set(ptr.get(), offset, length);
                    // Reinitialize iterator to be positioned at previous slot position
                    schema.setAccessor(ptr, i, ValueBitSet.EMPTY_VALUE_BITSET);
                } else {
                    // Copy the leading part of the actual current key into startKey which we'll use
                    // as our buffer through the rest of the loop.
                    startKeyLength = ptr.getOffset() - offset;
                    startKey = copyKey(startKey, startKeyLength + this.maxKeyLength, ptr.get(), offset, startKeyLength);
                    int nextKeyLength = startKeyLength;
                    appendToStartKey(i+1, startKeyLength);
                    // From here on, we use startKey as our buffer with offset reset to 0
                    // We've copied the part of the current key above that we need into startKey
                    offset = 0;
                    length = startKeyLength;
                    ptr.set(startKey, offset, startKeyLength);
                    // Reinitialize iterator to be positioned at previous slot position
                    // TODO: a schema.previous would potentially be more efficient
                    schema.setAccessor(ptr, i, ValueBitSet.EMPTY_VALUE_BITSET);
                    // Do nextKey after setting the accessor b/c otherwise the null byte may have
                    // been incremented causing us not to find it
                    ByteUtil.nextKey(startKey, nextKeyLength);
                }
            } else if (slots.get(i).get(position[i]).compareLowerToUpperBound(ptr) > 0) {
                // Our current key is less than the lower range of the current position in the current slot.
                // Seek to the lower range, since it's bigger than the current key
                int currentLength = ptr.getOffset() - offset;
                setStartKey(currentLength + this.maxKeyLength, ptr.get(), offset, currentLength);
                appendToStartKey(i, currentLength);
                // TODO: come up with test case where this is required or remove
                //Arrays.fill(position, earliestRangeIndex+1, position.length, 0);
                return ReturnCode.SEEK_NEXT_USING_HINT;
            } else { // We're in range, check the next slot
                if (!slots.get(i).get(position[i]).isSingleKey() && i < earliestRangeIndex) {
                    earliestRangeIndex = i;
                }
                i++;
                // If we're past the last slot or we know we're seeking to the next (in
                // which case the previously updated slot was verified to be within the
                // range, so we don't need to check the rest of the slots. If we were
                // to check the rest of the slots, we'd get into trouble because we may
                // have a null byte that was incremented which screws up our schema.next call)
                if (i >= nSlots || seek) {
                    break;
                }
                // If we run out of slots in our key, it means we have a partial key. In this
                // case, we seek to the next full key after this one.
                // TODO: test for this
                if (schema.next(ptr, i, offset + length, ValueBitSet.EMPTY_VALUE_BITSET) == null) {
                    int currentLength = ptr.getOffset() - offset;
                    setStartKey(currentLength + this.maxKeyLength, ptr.get(), offset, currentLength);
                    appendToStartKey(i, currentLength);
                    // TODO: come up with test case where this is required or remove
                    //Arrays.fill(position, earliestRangeIndex+1, position.length, 0);
                    return ReturnCode.SEEK_NEXT_USING_HINT;
                }
            }
        }
            
        if (seek) {
            return ReturnCode.SEEK_NEXT_USING_HINT;
        }
        // Else, we're in range for all slots and can include this row plus all rows 
        // up to the upper range of our last slot. We do this for ranges and single keys
        // since we potentially have multiple key values for the same row key.
        setEndKey(ptr.getOffset() - offset + this.maxKeyLength, ptr.get(), offset, ptr.getOffset() - offset);
        appendToEndKey(nSlots-1, ptr.getOffset() - offset);
        return ReturnCode.INCLUDE;
    }

    private int incrementKey(int i) {
        while (i >= 0 && slots.get(i).get(position[i]).isSingleKey() && (position[i] = (position[i] + 1) % slots.get(i).size()) == 0) {
            i--;
        }
        return i;
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

    private int setKey(Bound bound, byte[] key, int keyOffset, int slotStartIndex) {
        return setKey(bound, key, keyOffset, slotStartIndex, position.length);
    }

    private int setKey(Bound bound, byte[] key, int keyOffset, int slotStartIndex, int slotEndIndex) {
        return setKey(bound, position, key, keyOffset, slotStartIndex, slotEndIndex);
    }

    private int setKey(Bound bound, int[] position, byte[] key, int keyOffset, 
            int slotStartIndex, int slotEndIndex) {
        return ScanUtil.setKey(schema, slots, position, bound, key, keyOffset, slotStartIndex, slotEndIndex);
    }

    private void setStartKey() {
        startKeyLength = setKey(Bound.LOWER, startKey, 0, 0);
    }

    private void appendToStartKey(int slotIndex, int byteOffset) {
        startKeyLength += setKey(Bound.LOWER, startKey, byteOffset, slotIndex);
    }

    private void appendToEndKey(int slotIndex, int byteOffset) {
        endKeyLength += setKey(Bound.UPPER, endKey, byteOffset, slotIndex);
    }

    private int getTerminatorCount(RowKeySchema schema) {
        int nTerminators = 0;
        for (int i = 0; i < schema.getFieldCount(); i++) {
            Field field = schema.getField(i);
            // We won't have a terminator on the last PK column
            // unless it is variable length and exclusive, but
            // having the extra byte irregardless won't hurt anything
            if (!field.getType().isFixedWidth()) {
                nTerminators++;
            }
        }
        return nTerminators;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        RowKeySchema schema = new RowKeySchema();
        schema.readFields(in);
        int maxLength = getTerminatorCount(schema);
        int andLen = in.readInt();
        List<List<KeyRange>> slots = Lists.newArrayListWithExpectedSize(andLen);
        for (int i=0; i<andLen; i++) {
            int orlen = in.readInt();
            List<KeyRange> orclause = Lists.newArrayListWithExpectedSize(orlen);
            slots.add(orclause);
            int maxSlotLength = 0;
            for (int j=0; j<orlen; j++) {
                KeyRange range = new KeyRange();
                range.readFields(in);
                if (range.getLowerRange().length > maxSlotLength) {
                    maxSlotLength = range.getLowerRange().length;
                }
                if (range.getUpperRange().length > maxSlotLength) {
                    maxSlotLength = range.getUpperRange().length;
                }
                orclause.add(range);
            }
            maxLength += maxSlotLength;
        }
        this.init(slots, schema, maxLength);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        schema.write(out);
        out.writeInt(slots.size());
        for (List<KeyRange> orclause : slots) {
            out.writeInt(orclause.size());
            for (KeyRange range : orclause) {
                range.write(out);
            }
        }
    }

    @Override
    public int hashCode() {
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

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SkipScanFilter)) return false;
        SkipScanFilter other = (SkipScanFilter)obj;
        return Objects.equal(slots, other.slots) && Objects.equal(schema, other.schema);
    }

    @Override
    public String toString() {
        // TODO: make static util methods in ExplainTable that use type to print
        // key ranges
        return "SkipScanFilter "+ slots.toString() ;
    }
}
