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
import java.util.*;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Objects;
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
 * TODO: switch includeUntilKey to use endKey
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
    private byte[] startKey;
    private int startKeyLength;
    // buffer used for current end key after which we need to increment the position
    private byte[] endKey; 
    private int endKeyLength;
    private int maxKeyLength;

    // TODO: use endKey for this purpose instead since it's not being used anywhere 
    private byte[] includeUntilKey; 
    private int includeUntilKeyLength;

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
        includeUntilKey = new byte[maxKeyLength];
        // Start key for the scan will initially be set to start at the right place
        // We just need to set the end key for when we need to calculate the next skip hint
        // TODO: shouldn't be necessary, since the start key of the scan should be set to this
        // or a higher value
        setStartKey();
        setEndKey();
        if (endKeyLength > 0); // TODO: remove
    }

    @Override
    public boolean filterAllRemaining() {
        return startKey == null;
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

    private ReturnCode navigate(final byte[] currentKey, final int offset, final int length) {
        if (includeUntilKeyLength > 0 && Bytes.compareTo(currentKey, offset, length, includeUntilKey, 0, includeUntilKeyLength) < 0) {
            return ReturnCode.INCLUDE;
        }
        includeUntilKeyLength = 0;

        // TODO: optimize for isSingleKey cases, since we should be
        // able to skip the comparisons of these slots.
//        if (!updateEndKey) {
//            i = 1;
//            for (; i < nSlots; i++) {
//                // Stop one slot after the first range, since we
//                // know we're within the first range based on
//                // the scan start/stop key
//                if (!slots.get(i).get(position[i]).isSingleKey()) {
//                    i++;
//                    break;
//                }
//            }
//        }
        
        int i = 0;
        int nSlots = slots.size();
        ptr.set(currentKey, offset, length);
        schema.first(ptr, i, ValueBitSet.EMPTY_VALUE_BITSET);
        while (true) {
            while (position[i] < slots.get(i).size() && slots.get(i).get(position[i]).compareUpper(ptr) < 0) {
                position[i]++;
            }
            if (position[i] >= slots.get(i).size()) {
                while (true) {
                    if (i == 0) {
                        startKey = null;
                        return ReturnCode.NEXT_ROW;
                    } else { // Increment key and backtrack until in range
                        startKey = copyKey(startKey, ptr.getOffset() - offset + this.maxKeyLength, currentKey, offset, ptr.getOffset() - offset);
                        ByteUtil.nextKey(startKey, ptr.getOffset() - offset);
                        startKeyLength = ptr.getOffset() - offset;
                        Arrays.fill(position, i, position.length, 0);
                        i--;
                        if (slots.get(i).get(position[i]).compareUpper(startKey, 0, startKeyLength) < 0) {
                            // TODO: implement schema.previous to go backwards
                            ptr.set(currentKey, offset, length);
                            schema.setAccessor(ptr, i, ValueBitSet.EMPTY_VALUE_BITSET);
                            if (++position[i] >= slots.get(i).size()) {
                                continue;
                            }
                            setStartKey(ptr.getOffset() - offset + this.maxKeyLength, currentKey, offset, ptr.getOffset() - offset);
                        } else {
                            i++;
                        }
                        appendToStartKey(i, ptr.getOffset() - offset);
                        return ReturnCode.SEEK_NEXT_USING_HINT;
                    }
                }
            } else if (slots.get(i).get(position[i]).compareLower(ptr) > 0) {
                // Seek to the lower range, since it's bigger than the current key
                setStartKey(ptr.getOffset() - offset + this.maxKeyLength, currentKey, offset, ptr.getOffset() - offset);
                Arrays.fill(position, i+1, position.length, 0);
                appendToStartKey(i, ptr.getOffset() - offset); // FIXME: i+1 ?
                position[i] = 0;
                return ReturnCode.SEEK_NEXT_USING_HINT;
            } else {
                i++;
                if (i >= nSlots) {
                    break;
                }
                schema.next(ptr, i, offset + length, ValueBitSet.EMPTY_VALUE_BITSET);
            }
        }
            
        // Otherwise we're in range for all slots and can include this row plus all rows 
        // up to the upper range of our last slot
        includeUntilKey = copyKey(includeUntilKey, ptr.getOffset() - offset + this.maxKeyLength, currentKey, offset, ptr.getOffset() - offset);
        includeUntilKeyLength = ptr.getOffset() - offset;
        // TODO: review - don't want next key to automatically be done here
        includeUntilKeyLength += setKey(Bound.UPPER, includeUntilKey, ptr.getOffset() - offset, nSlots-1);
        // FIXME: what to set positions to here?
        Arrays.fill(position, 1, position.length, 0);
        return ReturnCode.INCLUDE;
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
    
//    private void setEndKey(int maxLength, byte[] sourceKey, int offset, int length) {
//        endKey = copyKey(endKey, maxLength, sourceKey, offset, length);
//        endKeyLength = length;
//    }
    
    private int setKey(Bound bound, byte[] key, int keyOffset, int slotStartIndex) {
        return setKey(bound, key, keyOffset, slotStartIndex, position.length);
    }

    private int setKey(Bound bound, byte[] key, int keyOffset, int slotStartIndex, int slotEndIndex) {
        return ScanUtil.setKey(schema, slots, position, bound, key, keyOffset, slotStartIndex, slotEndIndex);
    }

    private void setStartKey() {
        startKeyLength = setKey(Bound.LOWER, startKey, 0, 0);
    }

    private void appendToStartKey(int slotIndex, int byteOffset) {
        startKeyLength += setKey(Bound.LOWER, startKey, byteOffset, slotIndex);
    }

    private void setEndKey() {
        endKeyLength = setKey(Bound.UPPER, endKey, 0, 0);
//        endKeyLength = setKey(Bound.LOWER, endKey, 0, 0, position.length-1);
//        endKeyLength += setKey(Bound.UPPER, endKey, 0, position.length-1, position.length);
    }

//    private void appendToEndKey(int slotIndex, int byteOffset) {
//        endKeyLength += setKey(Bound.UPPER, endKey, byteOffset, slotIndex);
//    }

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
                boolean lowerUnbound = in.readBoolean();
                byte[] lower = KeyRange.UNBOUND_LOWER;
                if (!lowerUnbound) {
                    lower = WritableUtils.readCompressedByteArray(in);
                }
                if (lower.length > maxSlotLength) {
                    maxSlotLength = lower.length;
                }
                boolean lowerInclusive = in.readBoolean();
                boolean upperUnbound = in.readBoolean();
                byte[] upper = KeyRange.UNBOUND_UPPER;
                if (!upperUnbound) {
                    upper = WritableUtils.readCompressedByteArray(in);
                }
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
                boolean lowerUnbound = arr.lowerUnbound();
                out.writeBoolean(lowerUnbound);
                if (!lowerUnbound) {
                    WritableUtils.writeCompressedByteArray(out, arr.getLowerRange());
                }
                out.writeBoolean(arr.isLowerInclusive());
                boolean upperUnbound = arr.upperUnbound();
                out.writeBoolean(upperUnbound);
                if (!upperUnbound) {
                    WritableUtils.writeCompressedByteArray(out, arr.getUpperRange());
                }
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
