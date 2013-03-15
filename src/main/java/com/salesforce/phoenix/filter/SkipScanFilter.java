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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.TrustedByteArrayOutputStream;

public class SkipScanFilter extends FilterBase {
    private static final byte[] FIN = {};
    private List<List<KeyRange>> cnf;
    private int[] widths;
    private byte[] hint = null;

    public SkipScanFilter() {
    }
    private static final byte[] SEP = {0};
    /**
     * @param varlen if one of the keyslots is variable length
     */
    public SkipScanFilter setCnf(List<List<KeyRange>> cnf, int[] widths) {
        Preconditions.checkArgument(!cnf.isEmpty());
        Preconditions.checkNotNull(cnf);
        Preconditions.checkArgument(cnf.size() == widths.length, cnf.size()+" vs " + widths.length);
        this.widths = widths;
        for (int i=0; i<widths.length; i++) {
            if (-1 == widths[i]) {
                // TODO: handle variable width columns
                return null;
            }
            Preconditions.checkArgument(widths[i]==-1 || widths[i]>0);
        }
        for (int i=0; i<cnf.size(); i++) {
            List<KeyRange> disjunction = cnf.get(i);
            Preconditions.checkArgument(!disjunction.isEmpty());
            KeyRange prev = null;
            for (KeyRange range : disjunction) {
                Preconditions.checkArgument(KeyRange.EMPTY_RANGE != range);
                Preconditions.checkArgument(KeyRange.EVERYTHING_RANGE != range);

                // TODO: handle this
                if (range.upperUnbound()) return null;
                if (range.lowerUnbound()) return null;

                if (widths[i] != -1) {
                    Preconditions.checkArgument(widths[i] == range.getLowerRange().length, widths[i] +" " + range.getLowerRange().length + " " + i);
                    Preconditions.checkArgument(widths[i] == range.getUpperRange().length, widths[i] + " " + range.getUpperRange().length);
                }
                Preconditions.checkArgument(!range.upperUnbound());
                Preconditions.checkArgument(!range.lowerUnbound());
                if (prev != null) {
                    Preconditions.checkArgument(KeyRange.COMPARATOR.compare(prev, range) < 0);
                    prev = range;
                }
            }
        }
        this.cnf = cnf;
        return this;
    }

    @Override
    public void reset() {
        if (FIN != this.hint) {
            this.hint = null;
        }
    }

    @Override public boolean filterAllRemaining() {
        return FIN == this.hint;
    }

    @Override
    public boolean filterRowKey(final byte[] data, int offset, int length) {
        TrustedByteArrayOutputStream hint = new TrustedByteArrayOutputStream(1);
        boolean inside = true;
        boolean finished = true;
        int[] keyOffsets = new int[widths.length];
        int[] keyLengths = new int[widths.length];
        Arrays.fill(keyOffsets, -1);
        {
            int keyOffset = offset;
            for (int keySlot=0; keySlot<widths.length; keySlot++) {
                if (isVariableLength(keySlot)) {
                    // variable length
                    int keyLength = 0;
                    while (true) {
                        if (data[keyOffset + keyLength] == 0) {
                            keyLength -= 1;
                            break;
                        }
                        if (keyLength + keyOffset > offset + length) break;
                        keyLength++;
                    }
                    keyOffsets[keySlot] = keyOffset;
                    keyLengths[keySlot] = keyLength;
                    keyOffset += keyLength + 1;
                } else {
                    keyOffsets[keySlot] = keyOffset;
                    keyLengths[keySlot] = widths[keySlot];
                    keyOffset += keyLengths[keySlot];
                }
                if (keyLengths[keySlot] + keyOffsets[keySlot] > offset + length) {
                    throw new RuntimeException("went past the end of the rowkey: "+offset+" " + length+" " + Arrays.toString(keyLengths) + " " + Arrays.toString(keyOffsets));
                }
            }
        }

        for (int keySlot=0; keySlot<widths.length; keySlot++) {
            List<KeyRange> ranges = cnf.get(keySlot);
            KeyRange last = ranges.get(ranges.size() - 1);
            byte[] upper = last.getUpperRange();
            int cmpUpper = Bytes.compareTo(data, keyOffsets[keySlot], keyLengths[keySlot], upper, 0, upper.length);
            if (cmpUpper == 0 && !last.isUpperInclusive() || cmpUpper > 0) {
                if (keySlot == 0) {
                    this.hint = FIN;
                    return true;
                }

                // backtrack to find the last incrementable key
                int probe = keySlot - 1;
                while (probe >= 0) {
                    byte[] successor = successor(data, keyOffsets[probe], keyLengths[probe], probe);
                    if (successor == null) {
                        probe--;
                    } else {
                        // all slots prior to this one have headroom.
                        // we just need to increment the one immediately prior
                        for (int a=0; a<probe; a++) {
                            write(hint, data, keyOffsets[a], keyLengths[a], isVariableLength(a));
                        }
                        write(hint, successor, 0, successor.length, isVariableLength(probe));
                        for (int a=probe + 1; a<widths.length; a++) {
                            seekInto(hint, cnf.get(a).get(0), isVariableLength(a));
                        }
                        this.hint = hint.toByteArray();
                        return false;
                    }
                }
                this.hint = FIN;
                return true;
            }
        }

        nextSlot: for (int keySlot=0; keySlot<widths.length; keySlot++) {
            for (KeyRange range : cnf.get(keySlot)) {
                byte[] lower = range.getLowerRange();
                int cmpLower = Bytes.compareTo(data, keyOffsets[keySlot], keyLengths[keySlot], lower, 0, lower.length);
                if (cmpLower < 0) {
                    seekInto(hint, range, isVariableLength(keySlot));
                    for (int tailKey=keySlot + 1; tailKey<widths.length; tailKey++) {
                        seekInto(hint, cnf.get(tailKey).get(0), isVariableLength(tailKey));
                    }
                    this.hint = hint.toByteArray();
                    return false;
                } else if (cmpLower == 0) {
                    if (!range.isLowerInclusive()) {
                        seekInto(hint, range, isVariableLength(keySlot));
                        for (int tailKey=keySlot + 1; tailKey<widths.length; tailKey++) {
                            seekInto(hint, cnf.get(tailKey).get(0), isVariableLength(tailKey));
                        }
                        this.hint = hint.toByteArray();
                        return false;
                    } else {
                        hint.write(range.getLowerRange());
                        if (isVariableLength(keySlot)) {
                            hint.write(SEP);
                        }
                        finished = false;
                        continue nextSlot;
                    }
                }
                assert cmpLower > 0;
                byte[] upper = range.getUpperRange();
                int cmpUpper = Bytes.compareTo(data, keyOffsets[keySlot], keyLengths[keySlot], upper, 0, upper.length);
                if (cmpUpper < 0 || cmpUpper == 0 && range.isUpperInclusive()) {
                    // we're inside this range
                    hint.write(data, keyOffsets[keySlot], keyLengths[keySlot]);
                    finished = false;
                    continue nextSlot;
                }
                assert cmpUpper==0 && !range.isUpperInclusive() || cmpUpper>0;
                // probe next range within this slot
            }

            if (keySlot == 0) {
                this.hint = FIN;
                return true;
            }
            inside = false;
            // went past the uppermost range within this slot
            // ...x{[ap0,ap2],[na1,na3]}x{[176,178],[180,182]}x...
            // suppose we saw na2;183.  we should seek past value_after(na2)
            if (isVariableLength(keySlot)) {
                hint.write(0xFF); // not sure this seeks far enough
                hint.write(SEP);
            } else {
                for (int i=0; i<widths[keySlot]; i++) {
                    hint.write(0xFF);
                }
            }
        }
        if (inside) {
            return false;
        }
        if (finished) {
            this.hint = FIN;
            return true;
        }
        this.hint = hint.toByteArray();
        return false;
    }

    private static void write(TrustedByteArrayOutputStream hint, byte[] data, int keyOffset, int keyLength, boolean varlen) {
        hint.write(data, keyOffset, keyLength);
        if (varlen) {
            hint.write(SEP);
        }
    }

    /*
     * The pk col value must conform to the key slot (variable vs fixed width).
     * The value is expected not to fall beyond the disjunction for the slot.
     * In other words, the pk col value is not greater than the uppermost
     * range's upper boundary.
     *
     * @param data, keyOffset, keyLength -- pk col value
     * @param keySlot -- which pk col
     * @return null if there is no suitable successor given the slot's constraints
     */
    private byte[] successor(byte[] data, int keyOffset, int keyLength, int keySlot) {
        final byte[] ret;
        if (!isVariableLength(keySlot)) {
            ret = ByteUtil.nextKey(Arrays.copyOfRange(data, keyOffset, keyOffset + keyLength));
        } else {
            ret = new byte[keyLength + 1];
            System.arraycopy(data, keyOffset, ret, 0, keyLength);
            ret[keyLength] = (byte)0x01;
        }
        for (KeyRange range : cnf.get(keySlot)) {
            byte[] lower = range.getLowerRange();
            int cmpLower = Bytes.compareTo(ret, 0, ret.length, lower, 0, lower.length);
            if (cmpLower < 0) {
                return firstSeekable(range, isVariableLength(keySlot));
            }
            if (cmpLower == 0) {
                return ret;
            }
            byte[] upper = range.getUpperRange();
            int cmpUpper = Bytes.compareTo(ret, 0, ret.length, upper, 0, upper.length);
            if (cmpUpper < 0 || cmpUpper == 0 && range.isUpperInclusive()) {
                return ret;
            }
        }
        return null;
    }

    private static byte[] firstSeekable(KeyRange range, boolean varlen) {
        byte[] lower = range.getLowerRange();
        if (range.isLowerInclusive() && varlen) {
            return lower;
        } else if (range.isLowerInclusive() && !varlen) {
            return lower;
        } else if (!range.isLowerInclusive() && varlen) {
            byte[] ret = new byte[lower.length + 1];
            System.arraycopy(lower, 0, ret, 0, lower.length);
            ret[lower.length] = (byte)0x01;
            return ret;
        } else {
            assert !range.isLowerInclusive() && !varlen;
            return ByteUtil.nextKey(lower);
        }
    }

    private static void seekInto(TrustedByteArrayOutputStream hint, KeyRange range, boolean varlen) {
        byte[] seekable = firstSeekable(range, varlen);
        hint.write(seekable);
        if (varlen) {
            hint.write(SEP);
        }
    }

    private boolean isVariableLength(int keySlot) {
        return widths[keySlot] == -1;
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue v) {
        if (this.hint == null) {
            return ReturnCode.INCLUDE;
        }
        // we can skip!
        if (this.hint == FIN) {
            return ReturnCode.NEXT_ROW;
        }
        return ReturnCode.SEEK_NEXT_USING_HINT;
    }

    @Override public KeyValue getNextKeyHint(KeyValue currentKV) {
        Preconditions.checkState(hint != FIN);
        if (hint == null) {
            return null;
        }
        return KeyValue.createFirstOnRow(hint);
    }

    @Override public void readFields(DataInput in) throws IOException {
        int widthslen = in.readInt();
        widths = new int[widthslen];
        for (int i=0; i<widthslen; i++) {
            widths[i] = in.readInt();
        }
        int n = in.readInt();
        cnf = new ArrayList<List<KeyRange>>();
        for (int i=0; i<n; i++) {
            int orlen = in.readInt();
            List<KeyRange> orclause = new ArrayList<KeyRange>();
            cnf.add(orclause);
            for (int j=0; j<orlen; j++) {
                orclause.add(
                    KeyRange.getKeyRange(
                            WritableUtils.readCompressedByteArray(in), in.readBoolean(),
                            WritableUtils.readCompressedByteArray(in), in.readBoolean()));
            }
        }
    }

    @Override public void write(DataOutput out) throws IOException {
        out.writeInt(widths.length);
        for (int i=0; i<widths.length; i++) {
            out.writeInt(widths[i]);
        }
        out.writeInt(cnf.size());
        for (List<KeyRange> orclause : cnf) {
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
        h.putInt(cnf.size());
        for (int i=0; i<cnf.size(); i++) {
            h.putInt(cnf.get(i).size());
            for (int j=0; j<cnf.size(); j++) {
                h.putBytes(cnf.get(i).get(j).getLowerRange());
                h.putBytes(cnf.get(i).get(j).getUpperRange());
            }
        }
        return h.hash().asInt();
    }

    @Override public boolean equals(Object obj) {
        if (!(obj instanceof SkipScanFilter)) return false;
        SkipScanFilter other = (SkipScanFilter)obj;
        return Arrays.equals(widths, other.widths) && Objects.deepEquals(cnf, other.cnf);
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(cnf.toString());
        sb.append(Arrays.toString(widths));
        return sb.toString();
    }
}
