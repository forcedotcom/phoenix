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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.salesforce.phoenix.query.KeyRange;

public class SkipScanFilter extends FilterBase {
    private static final byte[] FIN = {};
    private static final byte[] NEXT_ROW = {};
    private List<List<KeyRange>> cnf;
    private byte[] hint = null;

    public SkipScanFilter() {
    }

    /**
     * @param varlen if one of the keyslots is variable length
     */
    public SkipScanFilter setCnf(List<List<KeyRange>> cnf, BitSet varlen) {
        Preconditions.checkArgument(!cnf.isEmpty());
        Preconditions.checkState(this.cnf == null);
        Preconditions.checkNotNull(cnf);
        int[] endstate = new int[cnf.size()];
        for (int i=0; i<cnf.size(); i++) {
            List<KeyRange> disjunction = cnf.get(i);
            Preconditions.checkArgument(!disjunction.isEmpty());
            endstate[i] = cnf.get(i).size();
            KeyRange prev = null;
            for (KeyRange range : disjunction) {
                Preconditions.checkArgument(!range.upperUnbound());
                Preconditions.checkArgument(!range.lowerUnbound());
                Preconditions.checkArgument(KeyRange.EMPTY_RANGE != range);
                Preconditions.checkArgument(KeyRange.EVERYTHING_RANGE != range);
                if (prev != null) {
                    Preconditions.checkArgument(KeyRange.COMPARATOR.compare(prev, range) < 0);
                    prev = range;
                }
            }
        }
        int numposts = cnf.get(0).size();
        for (int i=1;i<cnf.size();i++) {
            numposts *= endstate[i];
        }
        List<KeyRange> actuals = Lists.newArrayList();
        int[] counters = new int[cnf.size()];
        for (int x=0; x<numposts; x++) {
            for (int i=0; i<counters.length; i++) {
                if (counters[i] < endstate[i] - 1) {
                    counters[i]++;
                    break;
                } else {
                    counters[i] = 0;
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
        int mylen = 0;
        for (int i=0; i<cnf.size(); i++) {
            mylen += Math.max(cnf.get(i).get(0).getLowerRange().length, cnf.get(i).get(0).getUpperRange().length);
        }
        byte[] hint = new byte[mylen];
        int hintoffset = 0;
        outer: for (int i=0; i<cnf.size(); i++) {
            List<KeyRange> ors = cnf.get(i);
            int keyPartLen = 0;
            for (int j=0; j<ors.size(); j++) {
                KeyRange range = ors.get(j);

                while (hintoffset + keyPartLen < length 
                        && data[offset + hintoffset + keyPartLen] != 0
                        && keyPartLen < Math.max(range.getLowerRange().length, range.getUpperRange().length)) {
                    keyPartLen++;
                }
                
                Intersection intersection = intersect(range, data, offset + hintoffset, keyPartLen);
                byte[] lower = range.getLowerRange();
                switch (intersection) {
                case BEFORE_LOWER:
                    System.arraycopy(lower, 0, hint, hintoffset, lower.length);
                    hintoffset += lower.length;
                    while (++i < cnf.size()) {
                        KeyRange first = cnf.get(i).get(0);
                        System.arraycopy(first.getLowerRange(), 0, hint, hintoffset, first.getLowerRange().length);
                        hintoffset += first.getLowerRange().length;
                    }
                    this.hint = hint;
                    break outer;
                case AT_LOWER:
                    if (!range.isLowerInclusive()) {
                        this.hint = NEXT_ROW;
                        break;
                    } else {
                        System.arraycopy(data, offset + hintoffset, hint, hintoffset, lower.length);
                        hintoffset += lower.length;
                        continue outer;
                    }
                case BETWEEN_LOWER_AND_UPPER:
                    System.arraycopy(data, offset + hintoffset, hint, hintoffset, lower.length);
                    hintoffset += lower.length;
                    continue outer;
                case AT_UPPER:
                    if (!range.isUpperInclusive()) {
                        this.hint = FIN;
                        break outer;
                    } else {
                        System.arraycopy(data, offset + hintoffset, hint, hintoffset, lower.length);
                        hintoffset += lower.length;
                        continue outer;
                    }
                case ABOVE_UPPER:
                    this.hint = FIN;
                    break outer;
                default:
                    assert false;
                }
            }
        }
        if (this.hint == FIN) {
            return true;
        } else {
            return false;
        }
    }
    private static enum Intersection {
        BEFORE_LOWER,
        AT_LOWER,
        BETWEEN_LOWER_AND_UPPER,
        AT_UPPER,
        ABOVE_UPPER
    }
    private static Intersection intersect(KeyRange range, byte[] data, int offset, int length) {
        byte[] lower = range.getLowerRange();
        int cmpLower = Bytes.compareTo(data, offset, length, lower, 0, lower.length);
        if (cmpLower < 0) {
            return Intersection.BEFORE_LOWER;
        } else if (cmpLower == 0) {
            return Intersection.AT_LOWER;
        }
        byte[] upper = range.getUpperRange();
        int cmpUpper = Bytes.compareTo(data, offset, length, upper, 0, upper.length);
        if (cmpUpper < 0) {
            return Intersection.BETWEEN_LOWER_AND_UPPER;
        } else if (cmpUpper == 0) {
            return Intersection.AT_UPPER;
        } else {
            return Intersection.ABOVE_UPPER;
        }
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
        if (this.hint == NEXT_ROW) {
            return ReturnCode.NEXT_ROW;
        }
        return ReturnCode.SEEK_NEXT_USING_HINT;
    }

    @Override public KeyValue getNextKeyHint(KeyValue currentKV) {
        Preconditions.checkState(hint != FIN && hint != NEXT_ROW);
        if (hint == null) {
            return null;
        }
        return KeyValue.createFirstOnRow(hint);
    }
    
    @Override public void readFields(DataInput in) throws IOException {
        int n = in.readInt();
        cnf = new ArrayList<List<KeyRange>>();
        for (int i=0; i<n; i++) {
            int orlen = in.readInt();
            List<KeyRange> orclause = new ArrayList<KeyRange>();
            cnf.add(orclause);
            for (int j=0; j<orlen; j++) {
                orclause.add(
                        KeyRange.getKeyRange(
                                WritableUtils.readCompressedByteArray(in), true, 
                                WritableUtils.readCompressedByteArray(in), true));
            }
        }
    }

    @Override public void write(DataOutput out) throws IOException {
        out.writeInt(cnf.size());
        for (List<KeyRange> orclause : cnf) {
            out.writeInt(orclause.size());
            for (KeyRange arr : orclause) {
                WritableUtils.writeCompressedByteArray(out, arr.getLowerRange());
                WritableUtils.writeCompressedByteArray(out, arr.getUpperRange());
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
        return Objects.deepEquals(cnf, other.cnf);
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(cnf.toString());
        return sb.toString();
    }
}
