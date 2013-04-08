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
package com.salesforce.phoenix.query;

import static com.salesforce.phoenix.query.QueryConstants.SEPARATOR_BYTE_ARRAY;

import java.util.*;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.annotation.Immutable;

import com.google.common.base.Function;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.salesforce.phoenix.util.ByteUtil;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 *
 * Class that represents an upper/lower bound key range.
 *
 * @author jtaylor
 * @since 0.1
 */
@Immutable
public class KeyRange {
    public enum Bound { LOWER, UPPER };
    private static final byte[] DEGENERATE_KEY = new byte[] {1};
    public static final byte[] UNBOUND_LOWER = HConstants.EMPTY_START_ROW;
    public static final byte[] UNBOUND_UPPER = HConstants.EMPTY_END_ROW;
    public static final KeyRange EMPTY_RANGE = new KeyRange(DEGENERATE_KEY, false, DEGENERATE_KEY, false);
    public static final KeyRange EVERYTHING_RANGE = new KeyRange(UNBOUND_LOWER, false, UNBOUND_UPPER, false);
    public static final Function<byte[], KeyRange> POINT = new Function<byte[], KeyRange>() {
      @Override public KeyRange apply(byte[] input) {
        return new KeyRange(input, true, input, true);
      }
    };
    public static final Comparator<KeyRange> COMPARATOR = new Comparator<KeyRange>() {
        @Override public int compare(KeyRange o1, KeyRange o2) {
            return ComparisonChain.start()
                    .compare(o1.lowerUnbound(), o2.lowerUnbound())
                    .compare(o1.getLowerRange(), o2.getLowerRange(), Bytes.BYTES_COMPARATOR)
                    // we want o1 lower inclusive to come before o2 lower inclusive, but
                    // false comes before true, so we have to negate
                    .compare(!o1.isLowerInclusive(), !o2.isLowerInclusive())
                    // for the same lower bounding, we want a finite upper bound to
                    // be ordered before an infinite upper bound
                    .compare(!o1.upperUnbound(), !o2.upperUnbound())
                    .compare(o1.getUpperRange(), o2.getUpperRange(), Bytes.BYTES_COMPARATOR)
                    .compare(o1.isUpperInclusive(), o2.isUpperInclusive())
                    .result();
        }
    };

    private final byte[] lowerRange;
    private final boolean lowerInclusive;
    private final byte[] upperRange;
    private final boolean upperInclusive;
    private final boolean isSingleKey;

    // Make sure to pass in constants for unbound upper/lower, since an emtpy array means null otherwise
    public static KeyRange getKeyRange(HRegionInfo region) {
        return KeyRange.getKeyRange(region.getStartKey().length == 0 ? UNBOUND_LOWER : region.getStartKey(), true,
                region.getEndKey().length == 0 ? UNBOUND_UPPER : region.getEndKey(), false, false);
    }

    public static KeyRange getKeyRange(byte[] lowerRange, boolean lowerInclusive,
            byte[] upperRange, boolean upperInclusive, boolean isFixedWidth) {
        if (lowerRange == null || upperRange == null) {
            return EMPTY_RANGE;
        }
        // Equality comparison, since null comes through as an empty byte array
        final boolean unboundLower = lowerRange == UNBOUND_LOWER;
        final boolean unboundUpper = upperRange == UNBOUND_UPPER;

        if (unboundLower && unboundUpper) {
            return EVERYTHING_RANGE;
        }
        /*
         * Force lower bound to be inclusive for fixed width keys because it makes
         * comparisons less expensive when you can count on one bound or the other
         * being inclusive. Comparing two fixed width exclusive bounds against each
         * other is inherently more expensive, because you need to take into account
         * if the bigger key is equal to the next key after the smaller key. For
         * example:
         *   (A-B] compared against [A-B)
         * An exclusive lower bound A is bigger than an exclusive upper bound B.
         * By forcing a fixed width exclusive lower bound key to be inclusive
         * prevents us from having to do this extra logic in the compare function.
         */
        if (!unboundLower && !lowerInclusive && isFixedWidth) {
            lowerRange = ByteUtil.nextKey(lowerRange);
            lowerInclusive = true;
        }
        if (!unboundLower && !unboundUpper) {
            int cmp = Bytes.compareTo(lowerRange, upperRange);
            if (cmp > 0 || (cmp == 0 && !(lowerInclusive && upperInclusive))) {
                return EMPTY_RANGE;
            }
        }
        return new KeyRange(lowerRange, unboundLower ? false : lowerInclusive,
                upperRange, unboundUpper ? false : upperInclusive);
    }

    private KeyRange(byte[] lowerRange, boolean lowerInclusive, byte[] upperRange, boolean upperInclusive) {
        this.lowerRange = lowerRange;
        this.lowerInclusive = lowerInclusive;
        this.upperRange = upperRange;
        this.upperInclusive = upperInclusive;
        this.isSingleKey = lowerRange != UNBOUND_LOWER && upperRange != UNBOUND_UPPER
                && lowerInclusive && upperInclusive && Bytes.compareTo(lowerRange, upperRange) == 0;
    }

    public byte[] getRange(Bound bound) {
        return bound == Bound.LOWER ? getLowerRange() : getUpperRange();
    }
    
    public boolean isInclusive(Bound bound) {
        return bound == Bound.LOWER ? isLowerInclusive() : isUpperInclusive();
    }
    
    public boolean isUnbound(Bound bound) {
        return bound == Bound.LOWER ? lowerUnbound() : upperUnbound();
    }
    
    public boolean isSingleKey() {
        return isSingleKey;
    }
    
    public int compareLowerToUpperBound(ImmutableBytesWritable ptr, boolean isInclusive) {
        return compareLowerToUpperBound(ptr.get(), ptr.getOffset(), ptr.getLength(), isInclusive);
    }
    
    public int compareLowerToUpperBound(ImmutableBytesWritable ptr) {
        return compareLowerToUpperBound(ptr, true);
    }
    
    public int compareUpperToLowerBound(ImmutableBytesWritable ptr, boolean isInclusive) {
        return compareUpperToLowerBound(ptr.get(), ptr.getOffset(), ptr.getLength(), isInclusive);
    }
    
    public int compareUpperToLowerBound(ImmutableBytesWritable ptr) {
        return compareUpperToLowerBound(ptr, true);
    }
    
    public int compareLowerToUpperBound( byte[] b, int o, int l) {
        return compareLowerToUpperBound(b,o,l,true);
    }

    /**
     * Compares a lower bound against an upper bound
     * @param b upper bound byte array
     * @param o upper bound offset
     * @param l upper bound length
     * @param isInclusive upper bound inclusive
     * @return -1 if the lower bound is less than the upper bound,
     *          1 if the lower bound is greater than the upper bound,
     *          and 0 if they are equal.
     */
    public int compareLowerToUpperBound( byte[] b, int o, int l, boolean isInclusive) {
        if (lowerUnbound()) {
            return -1;
        }
        int cmp = Bytes.compareTo(lowerRange, 0, lowerRange.length, b, o, l);
        if (cmp > 0) {
            return 1;
        }
        if (cmp < 0) {
            return -1;
        }
        if (lowerInclusive && isInclusive) {
            return 0;
        }
        return 1;
    }
    
    public int compareUpperToLowerBound(byte[] b, int o, int l) {
        return compareUpperToLowerBound(b,o,l, true);
    }
    
    public int compareUpperToLowerBound(byte[] b, int o, int l, boolean isInclusive) {
        if (upperUnbound()) {
            return 1;
        }
        int cmp = Bytes.compareTo(upperRange, 0, upperRange.length, b, o, l);
        if (cmp > 0) {
            return 1;
        }
        if (cmp < 0) {
            return -1;
        }
        if (upperInclusive && isInclusive) {
            return 0;
        }
        return -1;
    }
    
    public boolean isInRange(byte[] b, int o, int l) {
        if (!lowerUnbound()) {
            int cmp = Bytes.compareTo(lowerRange, 0, lowerRange.length, b, o, l);
            if (cmp > 0 || cmp == 0 && !lowerInclusive) {
                return false;
            }
        }
        if (!upperUnbound()) {
            int cmp = Bytes.compareTo(upperRange, 0, upperRange.length, b, o, l);
            if (cmp < 0 || cmp == 0 && !upperInclusive) {
                return false;
            }
        }
        return true;
    }
    
    public byte[] getLowerRange() {
        return lowerRange;
    }

    public boolean isLowerInclusive() {
        return lowerInclusive;
    }

    public byte[] getUpperRange() {
        return upperRange;
    }

    public boolean isUpperInclusive() {
        return upperInclusive;
    }

    public boolean isUnbound() {
        return lowerUnbound() || upperUnbound();
    }

    public boolean upperUnbound() {
        return upperRange == UNBOUND_UPPER;
    }

    public boolean lowerUnbound() {
        return lowerRange == UNBOUND_LOWER;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(lowerRange);
        if (lowerRange != null)
            result = prime * result + (lowerInclusive ? 1231 : 1237);
        result = prime * result + Arrays.hashCode(upperRange);
        if (upperRange != null)
            result = prime * result + (upperInclusive ? 1231 : 1237);
        return result;
    }

    @Override
    public String toString() {
        if (isSingleKey()) {
            return Bytes.toStringBinary(lowerRange);
        }
        return (lowerInclusive ? "[" : 
            "(") + (lowerUnbound() ? "*" : 
                Bytes.toStringBinary(lowerRange)) + " - " + (upperUnbound() ? "*" : 
                    Bytes.toStringBinary(upperRange)) + (upperInclusive ? "]" : ")" );
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof KeyRange)) {
            return false;
        }
        KeyRange that = (KeyRange)o;
        return Bytes.compareTo(this.lowerRange,that.lowerRange) == 0 && this.lowerInclusive == that.lowerInclusive &&
               Bytes.compareTo(this.upperRange, that.upperRange) == 0 && this.upperInclusive == that.upperInclusive;
    }

    public KeyRange intersect(KeyRange range) {
        byte[] newLowerRange;
        byte[] newUpperRange;
        boolean newLowerInclusive;
        boolean newUpperInclusive;
        if (lowerUnbound()) {
            newLowerRange = range.lowerRange;
            newLowerInclusive = range.lowerInclusive;
        } else if (range.lowerUnbound()) {
            newLowerRange = lowerRange;
            newLowerInclusive = lowerInclusive;
        } else {
            int cmp = Bytes.compareTo(lowerRange, range.lowerRange);
            if (cmp != 0 || lowerInclusive == range.lowerInclusive) {
                if (cmp <= 0) {
                    newLowerRange = range.lowerRange;
                    newLowerInclusive = range.lowerInclusive;
                } else {
                    newLowerRange = lowerRange;
                    newLowerInclusive = lowerInclusive;
                }
            } else { // Same lower range, but one is not inclusive
                newLowerRange = range.lowerRange;
                newLowerInclusive = false;
            }
        }
        if (upperUnbound()) {
            newUpperRange = range.upperRange;
            newUpperInclusive = range.upperInclusive;
        } else if (range.upperUnbound()) {
            newUpperRange = upperRange;
            newUpperInclusive = upperInclusive;
        } else {
            int cmp = Bytes.compareTo(upperRange, range.upperRange);
            if (cmp != 0 || upperInclusive == range.upperInclusive) {
                if (cmp >= 0) {
                    newUpperRange = range.upperRange;
                    newUpperInclusive = range.upperInclusive;
                } else {
                    newUpperRange = upperRange;
                    newUpperInclusive = upperInclusive;
                }
            } else { // Same upper range, but one is not inclusive
                newUpperRange = range.upperRange;
                newUpperInclusive = false;
            }
        }
        if (newLowerRange == lowerRange && newLowerInclusive == lowerInclusive
                && newUpperRange == upperRange && newUpperInclusive == upperInclusive) {
            return this;
        }
        return getKeyRange(newLowerRange, newLowerInclusive, newUpperRange, newUpperInclusive, false);
    }

    public static boolean isDegenerate(byte[] lowerRange, byte[] upperRange) {
        return lowerRange == KeyRange.EMPTY_RANGE.getLowerRange() && upperRange == KeyRange.EMPTY_RANGE.getUpperRange();
    }

    public KeyRange appendSeparator() {
        byte[] lowerBound = getLowerRange();
        byte[] upperBound = getUpperRange();
        if (lowerBound != UNBOUND_LOWER) {
            lowerBound = ByteUtil.concat(lowerBound, SEPARATOR_BYTE_ARRAY);
        }
        if (upperBound != UNBOUND_UPPER) {
            upperBound = ByteUtil.concat(upperBound, SEPARATOR_BYTE_ARRAY);
        }
        return getKeyRange(lowerBound, lowerInclusive, upperBound, upperInclusive, false);
    }

    /**
     * @return list of at least size 1
     */
    @NonNull
    public static List<KeyRange> coalesce(List<KeyRange> keyRanges) {
        List<KeyRange> tmp = new ArrayList<KeyRange>();
        for (KeyRange keyRange : keyRanges) {
            if (EMPTY_RANGE == keyRange) {
                continue;
            }
            if (EVERYTHING_RANGE == keyRange) {
                tmp.clear();
                tmp.add(keyRange);
                break;
            }
            tmp.add(keyRange);
        }
        if (tmp.size() == 1) {
            return tmp;
        }
        if (tmp.size() == 0) {
            return Collections.singletonList(EMPTY_RANGE);
        }

        Collections.sort(tmp, COMPARATOR);
        List<KeyRange> tmp2 = new ArrayList<KeyRange>();
        KeyRange range = tmp.get(0);
        for (int i=1; i<tmp.size(); i++) {
            KeyRange otherRange = tmp.get(i);
            KeyRange intersect = range.intersect(otherRange);
            if (EMPTY_RANGE == intersect) {
                tmp2.add(range);
                range = otherRange;
            } else {
                range = range.union(otherRange);
            }
        }
        tmp2.add(range);
        List<KeyRange> tmp3 = new ArrayList<KeyRange>();
        range = tmp2.get(0);
        for (int i=1; i<tmp2.size(); i++) {
            KeyRange otherRange = tmp2.get(i);
            assert !range.upperUnbound();
            assert !otherRange.lowerUnbound();
            if (range.isUpperInclusive() != otherRange.isLowerInclusive()
                    && Bytes.equals(range.getUpperRange(), otherRange.getLowerRange())) {
                range = KeyRange.getKeyRange(range.getLowerRange(), range.isLowerInclusive(), otherRange.getUpperRange(), otherRange.isUpperInclusive(), false);
            } else {
                tmp3.add(range);
                range = otherRange;
            }
        }
        tmp3.add(range);
        
        return tmp3;
    }

    public KeyRange union(KeyRange other) {
        if (EMPTY_RANGE == other) return this;
        if (EMPTY_RANGE == this) return other;
        byte[] newLower, newUpper;
        boolean newLowerInclusive, newUpperInclusive;
        if (this.lowerUnbound() || other.lowerUnbound()) {
            newLower = UNBOUND_LOWER;
            newLowerInclusive = false;
        } else {
            int lowerCmp = Bytes.compareTo(this.lowerRange, other.lowerRange);
            if (lowerCmp < 0) {
                newLower = lowerRange;
                newLowerInclusive = lowerInclusive;
            } else if (lowerCmp == 0) {
                newLower = lowerRange;
                newLowerInclusive = this.lowerInclusive || other.lowerInclusive;
            } else {
                newLower = other.lowerRange;
                newLowerInclusive = other.lowerInclusive;
            }
        }

        if (this.upperUnbound() || other.upperUnbound()) {
            newUpper = UNBOUND_UPPER;
            newUpperInclusive = false;
        } else {
            int upperCmp = Bytes.compareTo(this.upperRange, other.upperRange);
            if (upperCmp > 0) {
                newUpper = upperRange;
                newUpperInclusive = this.upperInclusive;
            } else if (upperCmp == 0) {
                newUpper = upperRange;
                newUpperInclusive = this.upperInclusive || other.upperInclusive;
            } else {
                newUpper = other.upperRange;
                newUpperInclusive = other.upperInclusive;
            }
        }
        return KeyRange.getKeyRange(newLower, newLowerInclusive, newUpper, newUpperInclusive, false);
    }

    public static List<KeyRange> of(List<byte[]> keys) {
        return Lists.transform(keys, POINT);
    }

    public static List<KeyRange> intersect(List<KeyRange> keyRanges, List<KeyRange> keyRanges2) {
        List<KeyRange> tmp = new ArrayList<KeyRange>();
        for (KeyRange r1 : keyRanges) {
            for (KeyRange r2 : keyRanges2) {
                KeyRange r = r1.intersect(r2);
                if (EMPTY_RANGE != r) {
                    tmp.add(r);
                }
            }
        }
        if (tmp.size() == 0) {
            return Collections.singletonList(KeyRange.EMPTY_RANGE);
        }
        Collections.sort(tmp, KeyRange.COMPARATOR);
        List<KeyRange> tmp2 = new ArrayList<KeyRange>();
        KeyRange r = tmp.get(0);
        for (int i=1; i<tmp.size(); i++) {
            if (EMPTY_RANGE == r.intersect(tmp.get(i))) {
                tmp2.add(r);
                r = tmp.get(i);
            } else {
                r = r.intersect(tmp.get(i));
            }
        }
        tmp2.add(r);
        return tmp2;
    }
    
    /**
     * Fill both upper and lower range of keyRange to keyLength bytes.
     * If the upper bound is inclusive, it must be filled such that an
     * intersection with a longer key would still match if the shorter
     * length matches.  For example: (*,00C] intersected with [00Caaa,00Caaa]
     * should still return [00Caaa,00Caaa] since the 00C matches and is
     * inclusive.
     * @param keyLength
     * @return the newly filled KeyRange
     */
    public KeyRange fill(int keyLength) {
        byte[] lowerRange = this.getLowerRange();
        byte[] newLowerRange = lowerRange;
        if (!this.lowerUnbound()) {
            // If lower range is inclusive, fill with 0x00 since conceptually these bytes are included in the range
            newLowerRange = ByteUtil.fillKey(lowerRange, keyLength);
        }
        byte[] upperRange = this.getUpperRange();
        byte[] newUpperRange = upperRange;
        if (!this.upperUnbound()) {
            // If upper range is inclusive, fill with 0xFF since conceptually these bytes are included in the range
            newUpperRange = ByteUtil.fillKey(upperRange, keyLength);
        }
        if (newLowerRange != lowerRange || newUpperRange != upperRange) {
            return KeyRange.getKeyRange(newLowerRange, this.isLowerInclusive(), newUpperRange, this.isUpperInclusive(), false);
        }
        return this;
    }

}