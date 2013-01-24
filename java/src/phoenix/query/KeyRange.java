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
package phoenix.query;

import static phoenix.query.QueryConstants.SEPARATOR_BYTE_ARRAY;

import java.util.Arrays;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.annotation.Immutable;

import phoenix.util.ByteUtil;

import com.google.common.base.Objects;

/**
 * 
 * Class that represents an upper/lower bound key range.
 *
 * @author jtaylor
 * @since 0.1
 */
@Immutable
public class KeyRange {
    private static final byte[] DEGENERATE_KEY = new byte[] {1};
    public static final byte[] UNBOUND_LOWER = HConstants.EMPTY_START_ROW;
    public static final byte[] UNBOUND_UPPER = HConstants.EMPTY_END_ROW;
    public static final KeyRange EMPTY_RANGE = new KeyRange(DEGENERATE_KEY, false, DEGENERATE_KEY, false);
    public static final KeyRange EVERYTHING_RANGE = new KeyRange(UNBOUND_LOWER, false, UNBOUND_UPPER, false);

    private final byte[] lowerRange;
    private final boolean lowerInclusive;
    private final byte[] upperRange;
    private final boolean upperInclusive;
    
    // Make sure to pass in constants for unbound upper/lower, since an emtpy array means null otherwise
    public static KeyRange getKeyRange(HRegionInfo region) {
        return KeyRange.getKeyRange(region.getStartKey().length == 0 ? UNBOUND_LOWER : region.getStartKey(), true, region.getEndKey().length == 0 ? UNBOUND_UPPER : region.getEndKey(), false);
    }
    
    public static KeyRange getKeyRange(byte[] lowerRange, boolean lowerInclusive, byte[] upperRange, boolean upperInclusive) {
        if (lowerRange == null || upperRange == null) {
            return EMPTY_RANGE;
        }
        // Equality comparison, since null comes through as an empty byte array
        final boolean unboundLower = lowerRange == UNBOUND_LOWER;
        final boolean unboundUpper = upperRange == UNBOUND_UPPER;

        if (unboundLower && unboundUpper) {
            return EVERYTHING_RANGE;
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
        return Objects.toStringHelper(this)
            .addValue(Bytes.toStringBinary(lowerRange))
            .addValue(lowerInclusive)
            .addValue(Bytes.toStringBinary(upperRange))
            .addValue(upperInclusive)
            .toString();
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
        if (newLowerRange == lowerRange && newLowerInclusive == lowerInclusive && newUpperRange == upperRange && newUpperInclusive == upperInclusive) {
            return this;
        }
        return getKeyRange(newLowerRange, newLowerInclusive, newUpperRange, newUpperInclusive);
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
        return getKeyRange(lowerBound, lowerInclusive, upperBound, upperInclusive);
    }
}