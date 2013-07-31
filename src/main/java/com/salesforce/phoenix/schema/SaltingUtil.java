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
package com.salesforce.phoenix.schema;

import java.util.*;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Lists;
import com.salesforce.phoenix.compile.ScanRanges;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.query.KeyRange.Bound;
import com.salesforce.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import com.salesforce.phoenix.util.ScanUtil;


/**
 * Utility methods related to transparent salting of row keys.
 */
public class SaltingUtil {
    public static RowKeySchema VAR_BINARY_SCHEMA = new RowKeySchemaBuilder().setMinNullable(1).addField(new PDatum() {

        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public PDataType getDataType() {
            return PDataType.VARBINARY;
        }

        @Override
        public Integer getByteSize() {
            return null;
        }

        @Override
        public Integer getMaxLength() {
            return null;
        }

        @Override
        public Integer getScale() {
            return null;
        }

        @Override
        public ColumnModifier getColumnModifier() {
            return null;
        }
        
    }).build();

    public static final int NUM_SALTING_BYTES = 1;
    public static final Integer MAX_BUCKET_NUM = 256; // Unsigned byte.
    public static final String SALTING_COLUMN_NAME = "_SALT";
    public static final String SALTED_ROW_KEY_NAME = "_SALTED_KEY";
    public static final PColumnImpl SALTING_COLUMN = new PColumnImpl(
            new PNameImpl(SALTING_COLUMN_NAME), null, PDataType.BINARY, 1, 0, false, 0, null);

    public static List<KeyRange> generateAllSaltingRanges(int bucketNum) {
        List<KeyRange> allRanges = Lists.<KeyRange>newArrayListWithExpectedSize(bucketNum);
        for (int i=0; i<bucketNum; i++) {
            byte[] saltByte = new byte[] {(byte) i};
            allRanges.add(SALTING_COLUMN.getDataType().getKeyRange(
                    saltByte, true, saltByte, true));
        }
        return allRanges;
    }

    public static byte[][] getSalteByteSplitPoints(int saltBucketNum) {
        byte[][] splits = new byte[saltBucketNum-1][];
        for (int i = 1; i < saltBucketNum; i++) {
            splits[i-1] = new byte[] {(byte) i};
        }
        return splits;
    }

    // Compute the hash of the key value stored in key and set its first byte as the value. The
    // first byte of key should be left empty as a place holder for the salting byte.
    public static byte[] getSaltedKey(ImmutableBytesWritable key, int bucketNum) {
        byte[] keyBytes = new byte[key.getLength()];
        byte saltByte = getSaltingByte(key.get(), key.getOffset() + 1, key.getLength() - 1, bucketNum);
        keyBytes[0] = saltByte;
        System.arraycopy(key.get(), key.getOffset() + 1, keyBytes, 1, key.getLength() - 1);
        return keyBytes;
    }

    // Generate the bucket byte given a byte array and the number of buckets.
    public static byte getSaltingByte(byte[] value, int offset, int length, int bucketNum) {
        int hash = hashCode(value, offset, length);
        byte bucketByte = (byte) ((Math.abs(hash) % bucketNum));
        return bucketByte;
    }

    private static int hashCode(byte a[], int offset, int length) {
        if (a == null)
            return 0;
        int result = 1;
        for (int i = offset; i < offset + length; i++) {
            result = 31 * result + a[i];
        }
        return result;
    }

    public static List<List<KeyRange>> flattenRanges(List<List<KeyRange>> ranges, RowKeySchema schema, int bucketNum) {
        if (ranges == null || ranges.isEmpty()) {
            return ScanRanges.NOTHING.getRanges();
        }
        int count = 1;
        // Skip salt byte range in the first position
        for (int i = 1; i < ranges.size(); i++) {
            count *= ranges.get(i).size();
        }
        KeyRange[] expandedRanges = new KeyRange[count];
        int[] position = new int[ranges.size()];
        int estimatedKeyLength = ScanUtil.estimateMaximumKeyLength(schema, 1, ranges);
        int idx = 0, length;
        byte saltByte;
        byte[] key = new byte[estimatedKeyLength];
        do {
            length = ScanUtil.setKey(schema, ranges, position, Bound.LOWER, key, 1, 0, ranges.size(), 1);
            saltByte = SaltingUtil.getSaltingByte(key, 1, length, bucketNum);
            key[0] = saltByte;
            byte[] saltedKey = Arrays.copyOf(key, length + 1);
            KeyRange range = PDataType.VARBINARY.getKeyRange(saltedKey, true, saltedKey, true);
            expandedRanges[idx++] = range;
        } while (incrementKey(ranges, position));
        // The comparator is imperfect, but sufficient for all single keys.
        Arrays.sort(expandedRanges, KeyRange.COMPARATOR);
        List<KeyRange> expandedRangesList = Arrays.asList(expandedRanges);
        return Collections.singletonList(expandedRangesList);
    }

    private static boolean incrementKey(List<List<KeyRange>> slots, int[] position) {
        int idx = slots.size() - 1;
        while (idx >= 0 && (position[idx] = (position[idx] + 1) % slots.get(idx).size()) == 0) {
            idx--;
        }
        return idx >= 0;
    }
}
