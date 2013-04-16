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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Lists;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.util.ByteUtil;


/**
 * Utility methods related to transparent salting of row keys.
 */
public class SaltingUtil {

    public static final String SALTING_COLUMN_NAME = "_SALTING_BYTE";
    public static final PColumnImpl SALTING_COLUMN = new PColumnImpl(
            new PNameImpl(SALTING_COLUMN_NAME), null, PDataType.CHAR, 1, 0, false, -1);

    public static List<KeyRange> generateAllSaltingRanges(int bucketNum) {
        List<KeyRange> allRanges = Lists.<KeyRange>newArrayListWithExpectedSize(bucketNum);
        byte[] lowerBound = new byte[] {0};
        byte[] upperBound = new byte[] {1};
        for (int i=0; i<bucketNum; i++) {
            allRanges.add(SALTING_COLUMN.getDataType().getKeyRange(
                    Arrays.copyOf(lowerBound, lowerBound.length), true,
                    Arrays.copyOf(upperBound, upperBound.length), false));
            ByteUtil.nextKey(lowerBound, 1);
            ByteUtil.nextKey(upperBound, 1);
        }
        return allRanges;
    }

    public static boolean isSaltingColumn(PColumn col) {
        return col.getName().getString().equals(SALTING_COLUMN_NAME);
    }

    public static byte[] getSaltedKey(ImmutableBytesWritable key, int bucketNum) {
        byte[] keyBytes = new byte[key.getSize()];
        byte saltByte = getSaltingByte(key.get(), key.getOffset() + 1, key.getSize() - 1, bucketNum);
        keyBytes[0] = saltByte;
        System.arraycopy(key.get(), key.getOffset() + 1, keyBytes, 1, key.getSize() - 1);
        return keyBytes;
    }

    // Generate the bucket byte given a byte and the number of buckets.
    private static byte getSaltingByte(byte[] value, int offset, int length, int bucketNum) {
        int hash = Arrays.hashCode(value);
        byte bucketByte = (byte) ((Math.abs(hash) % bucketNum));
        return bucketByte;
    }
}
