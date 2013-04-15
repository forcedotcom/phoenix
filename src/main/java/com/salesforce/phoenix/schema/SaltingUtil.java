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
