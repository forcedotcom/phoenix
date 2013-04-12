package com.salesforce.phoenix.schema;

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Lists;
import com.google.common.hash.*;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.util.ByteUtil;


/**
 * Utility methods related to transparent salting of row keys.
 */
public class SaltingUtil {

    public static final List<KeyRange> ALL_SALTING_RANGES = generateAllSaltingRanges();
    private static final HashFunction MD5 = Hashing.md5();

    private static List<KeyRange> generateAllSaltingRanges() {
        List<KeyRange> allRanges = Lists.<KeyRange>newArrayListWithExpectedSize(Byte.MAX_VALUE);
        byte[] lowerBound = new byte[] {0};
        byte[] upperBound = new byte[] {1};
        for (int i=0; i<Byte.MAX_VALUE; i++) {
            allRanges.add(KeyRange.getKeyRange(lowerBound, upperBound));
            ByteUtil.nextKey(lowerBound, 1);
            ByteUtil.nextKey(upperBound, 1);
        }
        return allRanges;
    }

    public static boolean useSalting(int bucketNum) {
        return 0 < bucketNum && bucketNum <= Byte.MAX_VALUE;
    }

    public static byte[] getSaltedKey(ImmutableBytesWritable key, int bucketNum) {
        byte[] keyBytes = new byte[key.getSize() + 1];
        byte saltByte = getSaltingByte(key.get(), key.getOffset(), key.getSize(), bucketNum);
        System.arraycopy(saltByte, 0, keyBytes, 0, 1);
        System.arraycopy(key.get(), key.getOffset(), keyBytes, 1, key.getSize());
        return keyBytes;
    }

    // Generate the bucket byte given a byte and the number of buckets.
    public static byte getSaltingByte(byte[] value, int offset, int length, int bucketNum) {
        HashCode digest = MD5.hashBytes(value, offset, length);
        byte bucketByte = (byte) ((digest.asInt() % bucketNum) & 0x0f);
        return bucketByte;
    }

}
