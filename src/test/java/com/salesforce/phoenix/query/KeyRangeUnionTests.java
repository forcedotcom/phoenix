package com.salesforce.phoenix.query;

import static com.salesforce.phoenix.query.KeyRange.EMPTY_RANGE;
import static com.salesforce.phoenix.query.KeyRange.EVERYTHING_RANGE;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.util.Arrays;
import java.util.Collection;

import junit.framework.TestCase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class KeyRangeUnionTests extends TestCase {
    private final KeyRange a, b, union;

    public KeyRangeUnionTests(KeyRange a, KeyRange b, KeyRange union) {
        this.a = a;
        this.b = b;
        this.union = union;
    }

    private static KeyRange getKeyRange(byte[] lowerRange, boolean lowerInclusive, byte[] upperRange, boolean upperInclusive) {
        return KeyRange.getKeyRange(lowerRange, lowerInclusive, upperRange, upperInclusive, true);
    }
    
    @Parameters(name="union of {0} and {1} is {2}")
    public static Collection<?> data() {
        return Arrays.asList(new Object[][] {
                {
                    getKeyRange(toBytes("C"), true, toBytes("E"), true),
                    getKeyRange(toBytes("D"), true, toBytes("F"), true),
                    getKeyRange(toBytes("C"), true, toBytes("F"), true)
                },
                {
                    getKeyRange(toBytes("C"), false, toBytes("E"), false),
                    getKeyRange(toBytes("D"), true, toBytes("F"), true),
                    getKeyRange(toBytes("C"), false, toBytes("F"), true)
                },
                {
                    getKeyRange(toBytes("C"), false, toBytes("E"), false),
                    getKeyRange(toBytes("D"), true, toBytes("E"), true),
                    getKeyRange(toBytes("C"), false, toBytes("E"), true)
                },
                {
                    getKeyRange(toBytes("C"), false, toBytes("E"), false),
                    getKeyRange(toBytes("C"), true, toBytes("E"), true),
                    getKeyRange(toBytes("C"), true, toBytes("E"), true)
                },
                {
                    getKeyRange(toBytes("C"), true, toBytes("E"), false),
                    EMPTY_RANGE,
                    getKeyRange(toBytes("C"), true, toBytes("E"), false),
                },
                {
                    EVERYTHING_RANGE,
                    getKeyRange(toBytes("E"), false, toBytes("F"), true),
                    EVERYTHING_RANGE,
                },
                {
                    EVERYTHING_RANGE,
                    EVERYTHING_RANGE,
                    EVERYTHING_RANGE,
                },
                {
                    EMPTY_RANGE,
                    EVERYTHING_RANGE,
                    EVERYTHING_RANGE,
                },
        });
    }
    @Test
    public void union() {
        assertEquals(union, a.union(b));
        assertEquals(union, b.union(a));
    }
}
