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

import com.salesforce.phoenix.schema.PDataType;

@RunWith(Parameterized.class)
public class KeyRangeUnionTests extends TestCase {
    private final KeyRange a, b, union;

    public KeyRangeUnionTests(KeyRange a, KeyRange b, KeyRange union) {
        this.a = a;
        this.b = b;
        this.union = union;
    }

    @Parameters(name="union of {0} and {1} is {2}")
    public static Collection<?> data() {
        return Arrays.asList(new Object[][] {
                {
                    PDataType.CHAR.getKeyRange(toBytes("C"), true, toBytes("E"), true),
                    PDataType.CHAR.getKeyRange(toBytes("D"), true, toBytes("F"), true),
                    PDataType.CHAR.getKeyRange(toBytes("C"), true, toBytes("F"), true)
                },
                {
                    PDataType.CHAR.getKeyRange(toBytes("C"), false, toBytes("E"), false),
                    PDataType.CHAR.getKeyRange(toBytes("D"), true, toBytes("F"), true),
                    PDataType.CHAR.getKeyRange(toBytes("C"), false, toBytes("F"), true)
                },
                {
                    PDataType.CHAR.getKeyRange(toBytes("C"), false, toBytes("E"), false),
                    PDataType.CHAR.getKeyRange(toBytes("D"), true, toBytes("E"), true),
                    PDataType.CHAR.getKeyRange(toBytes("C"), false, toBytes("E"), true)
                },
                {
                    PDataType.CHAR.getKeyRange(toBytes("C"), false, toBytes("E"), false),
                    PDataType.CHAR.getKeyRange(toBytes("C"), true, toBytes("E"), true),
                    PDataType.CHAR.getKeyRange(toBytes("C"), true, toBytes("E"), true)
                },
                {
                    PDataType.CHAR.getKeyRange(toBytes("C"), true, toBytes("E"), false),
                    EMPTY_RANGE,
                    PDataType.CHAR.getKeyRange(toBytes("C"), true, toBytes("E"), false),
                },
                {
                    EVERYTHING_RANGE,
                    PDataType.CHAR.getKeyRange(toBytes("E"), false, toBytes("F"), true),
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
