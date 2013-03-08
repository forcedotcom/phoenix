package com.salesforce.phoenix.query;

import java.util.Arrays;
import java.util.Collection;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import static com.salesforce.phoenix.query.KeyRange.*;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

@RunWith(Parameterized.class)
public class KeyRangeIntersectTests extends TestCase {
    private final KeyRange a, b, intersection;

    public KeyRangeIntersectTests(KeyRange a, KeyRange b, KeyRange intersection) {
        this.a = a;
        this.b = b;
        this.intersection = intersection;
    }

    @Parameters(name="intersection of {0} and {1} is {2}")
    public static Collection<?> data() {
        return Arrays.asList(new Object[][] {
                {
                    getKeyRange(toBytes("C"), true, toBytes("E"), true),
                    getKeyRange(toBytes("D"), true, toBytes("F"), true),
                    getKeyRange(toBytes("D"), true, toBytes("E"), true)
                },
                {
                    getKeyRange(toBytes("C"), true, toBytes("E"), true),
                    getKeyRange(toBytes("D"), false, toBytes("F"), true),
                    getKeyRange(toBytes("D"), false, toBytes("E"), true)
                },
                {
                    getKeyRange(toBytes("C"), true, toBytes("E"), false),
                    getKeyRange(toBytes("D"), false, toBytes("F"), true),
                    getKeyRange(toBytes("D"), false, toBytes("E"), false)
                },
                {
                    getKeyRange(toBytes("C"), true, toBytes("E"), false),
                    getKeyRange(toBytes("E"), false, toBytes("F"), true),
                    EMPTY_RANGE
                },
                {
                    EVERYTHING_RANGE,
                    getKeyRange(toBytes("E"), false, toBytes("F"), true),
                    getKeyRange(toBytes("E"), false, toBytes("F"), true),
                },
                {
                    EVERYTHING_RANGE,
                    EVERYTHING_RANGE,
                    EVERYTHING_RANGE,
                },
                {
                    EMPTY_RANGE,
                    EVERYTHING_RANGE,
                    EMPTY_RANGE
                },
                {
                    EMPTY_RANGE,
                    getKeyRange(toBytes("E"), false, toBytes("F"), true),
                    EMPTY_RANGE
                },
        });
    }
    @Test
    public void intersect() {
        assertEquals(intersection, a.intersect(b));
        assertEquals(intersection, b.intersect(a));
    }
}
