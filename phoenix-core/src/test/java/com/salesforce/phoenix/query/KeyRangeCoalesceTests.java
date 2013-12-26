package com.salesforce.phoenix.query;

import static com.salesforce.phoenix.query.KeyRange.EMPTY_RANGE;
import static com.salesforce.phoenix.query.KeyRange.EVERYTHING_RANGE;
import static java.util.Arrays.asList;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.util.*;

import junit.framework.TestCase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.salesforce.phoenix.schema.PDataType;

@RunWith(Parameterized.class)
public class KeyRangeCoalesceTests extends TestCase {
    private static final Random RANDOM = new Random(1);
    private final List<KeyRange> expected, input;

    public KeyRangeCoalesceTests(List<KeyRange> expected, List<KeyRange> input) {
        this.expected = expected;
        this.input = input;
    }

    @Parameters(name="{0} coalesces to {1}")
    public static Collection<?> data() {
        return Arrays.asList(new Object[][] {
                {expect(
                    EMPTY_RANGE
                ),
                input(
                )},
                {expect(
                        PDataType.CHAR.getKeyRange(toBytes("C"), true, toBytes("E"), true)
                ),
                input(
                        PDataType.CHAR.getKeyRange(toBytes("C"), true, toBytes("E"), true)
                )},
                {expect(
                        PDataType.CHAR.getKeyRange(toBytes("C"), true, toBytes("E"), true)
                ),
                input(
                        PDataType.CHAR.getKeyRange(toBytes("C"), true, toBytes("D"), true),
                        PDataType.CHAR.getKeyRange(toBytes("D"), true, toBytes("E"), true)
                )},
                {expect(
                        PDataType.CHAR.getKeyRange(toBytes("C"), true, toBytes("Z"), true)
                ),
                input(
                        PDataType.CHAR.getKeyRange(toBytes("C"), true, toBytes("D"), true),
                        PDataType.CHAR.getKeyRange(toBytes("D"), true, toBytes("E"), true),
                        PDataType.CHAR.getKeyRange(toBytes("D"), true, toBytes("Z"), true)
                )},
                {expect(
                        PDataType.CHAR.getKeyRange(toBytes("B"), true, toBytes("Z"), true)
                ),
                input(
                        PDataType.CHAR.getKeyRange(toBytes("C"), true, toBytes("D"), true),
                        PDataType.CHAR.getKeyRange(toBytes("B"), true, toBytes("E"), true),
                        PDataType.CHAR.getKeyRange(toBytes("D"), true, toBytes("Z"), true)
                )},
                {expect(
                        PDataType.CHAR.getKeyRange(toBytes("B"), true, toBytes("Z"), true)
                ),
                input(
                        PDataType.CHAR.getKeyRange(toBytes("C"), true, toBytes("D"), true),
                        PDataType.CHAR.getKeyRange(toBytes("B"), true, toBytes("Z"), false),
                        PDataType.CHAR.getKeyRange(toBytes("D"), true, toBytes("Z"), true)
                )},
                {expect(
                        PDataType.CHAR.getKeyRange(toBytes("A"), true, toBytes("A"), true),
                        PDataType.CHAR.getKeyRange(toBytes("B"), true, toBytes("Z"), false)
                ),
                input(
                        PDataType.CHAR.getKeyRange(toBytes("A"), true, toBytes("A"), true),
                        PDataType.CHAR.getKeyRange(toBytes("B"), true, toBytes("Z"), false)
                )},
                {expect(
                        PDataType.CHAR.getKeyRange(toBytes("A"), true, toBytes("B"), false),
                        PDataType.CHAR.getKeyRange(toBytes("B"), false, toBytes("Z"), false)
                ),
                input(
                        PDataType.CHAR.getKeyRange(toBytes("A"), true, toBytes("B"), false),
                        PDataType.CHAR.getKeyRange(toBytes("B"), false, toBytes("Z"), false)
                )},
                {expect(
                        PDataType.CHAR.getKeyRange(toBytes("A"), true, toBytes("Z"), false)
                ),
                input(
                        PDataType.CHAR.getKeyRange(toBytes("A"), true, toBytes("B"), false),
                        PDataType.CHAR.getKeyRange(toBytes("B"), true, toBytes("Z"), false)
                )},
                {expect(
                    EVERYTHING_RANGE
                ),
                input(
                    EVERYTHING_RANGE,
                    EVERYTHING_RANGE
                )},
                {expect(
                    EVERYTHING_RANGE
                ),
                input(
                    EVERYTHING_RANGE
                )},
                {expect(
                    EVERYTHING_RANGE
                ),
                input(
                    EMPTY_RANGE,
                    EVERYTHING_RANGE,
                    EVERYTHING_RANGE
                )},
                {expect(
                    EMPTY_RANGE
                ),
                input(
                    EMPTY_RANGE
                )}
        });
    }
    @Test
    public void coalesce() {
        assertEquals(expected, KeyRange.coalesce(input));
        List<KeyRange> tmp = new ArrayList<KeyRange>(input);
        Collections.reverse(tmp);
        assertEquals(expected, KeyRange.coalesce(input));
        Collections.shuffle(tmp, RANDOM);
        assertEquals(expected, KeyRange.coalesce(input));
    }
    
    private static final List<KeyRange> expect(KeyRange... kr) {
        return asList(kr);
    }
    
    private static final List<KeyRange> input(KeyRange... kr) {
        return asList(kr);
    }
}
