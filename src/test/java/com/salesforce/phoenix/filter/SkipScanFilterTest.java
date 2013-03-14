package com.salesforce.phoenix.filter;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.salesforce.phoenix.query.KeyRange;

//reset()
//filterAllRemaining() -> true indicates scan is over, false, keep going on.
//filterRowKey(byte[],int,int) -> true to drop this row, if false, we will also call
//filterKeyValue(KeyValue) -> true to drop this key/value
//filterRow(List) -> allows directmodification of the final list to be submitted
//filterRow() -> last chance to drop entire row based on the sequence of filterValue() calls. Eg: filter a row if it doesn't contain a specified column.
@RunWith(Parameterized.class)
public class SkipScanFilterTest extends TestCase {
    private final SkipScanFilter skipper = new SkipScanFilter();
    private final Expectation expectation;

    public SkipScanFilterTest(List<List<KeyRange>> cnf, int varlenbitmask, Expectation expectation) {
        this.expectation = expectation;
        BitSet varlen = new BitSet();
        for (int i=0; i<32; i++) {
            if (0 != ((1<<i)&varlenbitmask)) varlen.set(i);
        }
        skipper.setCnf(cnf, varlen);
    }

    @Test
    public void test() {
        expectation.examine(skipper);
    }

    @Parameters(name="{0} {1} {2}")
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("def"), true)
                }},
                0x00,
                new SeekNext("aba", "abc"),
                new SeekNext("abb", "abc"),
                new Include("abc"),
                new Include("abe"),
                new Include("def"),
                new Finished("deg")));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("abc"), false, Bytes.toBytes("def"), true)
                }},
                0x00,
                new SeekNext("aba", "abd"),
                new SeekNext("abb", "abd"),
                new SeekNext("abc", "abd"),
                new Include("abe"),
                new Include("def"),
                new Finished("deg")));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("abc"), false, Bytes.toBytes("def"), false)
                }},
                0x00,
                new SeekNext("abb", "abd"),
                new SeekNext("aba", "abd"),
                new SeekNext("abc", "abd"),
                new Include("abe"),
                new Finished("def"))
        );
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("def"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("dzy"), false, Bytes.toBytes("xyz"), false),
                }},
                0x00,
                new Include("def"),
                new SeekNext("deg", "dzz"),
                new SeekNext("dyy", "dzz"),
                new SeekNext("dzy", "dzz"),
                new Include("eee"),
                new Finished("xyz"))
        );
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("abc"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("def"), true, Bytes.toBytes("def"), true),
                },
                {
                    KeyRange.getKeyRange(Bytes.toBytes("AB"), true, Bytes.toBytes("AX"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("EA"), false, Bytes.toBytes("EZ"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("PO"), true, Bytes.toBytes("PP"), false),
                }},
                0x00,
                new Include("abcAB"),
                new Include("abcEF"),
                new Include("defPO"),
                new SeekNext("abcPP","defAB"),
                new SeekNext("defEZ","defPO"),
                new SeekNext("abcAY","abcEB"),
                new Finished("defPP"),
                new Finished("degAB"),
                new Finished("defZZ"))
        );
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("AB"), true, Bytes.toBytes("AX"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("EA"), false, Bytes.toBytes("EZ"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("PO"), true, Bytes.toBytes("PP"), false),
                },
                {
                    KeyRange.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("abc"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("def"), true, Bytes.toBytes("def"), true),
                }},
                0x00,
                new Include("ABabc"),
                new Include("EFabc"),
                new Include("POdef"),
                new SeekNext("POabd","POdef"),
                new Finished("PPabc"),
                new SeekNext("EZdef","POabc"),
                new SeekNext("AYabc","EBabc"),
                new Finished("PPdef"),
                new SeekNext("ABdeg","ACabc"),
                new Finished("ZZdef"))
        );
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("PO"), true, Bytes.toBytes("PP"), false),
                },
                {
                    KeyRange.getKeyRange(Bytes.toBytes("def"), true, Bytes.toBytes("def"), true),
                }},
                0x00,
                new Finished("PPdef"),
                new Finished("POdeg"),
                new Include("POdef"))
        );
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("PO"), true, Bytes.toBytes("PO"), true),
                },
                {
                    KeyRange.getKeyRange(Bytes.toBytes("def"), true, Bytes.toBytes("def"), true),
                }},
                0x00,
                new Finished("PPdef"),
                new Finished("POdeg"),
                new Include("POdef"))
        );
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("def"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("dzy"), false, Bytes.toBytes("xyz"), false),
                },
                {
                    KeyRange.getKeyRange(Bytes.toBytes("AA"), true, Bytes.toBytes("AB"), false),
                },
                {
                    KeyRange.getKeyRange(Bytes.toBytes("AA"), true, Bytes.toBytes("AB"), false),
                },
                {
                    KeyRange.getKeyRange(Bytes.toBytes("AA"), true, Bytes.toBytes("AB"), false),
                },
                {
                    KeyRange.getKeyRange(Bytes.toBytes("AA"), true, Bytes.toBytes("AB"), false),
                }},
                0x00,
                new SeekNext("abcABABABAB", "abdAAAAAAAA"),
                new SeekNext("defABABABAB", "dzzAAAAAAAA"),
                new SeekNext("defAAABABAB", "dzzAAAAAAAA"),
                new SeekNext("defABABAAAB", "dzzAAAAAAAA"),
                new Finished("xyyABABABAB"))
        );
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("def"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("dzy"), false, Bytes.toBytes("xyz"), false),
                },
                {
                    KeyRange.getKeyRange(Bytes.toBytes("AB"), true, Bytes.toBytes("AX"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("EA"), false, Bytes.toBytes("EZ"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("PO"), true, Bytes.toBytes("PP"), false),
                }},
                0x00,
                new Include("defAB"),
                new Include("defAC"),
                new Include("defAW"),
                new Include("defAX"),
                new Include("defEB"),
                new Include("defPO"),
                new SeekNext("degAB", "dzzAB"),
                new SeekNext("dyyPO", "dzzAB"),
                new SeekNext("aaaAA", "abcAB"),
                new SeekNext("aaaZZ", "abcAB"),
                new SeekNext("aaaPP", "abcAB"),
                new SeekNext("defZZ", "dzzAB"),
                new SeekNext("defPP", "dzzAB"),
                new SeekNext("abcZZ", "abdAB"),
                new SeekNext("abcPP", "abdAB"),
                new SeekNext("abdZZ", "abeAB"),
                new SeekNext("abdPP", "abeAB"),
                new SeekNext("dzyZZ", "dzzAB"),
                new Include("dzzAX"),
                new SeekNext("dzzEZ", "dzzPO"),
                new Include("dzzEY"),
                new SeekNext("dffAA", "dzzAB"),
                new SeekNext("deaPP","debAB"),
                new SeekNext(new byte[]{'d','e','a',(byte)0xFF,(byte)0xFF}, new byte[]{'d','e','b','A','B'}),
                new SeekNext("dzzAA", "dzzAB"),
                new Include("eeeAB"),
                new Include("eeeAC"),
                new Include("eeeEF"),
                new SeekNext("dxxEA", "dzzAB"),
                new SeekNext("eeeEA", "eeeEB"),
                new SeekNext("eeeEZ","eeePO"),
                new Finished("xyzAA"),
                new Finished("xyzAC"),
                new Finished("zzzAA"))
        );
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("def"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("dzz"), true, Bytes.toBytes("xyz"), false),
                }},
                0x00,
                new SeekNext("abb", "abc"),
                new SeekNext("aba", "abc"),
                new Include("abc"),
                new Include("abe"),
                new Finished("xyz"),
                new Finished("zzz"))
        );
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("def"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("dzy"), false, Bytes.toBytes("xyz"), false),
                },
                {
                    KeyRange.getKeyRange(Bytes.toBytes("AB"), true, Bytes.toBytes("AX"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("EA"), false, Bytes.toBytes("EZ"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("PO"), true, Bytes.toBytes("PP"), false),
                },
                {
                    KeyRange.getKeyRange(Bytes.toBytes("100"), true, Bytes.toBytes("250"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("700"), false, Bytes.toBytes("901"), false),
                }},
                0x00,
                new Include("abcEB701"),
                new SeekNext("abcEB700", "abcEB701"),
                new SeekNext("abcEB250", "abcEB701"),
                new Finished("zzzAA000"))
        );
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("apple"), true, Bytes.toBytes("lemon"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("pear"), false, Bytes.toBytes("yam"), false),
                },
                {
                    KeyRange.getKeyRange(Bytes.toBytes("AB"), true, Bytes.toBytes("AX"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("EA"), false, Bytes.toBytes("EZ"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("PO"), true, Bytes.toBytes("PP"), false),
                },
                {
                    KeyRange.getKeyRange(Bytes.toBytes("100"), true, Bytes.toBytes("250"), false),
                    KeyRange.getKeyRange(Bytes.toBytes("700"), false, Bytes.toBytes("901"), false),
                }},
                0x01)
        );
        return testCases;
    }

    private static Collection<?> foreach(KeyRange[][] ranges, int varlenbitmask, Expectation... expectations) {
        List<List<KeyRange>> cnf = Lists.transform(Lists.newArrayList(ranges), ARRAY_TO_LIST);
        List<Object> ret = Lists.newArrayList();
        for (int i=0;i<expectations.length;i++) {
            ret.add(new Object[] { cnf, varlenbitmask, expectations[i] });
        }
        return ret;
    }

    private static final Function<KeyRange[], List<KeyRange>> ARRAY_TO_LIST = new Function<KeyRange[], List<KeyRange>>() {
        @Override public List<KeyRange> apply(KeyRange[] input) {
            return Lists.newArrayList(input);
        }
    };

    static interface Expectation {
        void examine(SkipScanFilter skipper);
    }
    private static final class SeekNext implements Expectation {
        private final byte[] rowkey, hint;
        public SeekNext(String rowkey, String hint) {
            this.rowkey = Bytes.toBytes(rowkey);
            this.hint = Bytes.toBytes(hint);
        }
        public SeekNext(byte[] rowkey, byte[] hint) {
            this.rowkey = rowkey;
            this.hint = hint;
        }

        @Override public void examine(SkipScanFilter skipper) {
            KeyValue kv = KeyValue.createFirstOnRow(rowkey);
            skipper.reset();
            assertFalse(skipper.filterAllRemaining());
            assertFalse(skipper.filterRowKey(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength()));

            assertEquals(ReturnCode.SEEK_NEXT_USING_HINT, skipper.filterKeyValue(kv));
            assertEquals(KeyValue.createFirstOnRow(hint), skipper.getNextKeyHint(kv));
        }

        @Override public String toString() {
            return "rowkey=" + Bytes.toStringBinary(rowkey)+", expected seek next using hint: " + Bytes.toStringBinary(hint);
        }
    }
    private static final class Include implements Expectation {
        private final byte[] rowkey;
        public Include(String rowkey) {
            this.rowkey = Bytes.toBytes(rowkey);
        }

        @Override public void examine(SkipScanFilter skipper) {
            KeyValue kv = KeyValue.createFirstOnRow(rowkey);
            skipper.reset();
            assertFalse(skipper.filterAllRemaining());
            assertFalse(skipper.filterRowKey(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength()));

            assertEquals(ReturnCode.INCLUDE, skipper.filterKeyValue(kv));
            assertNull(skipper.getNextKeyHint(kv));
        }

        @Override public String toString() {
            return "rowkey=" + Bytes.toStringBinary(rowkey)+", expected include";
        }
    }

    private static final class Finished implements Expectation {
        private final byte[] rowkey;
        public Finished(String rowkey) {
            this.rowkey = Bytes.toBytes(rowkey);
        }

        @Override public void examine(SkipScanFilter skipper) {
            KeyValue kv = KeyValue.createFirstOnRow(rowkey);
            skipper.reset();
            assertFalse(skipper.filterAllRemaining());
            assertTrue(skipper.filterRowKey(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength()));
            skipper.reset();
            assertTrue(skipper.filterAllRemaining());
        }

        @Override public String toString() {
            return "rowkey=" + Bytes.toStringBinary(rowkey)+", expected finished";
        }
    }
}
