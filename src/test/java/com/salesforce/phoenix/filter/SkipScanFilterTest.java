package com.salesforce.phoenix.filter;

import java.util.ArrayList;
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
import com.google.common.collect.Iterables;
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
    
    public SkipScanFilterTest(List<List<KeyRange>> cnf, Expectation expectation) {
        this.expectation = expectation;
        skipper.setCnf(cnf);
    }
    
    @Test
    public void test() {
        expectation.examine(skipper);
    }
    
    @Parameters(name="{0} {1}")
    public static Iterable<?> data() {
        return Iterables.concat(
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("def"), true)
                }},
                new SeekNext("abb", "abc"),
                new SeekNext("ab", "abc"),
                new Include("abc"),
                new Include("abe"),
                new Include("def"),
                new Finished("deg")),
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("abc"), false, Bytes.toBytes("def"), true)
                }},
                new SeekNext("abb", "abc"),
                new SeekNext("ab", "abc"),
                new NextRow("abc"),
                new Include("abe"),
                new Include("def"),
                new Finished("deg")),
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("abc"), false, Bytes.toBytes("def"), false)
                }},
                new SeekNext("abb", "abc"),
                new SeekNext("ab", "abc"),
                new NextRow("abc"),
                new Include("abe"),
                new Finished("def")),
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("def"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("dzz"), false, Bytes.toBytes("xyz"), false),
                }},
                new Include("def"),
                new SeekNext("deg", "dzz"),
                new SeekNext("dyy", "dzz"),
                new Include("dzz"),
                new Include("eee"),
                new Finished("xyz")),
                foreach(new KeyRange[][]{{
                    KeyRange.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("def"), true),
                    KeyRange.getKeyRange(Bytes.toBytes("dzz"), true, Bytes.toBytes("xyz"), false),
                }},
                new SeekNext("abb", "abc"),
                new SeekNext("ab", "abc"),
                new NextRow("abc"),
                new Include("abe"),
                new Finished("def"))
                );
    }
    
    private static Iterable<?> foreach(KeyRange[][] ranges, Expectation... expectations) {
        List<List<KeyRange>> cnf = Lists.transform(Lists.newArrayList(ranges), ARRAY_TO_LIST);
        List<Object> ret = Lists.newArrayList();
        for (int i=0;i<expectations.length;i++) {
            ret.add(new Object[] { cnf, expectations[i] });
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
        private final String rowkey, hint;
        public SeekNext(String rowkey, String hint) {
            this.rowkey = rowkey;
            this.hint = hint;
        }

        @Override public void examine(SkipScanFilter skipper) {
            KeyValue kv = KeyValue.createFirstOnRow(Bytes.toBytes(rowkey));
            skipper.reset();
            assertFalse(skipper.filterAllRemaining());
            assertFalse(skipper.filterRowKey(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength()));
            
            assertEquals(ReturnCode.SEEK_NEXT_USING_HINT, skipper.filterKeyValue(kv));
            assertEquals(KeyValue.createFirstOnRow(Bytes.toBytes(hint)), skipper.getNextKeyHint(kv));
        }
        
        @Override public String toString() {
            return "rowkey=" + rowkey+", expected seek next using hint: " + hint;
        }
    }
    private static final class Include implements Expectation {
        private final String rowkey;
        public Include(String rowkey) {
            this.rowkey = rowkey;
        }

        @Override public void examine(SkipScanFilter skipper) {
            KeyValue kv = KeyValue.createFirstOnRow(Bytes.toBytes(rowkey));
            skipper.reset();
            assertFalse(skipper.filterAllRemaining());
            assertFalse(skipper.filterRowKey(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength()));
            
            assertEquals(ReturnCode.INCLUDE, skipper.filterKeyValue(kv));
            assertNull(skipper.getNextKeyHint(kv));
        }
        
        @Override public String toString() {
            return "rowkey=" + rowkey+", expected include";
        }
    }
    
    private static final class Finished implements Expectation {
        private final String rowkey;
        public Finished(String rowkey) {
            this.rowkey = rowkey;
        }

        @Override public void examine(SkipScanFilter skipper) {
            KeyValue kv = KeyValue.createFirstOnRow(Bytes.toBytes(rowkey));
            skipper.reset();
            assertFalse(skipper.filterAllRemaining());
            assertTrue(skipper.filterRowKey(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength()));
            skipper.reset();
            assertTrue(skipper.filterAllRemaining());
        }
        
        @Override public String toString() {
            return "rowkey=" + rowkey+", expected include";
        }
    }
    
    private static final class NextRow implements Expectation {
        private final String rowkey;
        public NextRow(String rowkey) {
            this.rowkey = rowkey;
        }

        @Override public void examine(SkipScanFilter skipper) {
            KeyValue kv = KeyValue.createFirstOnRow(Bytes.toBytes(rowkey));
            skipper.reset();
            assertFalse(skipper.filterAllRemaining());
            assertFalse(skipper.filterRowKey(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength()));
            
            assertEquals(ReturnCode.NEXT_ROW, skipper.filterKeyValue(kv));
        }
        @Override public String toString() {
            return "rowkey=" + rowkey+", expected next row";
        }
    }
}
