package com.salesforce.phoenix.filter;

import java.util.Arrays;
import java.util.List;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;


//reset()
//filterAllRemaining() -> true indicates scan is over, false, keep going on.
//filterRowKey(byte[],int,int) -> true to drop this row, if false, we will also call
//filterKeyValue(KeyValue) -> true to drop this key/value
//filterRow(List) -> allows directmodification of the final list to be submitted
//filterRow() -> last chance to drop entire row based on the sequence of filterValue() calls. Eg: filter a row if it doesn't contain a specified column.        
public class SkipScanFilterTest extends TestCase {
    SkipScanFilter skipper = new SkipScanFilter();
    
    public void testSkip() {
        List<List<byte[]>> cnf =
            Arrays.asList(
                Arrays.asList(
                    Bytes.toBytes("abc"), Bytes.toBytes("def")
                )
            );
                
        Filter filter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("abd")));
        skipper.setFilter(filter).setCnf(cnf);
        
        {
            KeyValue kv = KeyValue.createFirstOnRow(Bytes.toBytes("abb"));
            skipper.reset();
            assertFalse(skipper.filterAllRemaining());
            assertFalse(skipper.filterRowKey(kv.getBuffer(), kv.getOffset(), kv.getLength()));
            assertEquals(ReturnCode.SEEK_NEXT_USING_HINT, skipper.filterKeyValue(kv));
            assertEquals(KeyValue.createFirstOnRow(Bytes.toBytes("abc")), skipper.getNextKeyHint(kv));
        }        
        
        {
            KeyValue kv = KeyValue.createFirstOnRow(Bytes.toBytes("ab"));
            skipper.reset();
            assertFalse(skipper.filterAllRemaining());
            assertFalse(skipper.filterRowKey(kv.getBuffer(), kv.getOffset(), kv.getLength()));
            assertEquals(ReturnCode.SEEK_NEXT_USING_HINT, skipper.filterKeyValue(kv));
            assertEquals(KeyValue.createFirstOnRow(Bytes.toBytes("abc")), skipper.getNextKeyHint(kv));
        }
        
        {
            KeyValue kv = KeyValue.createFirstOnRow(Bytes.toBytes("abc"));
            skipper.reset();
            assertFalse(skipper.filterAllRemaining());
            assertFalse(skipper.filterRowKey(kv.getBuffer(), kv.getOffset(), kv.getLength()));
            assertEquals(ReturnCode.NEXT_ROW, skipper.filterKeyValue(kv));
        }
        
        {
            KeyValue kv = KeyValue.createFirstOnRow(Bytes.toBytes("abd"));
            skipper.reset();
            assertFalse(skipper.filterAllRemaining());
            assertFalse(skipper.filterRowKey(kv.getBuffer(), kv.getOffset(), kv.getLength()));
            assertEquals(ReturnCode.INCLUDE, skipper.filterKeyValue(kv));
        }

        {
            KeyValue kv = KeyValue.createFirstOnRow(Bytes.toBytes("abe"));
            skipper.reset();
            assertFalse(skipper.filterAllRemaining());
            assertFalse(skipper.filterRowKey(kv.getBuffer(), kv.getOffset(), kv.getLength()));
            assertEquals(ReturnCode.NEXT_ROW, skipper.filterKeyValue(kv));
        }

        {
            KeyValue kv = KeyValue.createFirstOnRow(Bytes.toBytes("def"));
            skipper.reset();
            assertFalse(skipper.filterAllRemaining());
            assertFalse(skipper.filterRowKey(kv.getBuffer(), kv.getOffset(), kv.getLength()));
            assertEquals(ReturnCode.NEXT_ROW, skipper.filterKeyValue(kv));
        }
        
        
        {
            KeyValue kv = KeyValue.createFirstOnRow(Bytes.toBytes("deg"));
            skipper.reset();
            assertFalse(skipper.filterAllRemaining());
            assertFalse(skipper.filterRowKey(kv.getBuffer(), kv.getOffset(), kv.getLength()));
            assertEquals(ReturnCode.NEXT_ROW, skipper.filterKeyValue(kv));
        }
}
}
