package com.salesforce.phoenix.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

public class SkipScanFilterTest extends TestCase {
    SkipScanFilter skipper = new SkipScanFilter();
    Filter inner = new FilterBaseExtension();
    
    public void testSkip() {
        List<List<byte[]>> cnf =
            Arrays.asList(
                Arrays.asList(
                        Bytes.toBytes("abc"), Bytes.toBytes("def")
                )
            );
                
        skipper.setFilter(new FilterBaseExtension()).setCnf(cnf);
        
        assertFalse(skipper.filterAllRemaining());
    }
    
    private final class FilterBaseExtension extends FilterBase {
        @Override public boolean filterRowKey(byte[] buffer, int offset, int length) {
            return super.filterRowKey(buffer, offset, length);
        }
        @Override public void readFields(DataInput in) throws IOException {
            throw new UnsupportedOperationException();
        }
        @Override public void write(DataOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

}
