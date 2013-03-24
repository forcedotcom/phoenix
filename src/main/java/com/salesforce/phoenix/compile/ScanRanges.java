package com.salesforce.phoenix.compile;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;

import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.schema.RowKeySchema;
import com.salesforce.phoenix.util.ScanUtil;

public class ScanRanges {
    private static final List<List<KeyRange>> EVERYTHING_RANGES = Collections.<List<KeyRange>>emptyList();
    private static final List<List<KeyRange>> NOTHING_RANGES = Collections.<List<KeyRange>>singletonList(Collections.<KeyRange>singletonList(KeyRange.EMPTY_RANGE));
    public static final ScanRanges EVERYTHING = new ScanRanges(EVERYTHING_RANGES,null);
    public static final ScanRanges NOTHING = new ScanRanges(NOTHING_RANGES,null);

    public static ScanRanges create(List<List<KeyRange>> ranges, RowKeySchema schema) {
        if (ranges.isEmpty()) {
            return EVERYTHING;
        } else if (ranges.size() == 1 && ranges.get(0).size() == 1 && ranges.get(0).get(0) == KeyRange.EMPTY_RANGE) {
            return NOTHING;
        }
        
        return new ScanRanges(ranges, schema);
    }
    
    private final List<List<KeyRange>> ranges;
    private final RowKeySchema schema;
    
    private ScanRanges (List<List<KeyRange>> ranges, RowKeySchema schema) {
        this.ranges = ranges;
        this.schema = schema;
    }

    public List<List<KeyRange>> getRanges() {
        return ranges;
    }

    public RowKeySchema getSchema() {
        return schema;
    }
    
    public boolean isEverything() {
        return this == EVERYTHING;
    }
    
    public boolean isDegenerate() {
        return this == NOTHING;
    }
    
    /**
     * Use SkipScanFilter under two circumstances:
     * 1) If we have multiple ranges for a given key slot (use of IN)
     * 2) If we have a range (i.e. not a single/point key) that is
     *    not the last key slot
     */
    public boolean useSkipScanFilter() {
        boolean hasRangeKey = false, useSkipScan = false;
        for (List<KeyRange> orRanges : ranges) {
            useSkipScan |= orRanges.size() > 1 | hasRangeKey;
            if (useSkipScan) {
                return true;
            }
            for (KeyRange range : orRanges) {
                hasRangeKey |= !range.isSingleKey();
            }
        }
        return false;
    }
    
    /**
     * @return true if this represents the full key to a single row
     */
    public boolean isSingleRowScan() {
        if (schema == null || ranges.size() < schema.getMaxFields()) {
            return false;
        }
        boolean isSingleKey = true;
        for (List<KeyRange> orRanges : ranges) {
            if (orRanges.size() > 1) {
                return false;
            }
            isSingleKey &= orRanges.get(0).isSingleKey();
        }
        return isSingleKey;
    }

    public void setScanStartStopRow(Scan scan) {
        if (this == EVERYTHING) {
            return;
        }
        if (this == NOTHING) {
            scan.setStartRow(KeyRange.EMPTY_RANGE.getLowerRange());
            scan.setStopRow(KeyRange.EMPTY_RANGE.getUpperRange());
            return;
        }
        
        byte[] expectedKey;
        expectedKey = ScanUtil.getMinKey(schema, ranges);
        if (expectedKey != null) {
            scan.setStartRow(expectedKey);
        }
        expectedKey = ScanUtil.getMaxKey(schema, ranges);
        if (expectedKey != null) {
            scan.setStopRow(expectedKey);
        }
    }
}
