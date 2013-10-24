/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *     Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *     Neither the name of Salesforce.com nor the names of its contributors may 
 *     be used to endorse or promote products derived from this software without 
 *     specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.phoenix.iterate;

import java.text.Format;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Iterators;
import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.ScanRanges;
import com.salesforce.phoenix.compile.StatementContext;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.query.KeyRange.Bound;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.RowKeySchema;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.StringUtil;


public abstract class ExplainTable {
    private static final List<KeyRange> EVERYTHING = Collections.singletonList(KeyRange.EVERYTHING_RANGE);
    protected final StatementContext context;
    protected final TableRef tableRef;
    protected final GroupBy groupBy;
   
    public ExplainTable(StatementContext context, TableRef table) {
        this(context,table,GroupBy.EMPTY_GROUP_BY);
    }

    public ExplainTable(StatementContext context, TableRef table, GroupBy groupBy) {
        this.context = context;
        this.tableRef = table;
        this.groupBy = groupBy;
    }

    private boolean explainSkipScan(StringBuilder buf) {
        ScanRanges scanRanges = context.getScanRanges();
        if (scanRanges.useSkipScanFilter()) {
            buf.append("SKIP SCAN ");
            int count = 1;
            boolean hasRanges = false;
            for (List<KeyRange> ranges : scanRanges.getRanges()) {
                count *= ranges.size();
                for (KeyRange range : ranges) {
                    hasRanges |= !range.isSingleKey();
                }
            }
            buf.append("ON ");
            buf.append(count);
            buf.append(hasRanges ? " RANGE" : " KEY");
            buf.append(count > 1 ? "S " : " ");
            return true;
        } else {
            buf.append("RANGE SCAN ");
        }
        return false;
    }
    
    protected void explain(String prefix, List<String> planSteps) {
        StringBuilder buf = new StringBuilder(prefix);
        ScanRanges scanRanges = context.getScanRanges();
        boolean hasSkipScanFilter = false;
        if (scanRanges.isEverything()) {
            buf.append("FULL SCAN ");
        } else {
            hasSkipScanFilter = explainSkipScan(buf);
        }
        buf.append("OVER " + tableRef.getTable().getName().getString());
        appendKeyRanges(buf);
        planSteps.add(buf.toString());
        
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        PageFilter pageFilter = null;
        if (filter != null) {
            int offset = 0;
            boolean hasFirstKeyOnlyFilter = false;
            String filterDesc = "";
            if (hasSkipScanFilter) {
                if (filter instanceof FilterList) {
                    List<Filter> filterList = ((FilterList) filter).getFilters();
                    if (filterList.get(0) instanceof FirstKeyOnlyFilter) {
                        hasFirstKeyOnlyFilter = true;
                        offset = 1;
                    }
                    if (filterList.size() > offset+1) {
                        filterDesc = filterList.get(offset+1).toString();
                        if (filterList.size() > offset+2) {
                            pageFilter = (PageFilter) filterList.get(offset+2);
                        }
                    }
                }
            } else if (filter instanceof FilterList) {
                List<Filter> filterList = ((FilterList) filter).getFilters();
                if (filterList.get(0) instanceof FirstKeyOnlyFilter) {
                    hasFirstKeyOnlyFilter = true;
                    offset = 1;
                }
                if (filterList.size() > offset) {
                    filterDesc = filterList.get(offset).toString();
                    if (filterList.size() > offset+1) {
                        pageFilter = (PageFilter) filterList.get(offset+1);
                    }
                }
            } else {
                if (filter instanceof FirstKeyOnlyFilter) {
                    hasFirstKeyOnlyFilter = true;
                } else {
                    filterDesc = filter.toString();
                }
            }
            if (filterDesc.length() > 0) {
                planSteps.add("    SERVER FILTER BY " + (hasFirstKeyOnlyFilter ? "FIRST KEY ONLY AND " : "") + filterDesc);
            } else if (hasFirstKeyOnlyFilter) {
                planSteps.add("    SERVER FILTER BY FIRST KEY ONLY");
            }
            if (pageFilter != null) {
                planSteps.add("    SERVER " + pageFilter.getPageSize() + " ROW LIMIT");
            }
        }
        groupBy.explain(planSteps);
    }

    private void appendPKColumnValue(StringBuilder buf, byte[] range, Boolean isNull, int slotIndex) {
        if (Boolean.TRUE.equals(isNull)) {
            buf.append("null");
            return;
        }
        if (Boolean.FALSE.equals(isNull)) {
            buf.append("not null");
            return;
        }
        if (range.length == 0) {
            buf.append('*');
            return;
        }
        ScanRanges scanRanges = context.getScanRanges();
        PDataType type = scanRanges.getSchema().getField(slotIndex).getDataType();
        ColumnModifier modifier = tableRef.getTable().getPKColumns().get(slotIndex).getColumnModifier();
        if (modifier != null) {
            buf.append('~');
            range = modifier.apply(range, 0, new byte[range.length], 0, range.length);
        }
        Format formatter = context.getConnection().getFormatter(type);
        buf.append(type.toStringLiteral(range, formatter));
    }
    
    private static class RowKeyValueIterator implements Iterator<byte[]> {
        private final RowKeySchema schema;
        private ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        private int position = 0;
        private final int maxOffset;
        private byte[] nextValue;
       
        public RowKeyValueIterator(RowKeySchema schema, byte[] rowKey) {
            this.schema = schema;
            this.maxOffset = schema.iterator(rowKey, ptr);
            iterate();
        }
        
        private void iterate() {
            if (schema.next(ptr, position++, maxOffset) == null) {
                nextValue = null;
            } else {
                nextValue = ptr.copyBytes();
            }
        }
        
        @Override
        public boolean hasNext() {
            return nextValue != null;
        }

        @Override
        public byte[] next() {
            if (nextValue == null) {
                throw new NoSuchElementException();
            }
            byte[] value = nextValue;
            iterate();
            return value;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
    }
    
    private void appendScanRow(StringBuilder buf, Bound bound) {
        ScanRanges scanRanges = context.getScanRanges();
        KeyRange minMaxRange = context.getMinMaxRange();
        Iterator<byte[]> minMaxIterator = Iterators.emptyIterator();
        if (minMaxRange != null) {
            RowKeySchema schema = tableRef.getTable().getRowKeySchema();
            if (!minMaxRange.isUnbound(bound)) {
                minMaxIterator = new RowKeyValueIterator(schema, minMaxRange.getRange(bound));
            }
        }
        int nRanges = scanRanges.getRanges().size();
        for (int i = 0, minPos = 0; minPos < nRanges || minMaxIterator.hasNext(); i++) {
            List<KeyRange> ranges = minPos >= nRanges ? EVERYTHING :  scanRanges.getRanges().get(minPos++);
            KeyRange range = bound == Bound.LOWER ? ranges.get(0) : ranges.get(ranges.size()-1);
            byte[] b = range.getRange(bound);
            Boolean isNull = KeyRange.IS_NULL_RANGE == range ? Boolean.TRUE : KeyRange.IS_NOT_NULL_RANGE == range ? Boolean.FALSE : null;
            if (minMaxIterator.hasNext()) {
                byte[] bMinMax = minMaxIterator.next();
                int cmp = Bytes.compareTo(bMinMax, b) * (bound == Bound.LOWER ? 1 : -1);
                if (cmp > 0) {
                    minPos = nRanges;
                    b = bMinMax;
                    isNull = null;
                } else if (cmp < 0) {
                    minMaxIterator = Iterators.emptyIterator();
                }
            }
            appendPKColumnValue(buf, b, isNull, i);
            buf.append(',');
        }
    }
    
    private void appendKeyRanges(StringBuilder buf) {
        ScanRanges scanRanges = context.getScanRanges();
        KeyRange minMaxRange = context.getMinMaxRange();
        if (minMaxRange == null && (scanRanges == ScanRanges.EVERYTHING || scanRanges == ScanRanges.NOTHING)) {
            return;
        }
        buf.append(" [");
        StringBuilder buf1 = new StringBuilder();
        appendScanRow(buf1, Bound.LOWER);
        buf.append(buf1);
        buf.setCharAt(buf.length()-1, ']');
        StringBuilder buf2 = new StringBuilder();
        appendScanRow(buf2, Bound.UPPER);
        if (!StringUtil.equals(buf1, buf2)) {
            buf.append( " - [");
            buf.append(buf2);
        }
        buf.setCharAt(buf.length()-1, ']');
    }
}
