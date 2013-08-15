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
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;

import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.*;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.util.SchemaUtil;


public abstract class ExplainTable {
    protected final StatementContext context;
    protected final TableRef table;
    protected final GroupBy groupBy;
   
    public ExplainTable(StatementContext context, TableRef table) {
        this(context,table,GroupBy.EMPTY_GROUP_BY);
    }

    public ExplainTable(StatementContext context, TableRef table, GroupBy groupBy) {
        this.context = context;
        this.table = table;
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
        buf.append("OVER " + SchemaUtil.getTableDisplayName(this.table.getSchema().getName(), table.getTable().getName().getString()));
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

    private void appendPKColumnValue(StringBuilder buf, byte[] range, int slotIndex) {
        if (range.length == 0) {
            buf.append("null");
            return;
        }
        ScanRanges scanRanges = context.getScanRanges();
        PDataType type = scanRanges.getSchema().getField(slotIndex).getType();
        ColumnModifier modifier = table.getTable().getPKColumns().get(slotIndex).getColumnModifier();
        if (modifier != null) {
            range = modifier.apply(range, 0, new byte[range.length], 0, range.length);
        }
        Format formatter = context.getConnection().getFormatter(type);
        buf.append(type.toStringLiteral(range, formatter));
    }
    
    private void appendKeyRange(StringBuilder buf, KeyRange range, int i) {
        if (range.isSingleKey()) {
            appendPKColumnValue(buf, range.getLowerRange(), i);
        } else {
            buf.append(range.isLowerInclusive() ? '[' : '(');
            if (range.lowerUnbound()) {
                buf.append('*');
            } else {
                appendPKColumnValue(buf, range.getLowerRange(), i);
            }
            buf.append('-');
            if (range.upperUnbound()) {
                buf.append('*');
            } else {
                appendPKColumnValue(buf, range.getUpperRange(), i);
            }
            buf.append(range.isUpperInclusive() ? ']' : ')');
        }
    }
    
    private void appendKeyRanges(StringBuilder buf) {
        ScanRanges scanRanges = context.getScanRanges();
        if (scanRanges == ScanRanges.EVERYTHING || scanRanges == ScanRanges.NOTHING) {
            return;
        }
        buf.append(' ');
        for (int i = 0; i < scanRanges.getRanges().size(); i++) {
            List<KeyRange> ranges = scanRanges.getRanges().get(i);
            KeyRange lower = ranges.get(0);
            appendKeyRange(buf, lower, i);
            if (ranges.size() > 1) {
                KeyRange upper = ranges.get(ranges.size()-1);
                buf.append("...");
                appendKeyRange(buf, upper, i);
            }
            buf.append(",");
        }
        buf.setLength(buf.length() - 1);
    }
}
