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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import com.salesforce.phoenix.compile.ScanRanges;
import com.salesforce.phoenix.compile.StatementContext;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.SchemaUtil;


public abstract class ExplainTable {
    protected final StatementContext context;
    protected final TableRef table;
   
    public ExplainTable(StatementContext context, TableRef table) {
        this.context = context;
        this.table = table;
    }

    private boolean explainSkipScan(StringBuilder buf) {
        ScanRanges scanRanges = context.getScanRanges();
        if (scanRanges.useSkipScanFilter()) {
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
            buf.append("RANGE SCAN ");
            hasSkipScanFilter = explainSkipScan(buf);
        }
        buf.append("OVER " + SchemaUtil.getTableDisplayName(this.table.getSchema().getName(), table.getTable().getName().getString()));
        appendKeyRange(buf);
        planSteps.add(buf.toString());
        
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        if (filter != null) {
            String filterDesc = "";
            if (hasSkipScanFilter) {
                if (filter instanceof FilterList) {
                    FilterList filterList = (FilterList) filter;
                    assert (filterList.getFilters().size() == 2);
                    filterDesc = filterList.getFilters().get(1).toString();
                }
            } else {
                filterDesc = filter.toString();
            }
            if (filterDesc.length() > 0) {
                planSteps.add("    SERVER FILTER BY " + filterDesc);
            }
        }
        context.getGroupBy().explain(planSteps);
    }

    private void appendPKColumnValue(StringBuilder buf, byte[] range, PDataType type) {
        if (range.length == 0) {
            buf.append("null");
            return;
        }
        Format formatter = context.getConnection().getFormatter(type);
        //Object value = type.toObject(ptr);
        boolean isString = type.isCoercibleTo(PDataType.VARCHAR);
        if (isString) buf.append('\''); // TODO: PDataType.toString(Object, Format) method?
        Object o = type.toObject(range);
        if (isString) {
            String s = (String) o;
            for (int i = 0; i < s.length() && s.charAt(i) != '\0'; i++) {
                buf.append(s.charAt(i));
            }
        } else {
            buf.append(formatter == null ? o : formatter.format(o));
        }
        if (isString) buf.append('\'');
    }
    
    private void appendKeyRange(StringBuilder buf) {
        ScanRanges scanRanges = context.getScanRanges();
        if (scanRanges == ScanRanges.EVERYTHING || scanRanges == ScanRanges.NOTHING) {
            return;
        }
//        boolean noStartKey = scanKey.getLowerRange().length == 0;
//        boolean noEndKey = scanKey.getUpperRange().length == 0;
//        if (!noStartKey) {
//            buf.append(" FROM (");
//            appendPKColumnValues(buf, scanKey.getLowerRange(), scanKey.ge
//            buf.setCharAt(buf.length()-1, ')');
//            buf.append(scanKey.isLowerInclusive() ? " INCLUSIVE" : " EXCL
//        }
//        if (!noEndKey) {
//            buf.append(" TO (");
//            appendPKColumnValues(buf, scanKey.getUpperRange(), scanKey.ge
//            buf.setCharAt(buf.length()-1, ')');
//            buf.append(scanKey.isUpperInclusive() ? " INCLUSIVE" : " EXCL
//        }
        int i = 0;
        buf.append(' ');
        for (List<KeyRange> ranges : scanRanges.getRanges()) {
            KeyRange lower = ranges.get(0);
            KeyRange upper = ranges.get(ranges.size()-1);
            PDataType dataType = scanRanges.getSchema().getField(i).getType();
            if (lower.isSingleKey() && ranges.size() == 1) {
                appendPKColumnValue(buf, lower.getLowerRange(), dataType);
            } else {
                buf.append(lower.isLowerInclusive() ? '[' : '(');
                if (lower.lowerUnbound()) {
                    buf.append('*');
                } else {
                    appendPKColumnValue(buf, lower.getLowerRange(), dataType);
                }
                buf.append('-');
                if (upper.upperUnbound()) {
                    buf.append('*');
                } else {
                    appendPKColumnValue(buf, upper.getUpperRange(), dataType);
                }
                buf.append(upper.isUpperInclusive() ? ']' : ')');
            }
            buf.append(",");
        }
        buf.setLength(buf.length() - 1);
    }
}
