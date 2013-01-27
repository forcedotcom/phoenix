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
package phoenix.iterate;

import java.text.Format;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import phoenix.compile.ScanKey;
import phoenix.compile.StatementContext;
import phoenix.schema.*;
import phoenix.util.SchemaUtil;

public abstract class ExplainTable {
    protected final StatementContext context;
    protected final TableRef table;
   
    public ExplainTable(StatementContext context, TableRef table) {
        this.context = context;
        this.table = table;
    }

    protected void explain(String prefix, List<String> planSteps) {
        StringBuilder buf = new StringBuilder(prefix);
        ScanKey scanKey = context.getScanKey();
        if (scanKey.getLowerRange().length == 0 && scanKey.getUpperRange().length == 0) {
            buf.append("FULL SCAN ");
        } else {
            buf.append("RANGE SCAN ");
        }
        buf.append("OVER " + SchemaUtil.getTableDisplayName(this.table.getSchema().getName(), table.getTable().getName().getString()));
        appendKeyRange(buf);
        planSteps.add(buf.toString());
        
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        if (filter != null) {
            planSteps.add("    SERVER FILTER BY " + filter.toString());
        }
        context.getGroupBy().explain(planSteps);
    }

    private void appendPKColumnValues(StringBuilder buf, byte[] key, RowKeySchema schema) {
        ImmutableBytesWritable ptr = context.getTempPtr();
        ptr.set(key, 0, 0);

        int i = 0;
        for (Boolean hasValue = schema.first(ptr, i, ValueBitSet.EMPTY_VALUE_BITSET); hasValue != null; hasValue=schema.next(ptr, ++i, ValueBitSet.EMPTY_VALUE_BITSET)) {
            if (hasValue) {
                PDataType type = schema.getField(i).getType();
                Format formatter = context.getConnection().getFormatter(type);
                Object value = type.toObject(ptr);
                boolean isString = type.isCoercibleTo(PDataType.VARCHAR);
                if (isString) buf.append('\''); // TODO: PDataType.toString(Object, Format) method?
                buf.append(formatter == null ? value.toString() : formatter.format(value));
                if (isString) buf.append('\'');
            } else {
                buf.append("null");
            }
            buf.append(',');
        }
    }
    
    private void appendKeyRange(StringBuilder buf) {
        ScanKey scanKey = context.getScanKey();
        boolean noStartKey = scanKey.getLowerRange().length == 0;
        boolean noEndKey = scanKey.getUpperRange().length == 0;
        if (noStartKey && noEndKey) {
            return;
        } else {
            if (!noStartKey) {
                buf.append(" FROM (");
                appendPKColumnValues(buf, scanKey.getLowerRange(), scanKey.getLowerSchema());
                buf.setCharAt(buf.length()-1, ')');
                buf.append(scanKey.isLowerInclusive() ? " INCLUSIVE" : " EXCLUSIVE");
            }
            if (!noEndKey) {
                buf.append(" TO (");
                appendPKColumnValues(buf, scanKey.getUpperRange(), scanKey.getUpperSchema());
                buf.setCharAt(buf.length()-1, ')');
                buf.append(scanKey.isUpperInclusive() ? " INCLUSIVE" : " EXCLUSIVE");
            }
        }
    }
}
