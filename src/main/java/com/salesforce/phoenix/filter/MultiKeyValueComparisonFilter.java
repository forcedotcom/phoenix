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
package com.salesforce.phoenix.filter;

import java.io.DataInput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.KeyValueColumnExpression;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Modeled after {@link org.apache.hadoop.hbase.filter.SingleColumnValueFilter},
 * but for general expression evaluation in the case where multiple KeyValue
 * columns are referenced in the expression.
 *
 * @author jtaylor
 * @since 0.1
 */
public abstract class MultiKeyValueComparisonFilter extends BooleanExpressionFilter {
    private static final byte[] UNITIALIZED_KEY_BUFFER = new byte[0];

    private Boolean matchedColumn;
    protected final IncrementalResultTuple inputTuple = new IncrementalResultTuple();

    public MultiKeyValueComparisonFilter() {
    }

    public MultiKeyValueComparisonFilter(Expression expression) {
        super(expression);
        init();
    }

    private static final class KeyValueRef {
        public KeyValue keyValue;
        
        @Override
        public String toString() {
            if(keyValue != null) {
                return keyValue.toString() + " value = " + Bytes.toStringBinary(keyValue.getValue());
            } else {
                return super.toString();
            }
        }
    }
    
    protected abstract Object setColumnKey(byte[] cf, int cfOffset, int cfLength, byte[] cq, int cqOffset, int cqLength);
    protected abstract Object newColumnKey(byte[] cf, int cfOffset, int cfLength, byte[] cq, int cqOffset, int cqLength);
    
    private final class IncrementalResultTuple implements Tuple {
        private int refCount;
        private final ImmutableBytesWritable keyPtr = new ImmutableBytesWritable(UNITIALIZED_KEY_BUFFER);
        private final Map<Object,KeyValueRef> foundColumns = new HashMap<Object,KeyValueRef>(5);
        
        public void reset() {
            refCount = 0;
            keyPtr.set(UNITIALIZED_KEY_BUFFER);
            for (KeyValueRef ref : foundColumns.values()) {
                ref.keyValue = null;
            }
        }
        
        @Override
        public boolean isImmutable() {
            return refCount == foundColumns.size();
        }
        
        public void setImmutable() {
            refCount = foundColumns.size();
        }
        
        public ReturnCode resolveColumn(KeyValue value) {
            // Always set key, in case we never find a key value column of interest,
            // and our expression uses row key columns.
            setKey(value);
            byte[] buf = value.getBuffer();
            Object ptr = setColumnKey(buf, value.getFamilyOffset(), value.getFamilyLength(), buf, value.getQualifierOffset(), value.getQualifierLength());
            KeyValueRef ref = foundColumns.get(ptr);
            if (ref == null) {
                // Return INCLUDE here. Although this filter doesn't need this KV
                // it should still be projected into the Result
                return ReturnCode.INCLUDE;
            }
            // Since we only look at the latest key value for a given column,
            // we are not interested in older versions
            // TODO: test with older versions to confirm this doesn't get tripped
            // This shouldn't be necessary, because a scan only looks at the latest
            // version
            if (ref.keyValue != null) {
                // Can't do NEXT_ROW, because then we don't match the other columns
                // SKIP, INCLUDE, and NEXT_COL seem to all act the same
                return ReturnCode.NEXT_COL;
            }
            ref.keyValue = value;
            refCount++;
            return null;
        }
        
        public void addColumn(byte[] cf, byte[] cq) {
            Object ptr = MultiKeyValueComparisonFilter.this.newColumnKey(cf, 0, cf.length, cq, 0, cq.length);
            foundColumns.put(ptr, new KeyValueRef());
        }
        
        public void setKey(KeyValue value) {
            keyPtr.set(value.getBuffer(), value.getRowOffset(), value.getRowLength());
        }
        
        @Override
        public void getKey(ImmutableBytesWritable ptr) {
            ptr.set(keyPtr.get(),keyPtr.getOffset(),keyPtr.getLength());
        }
        
        @Override
        public KeyValue getValue(byte[] cf, byte[] cq) {
            Object ptr = setColumnKey(cf, 0, cf.length, cq, 0, cq.length);
            KeyValueRef ref = foundColumns.get(ptr);
            return ref == null ? null : ref.keyValue;
        }
        
        @Override
        public String toString() {
            return foundColumns.toString();
        }

        @Override
        public int size() {
            return refCount;
        }

        @Override
        public KeyValue getValue(int index) {
            // This won't perform very well, but it's not
            // currently used anyway
            for (KeyValueRef ref : foundColumns.values()) {
                if (ref.keyValue == null) {
                    continue;
                }
                if (index == 0) {
                    return ref.keyValue;
                }
                index--;
            }
            throw new IndexOutOfBoundsException(Integer.toString(index));
        }
    }
    
    protected void init() {
        EvaluateOnCompletionVisitor visitor = new EvaluateOnCompletionVisitor() {
            @Override
            public Void visit(KeyValueColumnExpression expression) {
                inputTuple.addColumn(expression.getColumnFamily(), expression.getColumnName());
                return null;
            }
        };
        expression.accept(visitor);
        this.evaluateOnCompletion = visitor.evaluateOnCompletion();
        expression.reset();
    }
    
    @Override
    public ReturnCode filterKeyValue(KeyValue keyValue) {
        if (this.matchedColumn == Boolean.TRUE) {
          // We already found and matched the single column, all keys now pass
          return ReturnCode.INCLUDE;
        }
        if (this.matchedColumn == Boolean.FALSE) {
          // We found all the columns, but did not match the expression, so skip to next row
          return ReturnCode.NEXT_ROW;
        }
        // This is a key value we're not interested in (TODO: why INCLUDE here instead of NEXT_COL?)
        ReturnCode code = inputTuple.resolveColumn(keyValue);
        if (code != null) {
            return code;
        }
        
        // We found a new column, so we can re-evaluate
        // TODO: if we have row key columns in our expression, should
        // we always evaluate or just wait until the end?
        this.matchedColumn = this.evaluate(inputTuple);
        if (this.matchedColumn == null) {
            if (inputTuple.isImmutable()) {
                this.matchedColumn = Boolean.FALSE;
            } else {
                return ReturnCode.INCLUDE;
            }
        }
        return this.matchedColumn ? ReturnCode.INCLUDE : ReturnCode.NEXT_ROW;
    }

    @Override
    public boolean filterRow() {
        if (this.matchedColumn == null && !inputTuple.isImmutable() && evaluateOnCompletion()) {
            inputTuple.setImmutable();
            this.matchedColumn = this.evaluate(inputTuple);
        }
        
        return ! (Boolean.TRUE.equals(this.matchedColumn));
    }

      @Override
    public void reset() {
        matchedColumn = null;
        inputTuple.reset();
        super.reset();
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        init();
    }
}
