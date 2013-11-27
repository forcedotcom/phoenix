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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.KeyValueColumnExpression;
import com.salesforce.phoenix.schema.tuple.SingleKeyValueTuple;



/**
 *
 * Modeled after {@link org.apache.hadoop.hbase.filter.SingleColumnValueFilter},
 * but for general expression evaluation in the case where only a single KeyValue
 * column is referenced in the expression.
 *
 * @author jtaylor
 * @since 0.1
 */
public abstract class SingleKeyValueComparisonFilter extends BooleanExpressionFilter {
    private final SingleKeyValueTuple inputTuple = new SingleKeyValueTuple();
    private boolean matchedColumn;
    protected byte[] cf;
    protected byte[] cq;

    public SingleKeyValueComparisonFilter() {
    }

    public SingleKeyValueComparisonFilter(Expression expression) {
        super(expression);
        init();
    }

    protected abstract int compare(byte[] cfBuf, int cfOffset, int cfLength, byte[] cqBuf, int cqOffset, int cqLength);

    private void init() {
        EvaluateOnCompletionVisitor visitor = new EvaluateOnCompletionVisitor() {
            @Override
            public Void visit(KeyValueColumnExpression expression) {
                cf = expression.getColumnFamily();
                cq = expression.getColumnName();
                return null;
            }
        };
        expression.accept(visitor);
        this.evaluateOnCompletion = visitor.evaluateOnCompletion();
    }

    private boolean foundColumn() {
        return inputTuple.size() > 0;
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue keyValue) {
        if (this.matchedColumn) {
          // We already found and matched the single column, all keys now pass
          // TODO: why won't this cause earlier versions of a kv to be included?
          return ReturnCode.INCLUDE;
        }
        if (this.foundColumn()) {
          // We found all the columns, but did not match the expression, so skip to next row
          return ReturnCode.NEXT_ROW;
        }
        byte[] buf = keyValue.getBuffer();
        if (compare(buf, keyValue.getFamilyOffset(), keyValue.getFamilyLength(), buf, keyValue.getQualifierOffset(), keyValue.getQualifierLength()) != 0) {
            // Remember the key in case this is the only key value we see.
            // We'll need it if we have row key columns too.
            inputTuple.setKey(keyValue);
            // This is a key value we're not interested in
            // TODO: use NEXT_COL when bug fix comes through that includes the row still
            return ReturnCode.INCLUDE;
        }
        inputTuple.setKeyValue(keyValue);

        // We have the columns, so evaluate here
        if (!Boolean.TRUE.equals(evaluate(inputTuple))) {
            return ReturnCode.NEXT_ROW;
        }
        this.matchedColumn = true;
        return ReturnCode.INCLUDE;
    }

    @Override
    public boolean filterRow() {
        // If column was found, return false if it was matched, true if it was not.
        if (foundColumn()) {
            return !this.matchedColumn;
        }
        // If column was not found, evaluate the expression here upon completion.
        // This is required with certain expressions, for example, with IS NULL
        // expressions where they'll evaluate to TRUE when the column being
        // tested wasn't found.
        // Since the filter is called also to position the scan initially, we have
        // to guard against this by checking whether or not we've filtered in
        // the key value (i.e. filterKeyValue was called and we found the keyValue
        // for which we're looking).
        if (inputTuple.hasKey() && evaluateOnCompletion()) {
            return !Boolean.TRUE.equals(evaluate(inputTuple));
        }
        // Finally, if we have no values, and we're not required to re-evaluate it
        // just filter the row
        return true;
    }

      @Override
    public void reset() {
        inputTuple.reset();
        matchedColumn = false;
        super.reset();
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        init();
    }

    @SuppressWarnings("all") // suppressing missing @Override since this doesn't exist for HBase 0.94.4
    public boolean isFamilyEssential(byte[] name) {
        // Only the column families involved in the expression are essential.
        // The others are for columns projected in the select expression
        return Bytes.compareTo(cf, name) == 0;
    }
}
