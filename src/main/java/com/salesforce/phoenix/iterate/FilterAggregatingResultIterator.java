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

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Post aggregation filter for HAVING clause. Due to the way we cache aggregation values
 * we cannot have a look ahead for this Iterator, because the expressions in the SELECT
 * clause would return values for the peeked row instead of the current row. If we only
 * use the Result argument in {@link com.salesforce.phoenix.expression.Expression}
 * instead of our cached value in Aggregators, we could have a look ahead.
 *
 * @author jtaylor
 * @since 0.1
 */
public class FilterAggregatingResultIterator  implements AggregatingResultIterator {
    private final AggregatingResultIterator delegate;
    private final Expression expression;
    private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    
    public FilterAggregatingResultIterator(AggregatingResultIterator delegate, Expression expression) {
        this.delegate = delegate;
        this.expression = expression;
        if (expression.getDataType() != PDataType.BOOLEAN) {
            throw new IllegalArgumentException("FilterResultIterator requires a boolean expression, but got " + expression);
        }
    }

    @Override
    public Tuple next() throws SQLException {
        Tuple next;
        do {
            next = delegate.next();
        } while (next != null && expression.evaluate(next, ptr) && Boolean.FALSE.equals(expression.getDataType().toObject(ptr)));
        return next;
    }

    @Override
    public void close() throws SQLException {
        delegate.close();
    }

    @Override
    public void aggregate(Tuple result) {
        delegate.aggregate(result);
    }

    @Override
    public void explain(List<String> planSteps) {
        delegate.explain(planSteps);
        planSteps.add("CLIENT FILTER BY " + expression.toString());
    }
}
