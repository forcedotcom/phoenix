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

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import phoenix.expression.Expression;
import phoenix.schema.PDataType;
import phoenix.schema.tuple.Tuple;

/**
 * 
 * Result scanner that filters out rows based on the results of a boolean
 * expression (i.e. filters out if {@link phoenix.expression.Expression#evaluate(Tuple, ImmutableBytesWritable)}
 * returns false or the ptr contains a FALSE value}). May not be used where
 * the delegate provided is an {@link phoenix.iterate.AggregatingResultIterator}.
 * For these, the {@link phoenix.iterate.FilterAggregatingResultIterator} should be used.
 *
 * @author jtaylor
 * @since 0.1
 */
public class FilterResultIterator  extends LookAheadResultIterator {
    private final ResultIterator delegate;
    private final Expression expression;
    private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    
    public FilterResultIterator(ResultIterator delegate, Expression expression) {
        if (delegate instanceof AggregatingResultIterator) {
            throw new IllegalArgumentException("FilterResultScanner may not be used with an aggregate delegate. Use phoenix.iterate.FilterAggregateResultScanner instead");
        }
        this.delegate = delegate;
        this.expression = expression;
        if (expression.getDataType() != PDataType.BOOLEAN) {
            throw new IllegalArgumentException("FilterResultIterator requires a boolean expression, but got " + expression);
        }
    }

    @Override
    protected Tuple advance() throws SQLException {
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
    public void explain(List<String> planSteps) {
        delegate.explain(planSteps);
        planSteps.add("CLIENT FILTER BY " + expression.toString());
    }
}
