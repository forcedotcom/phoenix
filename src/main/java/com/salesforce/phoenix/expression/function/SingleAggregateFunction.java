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
package com.salesforce.phoenix.expression.function;

import java.io.DataInput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.expression.aggregator.Aggregator;
import com.salesforce.phoenix.expression.visitor.ExpressionVisitor;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Base class for aggregate functions that calculate an aggregation
 * using a single {{@link Aggregator}
 *
 * @author jtaylor
 * @since 0.1
 */
abstract public class SingleAggregateFunction extends AggregateFunction {
    private static final List<Expression> DEFAULT_EXPRESSION_LIST = Arrays.<Expression>asList(LiteralExpression.newConstant(1));
    protected boolean isConstant;
    private Aggregator aggregator;
    
    /**
     * Sort aggregate functions with nullable fields last. This allows us not to have to store trailing null values.
     * Within non-nullable/nullable groups, put fixed width values first since we can access those more efficiently
     * (i.e. we can skip over groups of them in-mass instead of reading the length of each one to skip over as
     * required by a variable length value).
     */
    public static final Comparator<SingleAggregateFunction> SCHEMA_COMPARATOR = new Comparator<SingleAggregateFunction>() {

        @Override
        public int compare(SingleAggregateFunction o1, SingleAggregateFunction o2) {
            boolean isNullable1 = o1.isNullable();
            boolean isNullable2 = o2.isNullable();
            if (isNullable1 != isNullable2) {
                return isNullable1 ? 1 : -1;
            }
            isNullable1 = o1.getAggregatorExpression().isNullable();
            isNullable2 = o2.getAggregatorExpression().isNullable();
            if (isNullable1 != isNullable2) {
                return isNullable1 ? 1 : -1;
            }
            // Ensures COUNT(1) sorts first TODO: unit test for this
            boolean isConstant1 = o1.isConstantExpression();
            boolean isConstant2 = o2.isConstantExpression();
            if (isConstant1 != isConstant2) {
                return isConstant1 ? 1 : -1;
            }
            PDataType r1 = o1.getAggregator().getDataType();
            PDataType r2 = o2.getAggregator().getDataType();
            if (r1.isFixedWidth() != r2.isFixedWidth()) {
                return r1.isFixedWidth() ? -1 : 1;
            }
            return r1.compareTo(r2);
        }
    };
    
    protected SingleAggregateFunction() {
        this(DEFAULT_EXPRESSION_LIST, true);
    }

    public SingleAggregateFunction(List<Expression> children) {
        this(children, children.get(0) instanceof LiteralExpression);
    }
    
    private SingleAggregateFunction(List<Expression> children, boolean isConstant) {
        super(children);
        this.isConstant = children.get(0) instanceof LiteralExpression;
        this.aggregator = newClientAggregator();
    }

    public boolean isConstantExpression() {
        return isConstant;
    }
    
    @Override
    public PDataType getDataType() {
        return children.get(0).getDataType();
    }
    
    public Expression getAggregatorExpression() {
        return children.get(0);
    }
    
    public Aggregator getAggregator() {
        return aggregator;
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        return getAggregator().evaluate(tuple, ptr);
    }

    /**
     * Create the aggregator to do server-side aggregation.
     * The data type of the returned Aggregator must match
     * the data type returned by {@link #newClientAggregator()}
     * @return the aggregator to use on the server-side
     */
    abstract public Aggregator newServerAggregator();
    /**
     * Create the aggregator to do client-side aggregation
     * based on the results returned from the aggregating
     * coprocessor. The data type of the returned Aggregator
     * must match the data type returned by {@link #newServerAggregator()}
     * @return the aggregator to use on the client-side
     */
    public Aggregator newClientAggregator() {
        return newServerAggregator();
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        aggregator = newServerAggregator();
    }

    @Override
    public boolean isNullable() {
        return true;
    }
    
    protected SingleAggregateFunction getDelegate() {
        return this;
    }

    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        SingleAggregateFunction function = getDelegate();
        List<T> l = acceptChildren(visitor, visitor.visitEnter(function));
        T t = visitor.visitLeave(function, l);
        if (t == null) {
            t = visitor.defaultReturn(function, l);
        }
        return t;
    }
}
