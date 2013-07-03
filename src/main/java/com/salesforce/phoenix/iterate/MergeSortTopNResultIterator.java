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
import com.salesforce.phoenix.expression.OrderByExpression;
import com.salesforce.phoenix.schema.tuple.Tuple;

/**
 * 
 * ResultIterator that does a merge sort on the list of iterators provided,
 * returning the rows ordered by the OrderByExpression. The input
 * iterators must be ordered by the OrderByExpression.
 *
 * @author jtaylor
 * @since 0.1
 */
public class MergeSortTopNResultIterator extends MergeSortResultIterator {

    private final int limit;
    private int count = 0;
    private final List<OrderByExpression> orderByColumns;
    private final ImmutableBytesWritable ptr1 = new ImmutableBytesWritable();
    private final ImmutableBytesWritable ptr2 = new ImmutableBytesWritable();
    
    public MergeSortTopNResultIterator(ResultIterators iterators, Integer limit, List<OrderByExpression> orderByColumns) {
        super(iterators);
        this.limit = limit == null ? -1 : limit;
        this.orderByColumns = orderByColumns;
    }

    @Override
    protected int compare(Tuple t1, Tuple t2) {
        for (int i = 0; i < orderByColumns.size(); i++) {
            OrderByExpression order = orderByColumns.get(i);
            Expression orderExpr = order.getExpression();
            boolean isNull1 = !orderExpr.evaluate(t1, ptr1) || ptr1.getLength() == 0;
            boolean isNull2 = !orderExpr.evaluate(t2, ptr2) || ptr2.getLength() == 0;
            if (isNull1 && isNull2) {
                continue;
            } else if (isNull1) {
                return order.isNullsLast() ? 1 : -1;
            } else if (isNull2) {
                return order.isNullsLast() ? -1 : 1;
            }
            int cmp = ptr1.compareTo(ptr2);
            if (cmp == 0) {
                continue;
            }
            return order.isAscending() ? cmp : -cmp;
        }
        return 0;
    }

    @Override
    public Tuple peek() throws SQLException {
        if (limit >= 0 && count >= limit) {
            return null;
        }
        return super.peek();
    }

    @Override
    public Tuple next() throws SQLException {
        if (limit >= 0 && count++ >= limit) {
            return null;
        }
        return super.next();
    }


    @Override
    public void explain(List<String> planSteps) {
        resultIterators.explain(planSteps);
        planSteps.add("    SERVER TOP " + limit + " ROW" + (limit == 1 ? "" : "S") + " SORTED BY " + orderByColumns.toString());
        planSteps.add("CLIENT MERGE SORT");
    }
}
