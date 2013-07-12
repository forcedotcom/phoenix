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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndex;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.OrderByExpression;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.SizedUtil;

/**
 * Result scanner that sorts aggregated rows by columns specified in the ORDER BY clause.
 * <p>
 * Note that currently the sort is entirely done in memory. 
 *  
 * @author syyang, jtaylor
 * @since 0.1
 */
public class OrderedResultIterator implements ResultIterator {

    /** A container that holds pointers to a {@link Result} and its sort keys. */
    protected static class ResultEntry {
        protected final ImmutableBytesWritable[] sortKeys;
        protected final Tuple result;

        ResultEntry(ImmutableBytesWritable[] sortKeys, Tuple result) {
            this.sortKeys = sortKeys;
            this.result = result;
        }
        
        ImmutableBytesWritable getSortKey(int index) {
            checkPositionIndex(index, sortKeys.length);
            return sortKeys[index];
        }
        
        Tuple getResult() {
            return result;
        }
    }
    
    /** A function that returns Nth key for a given {@link ResultEntry}. */
    private static class NthKey implements Function<ResultEntry, ImmutableBytesWritable> {
        private final int index;

        NthKey(int index) {
            this.index = index;
        }
        @Override
        public ImmutableBytesWritable apply(ResultEntry entry) {
            return entry.getSortKey(index);
        }
    }

    /** Returns the expression of a given {@link OrderByExpression}. */
    private static final Function<OrderByExpression, Expression> TO_EXPRESSION = new Function<OrderByExpression, Expression>() {
        @Override
        public Expression apply(OrderByExpression column) {
            return column.getExpression();
        }
    };

    private final int thresholdBytes;
    private final Integer limit;
    private final ResultIterator delegate;
    private final List<OrderByExpression> orderByExpressions;
    private final long estimatedByteSize;
    
    private ResultIterator resultIterator;
    private long byteSize;

    protected ResultIterator getDelegate() {
        return delegate;
    }
    
    public OrderedResultIterator(ResultIterator delegate,
                                 List<OrderByExpression> orderByExpressions,
                                 int thresholdBytes, Integer limit) {
        this(delegate, orderByExpressions, thresholdBytes, limit, 0);
    }

    public OrderedResultIterator(ResultIterator delegate,
            List<OrderByExpression> orderByExpressions, int thresholdBytes) throws SQLException {
        this(delegate, orderByExpressions, thresholdBytes, null);
    }

    public OrderedResultIterator(ResultIterator delegate, List<OrderByExpression> orderByExpressions, 
            int thresholdBytes, Integer limit, int estimatedRowSize) {
        checkArgument(!orderByExpressions.isEmpty());
        this.delegate = delegate;
        this.orderByExpressions = orderByExpressions;
        this.thresholdBytes = thresholdBytes;
        this.limit = limit;
        long estimatedEntrySize =
            // ResultEntry
            SizedUtil.OBJECT_SIZE + 
            // ImmutableBytesWritable[]
            SizedUtil.ARRAY_SIZE + orderByExpressions.size() * SizedUtil.IMMUTABLE_BYTES_WRITABLE_SIZE +
            // Tuple
            SizedUtil.OBJECT_SIZE + estimatedRowSize;

        // Make sure we don't overflow Long, though this is really unlikely to happen.
        assert(limit == null || Long.MAX_VALUE / estimatedEntrySize >= limit);

        this.estimatedByteSize = limit == null ? 0 : limit * estimatedEntrySize;
    }

    public Integer getLimit() {
        return limit;
    }

    public long getEstimatedByteSize() {
        return estimatedByteSize;
    }

    public long getByteSize() {
        return byteSize;
    }
    /**
     * Builds a comparator from the list of columns in ORDER BY clause.
     * @param orderByExpressions the columns in ORDER BY clause.
     * @return the comparator built from the list of columns in ORDER BY clause.
     */
    // ImmutableBytesWritable.Comparator doesn't implement generics
    @SuppressWarnings("unchecked")
    private static Comparator<ResultEntry> buildComparator(List<OrderByExpression> orderByExpressions) {
        Ordering<ResultEntry> ordering = null;
        int pos = 0;
        for (OrderByExpression col : orderByExpressions) {
            Ordering<ImmutableBytesWritable> o = Ordering.from(new ImmutableBytesWritable.Comparator());
            if(!col.isAscending()) o = o.reverse();
            o = col.isNullsLast() ? o.nullsLast() : o.nullsFirst();
            Ordering<ResultEntry> entryOrdering = o.onResultOf(new NthKey(pos++));
            ordering = ordering == null ? entryOrdering : ordering.compound(entryOrdering);
        }
        return ordering;
    }

    @Override
    public Tuple next() throws SQLException {
        return getResultIterator().next();
    }
    
    private ResultIterator getResultIterator() throws SQLException {
        if (resultIterator != null) {
            return resultIterator;
        }
        
        final int numSortKeys = orderByExpressions.size();
        List<Expression> expressions = Lists.newArrayList(Collections2.transform(orderByExpressions, TO_EXPRESSION));
        final Comparator<ResultEntry> comparator = buildComparator(orderByExpressions);
        try{
            final MappedByteBufferSortedQueue queueEntries = new MappedByteBufferSortedQueue(comparator, limit, thresholdBytes);
            resultIterator = new BaseResultIterator() {
                int count = 0;
                @Override
                public Tuple next() throws SQLException {
                    ResultEntry entry = queueEntries.poll();
                    if (entry == null || (limit != null && ++count > limit)) {
                        resultIterator = ResultIterator.EMPTY_ITERATOR;
                        return null;
                    }
                    return entry.getResult();
                }

                @Override
                public void close() throws SQLException {
                    queueEntries.close();
                }
            };
            for (Tuple result = delegate.next(); result != null; result = delegate.next()) {
                int pos = 0;
                ImmutableBytesWritable[] sortKeys = new ImmutableBytesWritable[numSortKeys];
                for (Expression expression : expressions) {
                    final ImmutableBytesWritable sortKey = new ImmutableBytesWritable();
                    boolean evaluated = expression.evaluate(result, sortKey);
                    // set the sort key that failed to get evaluated with null
                    sortKeys[pos++] = evaluated && sortKey.getLength() > 0 ? sortKey : null;
                }
                queueEntries.add(new ResultEntry(sortKeys, result));
            }
            this.byteSize = queueEntries.getByteSize();
        } catch (IOException e) {
            throw new SQLException("", e);
        } finally {
            delegate.close();
        }
        
        return resultIterator;
    }

    @Override
    public void close()  {
        resultIterator = ResultIterator.EMPTY_ITERATOR;
    }


    @Override
    public void explain(List<String> planSteps) {
        delegate.explain(planSteps);
        planSteps.add("CLIENT" + (limit == null ? "" : " TOP " + limit + " ROW"  + (limit == 1 ? "" : "S"))  + " SORTED BY " + orderByExpressions.toString());
    }
}
