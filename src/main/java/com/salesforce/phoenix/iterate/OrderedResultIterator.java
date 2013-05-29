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

import java.sql.SQLException;
import java.util.*;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;

import com.google.common.base.Function;
import com.google.common.collect.*;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.OrderByExpression;
import com.salesforce.phoenix.schema.PDataType;
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
    private static class ResultEntry {
        private final ImmutableBytesWritable[] sortKeys;
        private final Tuple result;

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

    /** A function that returns Nth key as LONGWritable for a given {@link ResultEntry}. */
    private static class NthKeyLongWritable implements Function<ResultEntry, LongWritable> {
        private final int index;

        NthKeyLongWritable(int index) {
            this.index = index;
        }
        @Override
        public LongWritable apply(ResultEntry entry) {
            ImmutableBytesWritable key = entry.getSortKey(index);
            return new LongWritable(WritableComparator.readLong(key.get(), key.getOffset()));
        }
    }

    /** Returns the expression of a given {@link OrderByExpression}. */
    private static final Function<OrderByExpression, Expression> TO_EXPRESSION = new Function<OrderByExpression, Expression>() {
        @Override
        public Expression apply(OrderByExpression column) {
            return column.getExpression();
        }
    };

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
                                 Integer limit) {
        this(delegate, orderByExpressions, limit, 0);
    }

    public OrderedResultIterator(ResultIterator delegate,
            List<OrderByExpression> orderByExpressions) throws SQLException {
        this(delegate, orderByExpressions, null);
    }

    public OrderedResultIterator(ResultIterator delegate, List<OrderByExpression> orderByExpressions, Integer limit,
            int estimatedRowSize) {
        checkArgument(!orderByExpressions.isEmpty());
        this.delegate = delegate;
        this.orderByExpressions = orderByExpressions;
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
            Ordering<ResultEntry> entryOrdering = null;
            if (col.getExpression().getDataType() == PDataType.RAW_LONG) {
                // LONG type data won't be able to be compared by bytes directly. 
                Ordering<LongWritable> o = Ordering.from(new LongWritable.Comparator());
                if(!col.isAscending()) o = o.reverse();
                o = col.isNullsLast() ? o.nullsLast() : o.nullsFirst();
                entryOrdering = o.onResultOf(new NthKeyLongWritable(pos++));
            } else {
                Ordering<ImmutableBytesWritable> o = Ordering.from(new ImmutableBytesWritable.Comparator());
                if(!col.isAscending()) o = o.reverse();
                o = col.isNullsLast() ? o.nullsLast() : o.nullsFirst();
                entryOrdering = o.onResultOf(new NthKey(pos++));
            }

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
        Collection<ResultEntry> entries;
        if (limit == null) {
            final List<ResultEntry> listEntries =  Lists.<ResultEntry>newArrayList(); // TODO: size?
            entries = listEntries;
            resultIterator = new BaseResultIterator() {
                private int i = -1;

                @Override
                public Tuple next() throws SQLException {
                    if (i == -1) {
                        Collections.<ResultEntry>sort(listEntries, comparator);
                    } 
                    if (++i >= listEntries.size()) {
                        resultIterator = ResultIterator.EMPTY_ITERATOR;
                        return null;
                    }
                    
                    return listEntries.get(i).getResult();
                }
            };
        } else {
            final MinMaxPriorityQueue<ResultEntry> queueEntries = MinMaxPriorityQueue.<ResultEntry>orderedBy(comparator).maximumSize(limit).create();
            entries = queueEntries;
            resultIterator = new BaseResultIterator() {

                @Override
                public Tuple next() throws SQLException {
                    ResultEntry entry = queueEntries.pollFirst();
                    if (entry == null) {
                        resultIterator = ResultIterator.EMPTY_ITERATOR;
                        return null;
                    }
                    return entry.getResult();
                }
                
            };
        }
        try {
            long byteSize = 0;
            for (Tuple result = delegate.next(); result != null; result = delegate.next()) {
                int pos = 0;
                ImmutableBytesWritable[] sortKeys = new ImmutableBytesWritable[numSortKeys];
                for (Expression expression : expressions) {
                    final ImmutableBytesWritable sortKey = new ImmutableBytesWritable();
                    boolean evaluated = expression.evaluate(result, sortKey);
                    // set the sort key that failed to get evaluated with null
                    sortKeys[pos++] = evaluated && sortKey.getLength() > 0 ? sortKey : null;
                }
                entries.add(new ResultEntry(sortKeys, result));
                for (int i = 0; i < result.size(); i++) {
                    KeyValue keyValue = result.getValue(i);
                    byteSize += 
                        // ResultEntry
                        SizedUtil.OBJECT_SIZE + 
                        // ImmutableBytesWritable[]
                        SizedUtil.ARRAY_SIZE + numSortKeys * SizedUtil.IMMUTABLE_BYTES_WRITABLE_SIZE +
                        // Tuple
                        SizedUtil.OBJECT_SIZE + keyValue.getLength();
                }
            }
            this.byteSize = byteSize;
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
