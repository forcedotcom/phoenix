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
import java.util.*;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.salesforce.phoenix.compile.ColumnProjector;
import com.salesforce.phoenix.compile.RowProjector;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.schema.tuple.Tuple;

/**
 * Result scanner that dedups the incoming tuples to make them distinct.
 * <p>
 * Note that the results are held in memory
 *  
 * @author jtaylor
 * @since 1.2
 */
public class DistinctAggregatingResultIterator implements AggregatingResultIterator {
    private final AggregatingResultIterator delegate;
    private final RowProjector rowProjector;
    private Iterator<ResultEntry> resultIterator;
    private final ImmutableBytesWritable ptr1 = new ImmutableBytesWritable();
    private final ImmutableBytesWritable ptr2 = new ImmutableBytesWritable();

    private class ResultEntry {
        private final int hashCode;
        private final Tuple result;

        ResultEntry(Tuple result) {
            final int prime = 31;
            this.result = result;
            int hashCode = 0;
            for (ColumnProjector column : rowProjector.getColumnProjectors()) {
                Expression e = column.getExpression();
                if (e.evaluate(this.result, ptr1)) {
                    hashCode = prime * hashCode + ptr1.hashCode();
                }
            }
            this.hashCode = hashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o == null) {
                return false;
            }
            if (o.getClass() != this.getClass()) {
                return false;
            }
            ResultEntry that = (ResultEntry) o;
            for (ColumnProjector column : rowProjector.getColumnProjectors()) {
                Expression e = column.getExpression();
                boolean isNull1 = !e.evaluate(this.result, ptr1);
                boolean isNull2 = !e.evaluate(that.result, ptr2);
                if (isNull1 && isNull2) {
                    return true;
                }
                if (isNull1 || isNull2) {
                    return false;
                }
                if (ptr1.compareTo(ptr2) != 0) {
                    return false;
                }
            }
            return true;
        }
        
        @Override
        public int hashCode() {
            return hashCode;
        }
        
        Tuple getResult() {
            return result;
        }
    }
    
    protected ResultIterator getDelegate() {
        return delegate;
    }
    
    public DistinctAggregatingResultIterator(AggregatingResultIterator delegate,
            RowProjector rowProjector) {
        this.delegate = delegate;
        this.rowProjector = rowProjector;
    }

    @Override
    public Tuple next() throws SQLException {
        Iterator<ResultEntry> iterator = getResultIterator();
        if (iterator.hasNext()) {
            ResultEntry entry = iterator.next();
            Tuple tuple = entry.getResult();
            aggregate(tuple);
            return tuple;
        }
        resultIterator = Iterators.emptyIterator();
        return null;
    }
    
    private Iterator<ResultEntry> getResultIterator() throws SQLException {
        if (resultIterator != null) {
            return resultIterator;
        }
        
        Set<ResultEntry> entries = Sets.<ResultEntry>newHashSet(); // TODO: size?
        try {
            for (Tuple result = delegate.next(); result != null; result = delegate.next()) {
                ResultEntry entry = new ResultEntry(result);
                entries.add(entry);
            }
        } finally {
            delegate.close();
        }
        
        resultIterator = entries.iterator();
        return resultIterator;
    }

    @Override
    public void close()  {
        resultIterator = Iterators.emptyIterator();
    }


    @Override
    public void explain(List<String> planSteps) {
        delegate.explain(planSteps);
        planSteps.add("CLIENT DISTINCT ON " + rowProjector.toString());
    }

    @Override
    public void aggregate(Tuple result) {
        delegate.aggregate(result);
    }
}
