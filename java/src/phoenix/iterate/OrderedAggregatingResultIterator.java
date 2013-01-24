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
import java.util.Iterator;
import java.util.List;

import phoenix.compile.OrderByCompiler.OrderingColumn;
import phoenix.compile.StatementContext;
import phoenix.schema.tuple.Tuple;

/**
 * Result scanner that sorts aggregated rows by columns specified in the ORDER BY clause.
 * <p>
 * Note that currently the sort is entirely done in memory. 
 *  
 * @author syyang
 * @since 0.1
 */
public class OrderedAggregatingResultIterator extends OrderedResultIterator implements AggregatingResultIterator {

    public OrderedAggregatingResultIterator(StatementContext context,
                                AggregatingResultIterator delegate,
                                List<OrderingColumn> orderingColumns) throws SQLException {
        super (context, delegate, orderingColumns);
    }

    @Override
    protected AggregatingResultIterator getDelegate() {
        return (AggregatingResultIterator)super.getDelegate();
    }
    
    @Override
    protected Iterator<Tuple> newIterator(final Iterator<ResultEntry> iterator) {
        return new Iterator<Tuple>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Tuple next() {
                Tuple tuple = iterator.next().getResult();
                aggregate(tuple);
                return tuple;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
            
        };
    }

    @Override
    public void aggregate(Tuple result) {
        getDelegate().aggregate(result);
    }
}
