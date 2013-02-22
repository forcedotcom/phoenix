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
package com.salesforce.phoenix.execute;


import java.sql.SQLException;
import java.util.List;

import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.compile.*;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.aggregator.Aggregators;
import com.salesforce.phoenix.iterate.*;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.schema.TableRef;



/**
 * 
 * Query plan for aggregating queries
 *
 * @author jtaylor
 * @since 0.1
 */
public class AggregatePlan extends BasicQueryPlan {
    private final Aggregators aggregators;
    private final GroupBy groupBy;
    private final Expression having;
    private final int maxRows;
    private List<KeyRange> splits;

    public AggregatePlan(StatementContext context, TableRef table, RowProjector projector, Integer limit,
            GroupBy groupBy, Expression having, OrderBy orderBy, int maxRows) {
        super(context, table, projector, context.getBindManager().getParameterMetaData(), limit, orderBy);
        this.groupBy = groupBy;
        this.having = having;
        this.aggregators = context.getAggregationManager().getAggregators();
        this.maxRows = maxRows;
    }

    @Override
    public boolean isAggregate() {
        return true;
    }
    
    @Override
    public List<KeyRange> getSplits() {
        return splits;
    }
    
    @Override
    protected Scanner newScanner(ConnectionQueryServices services) throws SQLException {
        ResultIterators iterators;
        if (limit == null) {
            ParallelIterators parallelIterators = new ParallelIterators(context, table, RowCounter.UNLIMIT_ROW_COUNTER);
            iterators = parallelIterators;
            splits = parallelIterators.getSplits();
        } else {
            iterators = new SerialLimitingIterators(context, table, limit, new AggregateRowCounter(aggregators));            
        }

        AggregatingResultIterator resultScanner;
        // No need to merge sort for ungrouped aggregation
        if (groupBy.isEmpty()) {
            resultScanner = new UngroupedAggregatingResultIterator(new ConcatResultIterator(iterators), aggregators);
        } else {
            resultScanner = new GroupedAggregatingResultIterator(new MergeSortResultIterator(iterators), aggregators);
        }

        if (having != null) {
            resultScanner = new FilterAggregatingResultIterator(resultScanner, having);
        }
        
        if (!orderBy.getOrderingColumns().isEmpty()) {
            resultScanner = new OrderedAggregatingResultIterator(context, resultScanner, orderBy.getOrderingColumns());
        }

        return new WrappedScanner(resultScanner, getProjector(), maxRows);
    }
}
