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

import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.compile.*;
import com.salesforce.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.aggregator.Aggregators;
import com.salesforce.phoenix.iterate.*;
import com.salesforce.phoenix.iterate.ParallelIterators.ParallelIteratorFactory;
import com.salesforce.phoenix.parse.FilterableStatement;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.SchemaUtil;



/**
 *
 * Query plan for aggregating queries
 *
 * @author jtaylor
 * @since 0.1
 */
public class AggregatePlan extends BasicQueryPlan {
    private final Aggregators aggregators;
    private final Expression having;
    private List<KeyRange> splits;

    public AggregatePlan(
            StatementContext context, FilterableStatement statement, TableRef table, RowProjector projector,
            Integer limit, OrderBy orderBy, ParallelIteratorFactory parallelIteratorFactory, GroupBy groupBy,
            Expression having) {
        super(context, statement, table, projector, context.getBindManager().getParameterMetaData(), limit, orderBy, groupBy, parallelIteratorFactory);
        this.having = having;
        this.aggregators = context.getAggregationManager().getAggregators();
    }

    @Override
    public List<KeyRange> getSplits() {
        return splits;
    }

    @Override
    protected Scanner newScanner(ConnectionQueryServices services) throws SQLException {
        // Hack to set state on scan to make upgrade happen
        int upgradeColumnCount = SchemaUtil.upgradeColumnCount(context.getConnection().getURL(),context.getConnection().getClientInfo());
        if (upgradeColumnCount > 0) {
            context.getScan().setAttribute(SchemaUtil.UPGRADE_TO_2_0, Bytes.toBytes(upgradeColumnCount));
        }
        if (groupBy.isEmpty()) {
            UngroupedAggregateRegionObserver.serializeIntoScan(context.getScan());
        }
        ParallelIterators parallelIterators = new ParallelIterators(context, tableRef, statement, projection, groupBy, null, parallelIteratorFactory);
        splits = parallelIterators.getSplits();

        AggregatingResultIterator aggResultIterator;
        // No need to merge sort for ungrouped aggregation
        if (groupBy.isEmpty()) {
            aggResultIterator = new UngroupedAggregatingResultIterator(new ConcatResultIterator(parallelIterators), aggregators);
        } else {
            aggResultIterator = new GroupedAggregatingResultIterator(new MergeSortRowKeyResultIterator(parallelIterators), aggregators);
        }

        if (having != null) {
            aggResultIterator = new FilterAggregatingResultIterator(aggResultIterator, having);
        }
        
        if (statement.isDistinct() && statement.isAggregate()) { // Dedup on client if select distinct and aggregation
            aggResultIterator = new DistinctAggregatingResultIterator(aggResultIterator, getProjector());
        }

        ResultIterator resultScanner = aggResultIterator;
        if (orderBy.getOrderByExpressions().isEmpty()) {
            if (limit != null) {
                resultScanner = new LimitingResultIterator(aggResultIterator, limit);
            }
        } else {
            int thresholdBytes = services.getProps().getInt(QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, 
                    QueryServicesOptions.DEFAULT_SPOOL_THRESHOLD_BYTES);
            resultScanner = new OrderedAggregatingResultIterator(aggResultIterator, orderBy.getOrderByExpressions(), thresholdBytes, limit);
        }
        
        return new WrappedScanner(resultScanner, getProjector());
    }
}
