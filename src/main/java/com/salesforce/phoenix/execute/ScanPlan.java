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

import com.google.common.collect.Lists;
import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.compile.*;
import com.salesforce.phoenix.coprocessor.ScanRegionObserver;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.OrderByExpression;
import com.salesforce.phoenix.iterate.*;
import com.salesforce.phoenix.parse.HintNode.Hint;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.schema.*;



/**
 * 
 * Query plan for a basic table scan
 *
 * @author jtaylor
 * @since 0.1
 */
public class ScanPlan extends BasicQueryPlan {
    private List<KeyRange> splits;
    
    public ScanPlan(StatementContext context, TableRef table, RowProjector projector, Integer limit, OrderBy orderBy) {
        super(context, table, projector, context.getBindManager().getParameterMetaData(), limit, orderBy);
        if (limit != null && !orderBy.getOrderByExpressions().isEmpty() && !context.hasHint(Hint.NO_INTRA_REGION_PARALLELIZATION)) { // TopN
            ScanRegionObserver.serializeIntoScan(context.getScan(), limit, orderBy.getOrderByExpressions(), projector.getEstimatedByteSize());
        }
    }
    
    @Override
    public List<KeyRange> getSplits() {
        return splits;
    }
    
    @Override
    public boolean isAggregate() {
        return false;
    }
    
    @Override
    protected Scanner newScanner(ConnectionQueryServices services) throws SQLException {
        // Set any scan attributes before creating the scanner, as it will be too late afterwards
        context.getScan().setAttribute(ScanRegionObserver.NON_AGGREGATE_QUERY, QueryConstants.TRUE);
        ResultIterator scanner;
        TableRef tableRef = this.getTable();
        PTable table = tableRef.getTable();
        boolean isSalted = table.getBucketNum() != null;
        /* If no limit or topN, use parallel iterator so that we get results faster. Otherwise, if
         * limit is provided, run query serially.
         */
        if (limit == null || !orderBy.getOrderByExpressions().isEmpty()) {
            ParallelIterators iterators = new ParallelIterators(context, tableRef, RowCounter.UNLIMIT_ROW_COUNTER, GroupBy.EMPTY_GROUP_BY);
            splits = iterators.getSplits();
            if (orderBy.getOrderByExpressions().isEmpty()) {
                if (isSalted && 
                        services.getConfig().getBoolean(
                                QueryServices.ROW_KEY_ORDER_SALTED_TABLE_ATTRIB, 
                                QueryServicesOptions.DEFAULT_ROW_KEY_ORDER_SALTED_TABLE)) {
                    scanner = new MergeSortRowKeyResultIterator(iterators, SaltingUtil.NUM_SALTING_BYTES);
                } else {
                    scanner = new ConcatResultIterator(iterators);
                }
            } else {
                // If we expect to have a small amount of data in a single region
                // do the sort on the client side
                if (context.hasHint(Hint.NO_INTRA_REGION_PARALLELIZATION)) {
                    scanner = new ConcatResultIterator(iterators);
                    scanner = new OrderedResultIterator(scanner, orderBy.getOrderByExpressions(), limit);
                } else {
                    scanner = new MergeSortTopNResultIterator(iterators, limit, orderBy.getOrderByExpressions());
                }
            }
        } else {
            scanner = new TableResultIterator(context, tableRef);
            scanner = new SerialLimitingResultIterator(scanner, limit, new ScanRowCounter());
            // If the table is salted and you want the results in pk order, we have to do an explicit
            // in-memory sort after the scan, since otherwise they'll be scrambled.
            if (isSalted && 
                    services.getConfig().getBoolean(
                            QueryServices.ROW_KEY_ORDER_SALTED_TABLE_ATTRIB, 
                            QueryServicesOptions.DEFAULT_ROW_KEY_ORDER_SALTED_TABLE)) {
                List<PColumn> pkColumns = table.getPKColumns();
                // Create ORDER BY for PK columns
                List<OrderByExpression> orderByExpressions = Lists.newArrayListWithExpectedSize(pkColumns.size());
                for (PColumn pkColumn : pkColumns) {
                    Expression expression = new ColumnRef(tableRef, pkColumn.getPosition()).newColumnExpression();
                    orderByExpressions.add(new OrderByExpression(expression, false, pkColumn.getColumnModifier() == null));
                }
                // Add step for client side order by
                scanner = new OrderedResultIterator(scanner, orderByExpressions, limit);
            }
            splits = null;
        }

        return new WrappedScanner(scanner, getProjector());
    }
}
