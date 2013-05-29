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
package com.salesforce.phoenix.compile;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.*;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import com.salesforce.phoenix.execute.*;
import com.salesforce.phoenix.expression.*;
import com.salesforce.phoenix.expression.function.CountAggregateFunction;
import com.salesforce.phoenix.iterate.ResultIterator;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.query.Scanner;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ImmutableBytesPtr;

public class DeleteCompiler {
    private static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();
    
    private final PhoenixConnection connection;
    
    public DeleteCompiler(PhoenixConnection connection) {
        this.connection = connection;
    }
    
    public MutationPlan compile(DeleteStatement statement, List<Object> binds) throws SQLException {
        final boolean isAutoCommit = connection.getAutoCommit();
        ConnectionQueryServices services = connection.getQueryServices();
        final ColumnResolver resolver = FromCompiler.getResolver(statement, connection);
        final TableRef tableRef = resolver.getTables().get(0);
        if (tableRef.getTable().getType() == PTableType.VIEW) {
            throw new ReadOnlyTableException("Mutations not allowed for a view (" + tableRef.getTable() + ")");
        }
        Scan scan = new Scan();
        ParseNode where = statement.getWhere();
        final StatementContext context = new StatementContext(connection, resolver, binds, statement.getBindCount(), scan, statement.getHint());
        Integer limit = LimitCompiler.getLimit(context, statement.getLimit());
        OrderBy orderBy = OrderByCompiler.getOrderBy(context, statement.getOrderBy(), GroupBy.EMPTY_GROUP_BY, false, limit, Collections.<String,ParseNode>emptyMap()); 
        Expression whereClause = WhereCompiler.getWhereClause(context, where);
        final int maxSize = services.getConfig().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
        
        if (LiteralExpression.TRUE_EXPRESSION.equals(whereClause) && context.getScanRanges().isSingleRowScan()) {
            final ImmutableBytesPtr key = new ImmutableBytesPtr(scan.getStartRow());
            return new MutationPlan() {

                @Override
                public ParameterMetaData getParameterMetaData() {
                    return context.getBindManager().getParameterMetaData();
                }

                @Override
                public MutationState execute() {
                    Map<ImmutableBytesPtr,Map<PColumn,MutationValue>> mutation = Maps.newHashMapWithExpectedSize(1);
                    mutation.put(key, null);
                    return new MutationState(tableRef, mutation, 0, maxSize, connection);
                }

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("DELETE SINGLE ROW"));
                }

                @Override
                public PhoenixConnection getConnection() {
                    return connection;
                }
            };
        } else if (isAutoCommit && limit == null && orderBy.getOrderByExpressions().isEmpty()) {
            // TODO: better abstraction - DeletePlan ?
            scan.setAttribute(UngroupedAggregateRegionObserver.DELETE_AGG, QueryConstants.TRUE);
            // Build an ungrouped aggregate query: select COUNT(*) from <table> where <where>
            // The coprocessor will delete each row returned from the scan
            List<AliasedNode> select = Collections.<AliasedNode>singletonList(
                    NODE_FACTORY.aliasedNode(null, 
                            NODE_FACTORY.function(CountAggregateFunction.NORMALIZED_NAME, LiteralParseNode.STAR)));
            final RowProjector projector = ProjectionCompiler.getRowProjector(context, select, false, GroupBy.EMPTY_GROUP_BY, OrderBy.EMPTY_ORDER_BY, null);
            final QueryPlan plan = new AggregatePlan(context, tableRef, projector, null, GroupBy.EMPTY_GROUP_BY, false, null, OrderBy.EMPTY_ORDER_BY);
            return new MutationPlan() {

                @Override
                public PhoenixConnection getConnection() {
                    return connection;
                }

                @Override
                public ParameterMetaData getParameterMetaData() {
                    return context.getBindManager().getParameterMetaData();
                }

                @Override
                public MutationState execute() throws SQLException {
                    Scanner scanner = plan.getScanner();
                    ResultIterator iterator = scanner.iterator();
                    try {
                        Tuple row = iterator.next();
                        ImmutableBytesWritable ptr = context.getTempPtr();
                        final long mutationCount = (Long)projector.getColumnProjector(0).getValue(row, PDataType.LONG, ptr);
                        return new MutationState(maxSize, connection) {
                            @Override
                            public long getUpdateCount() {
                                return mutationCount;
                            }
                        };
                    } finally {
                        iterator.close();
                    }
                }

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    List<String> queryPlanSteps =  plan.getExplainPlan().getPlanSteps();
                    List<String> planSteps = Lists.newArrayListWithExpectedSize(queryPlanSteps.size()+1);
                    planSteps.add("DELETE ROWS");
                    planSteps.addAll(queryPlanSteps);
                    return new ExplainPlan(planSteps);
                }
            };
        } else {
            final int batchSize = Math.min(connection.getMutateBatchSize(), maxSize);
            List<AliasedNode> select = Collections.<AliasedNode>singletonList(
                    NODE_FACTORY.aliasedNode(null,
                        NODE_FACTORY.literal(1)));
            final RowProjector projector = ProjectionCompiler.getRowProjector(context, select, false, GroupBy.EMPTY_GROUP_BY, OrderBy.EMPTY_ORDER_BY, null);
            final QueryPlan plan = new ScanPlan(context, tableRef, projector, limit, orderBy);
            return new MutationPlan() {

                @Override
                public PhoenixConnection getConnection() {
                    return connection;
                }

                @Override
                public ParameterMetaData getParameterMetaData() {
                    return context.getBindManager().getParameterMetaData();
                }

                @Override
                public MutationState execute() throws SQLException {
                    Scanner scanner = plan.getScanner();
                    ResultIterator iterator = scanner.iterator();
                    int estSize = scanner.getEstimatedSize();
                    Map<ImmutableBytesPtr,Map<PColumn,MutationValue>> mutations = Maps.newHashMapWithExpectedSize(estSize);
                    try {
                        Tuple row;
                        int rowCount = 0;
                        while ((row = iterator.next()) != null) {
                            // Need to create new ptr each time since we're holding on to it
                            ImmutableBytesPtr ptr = new ImmutableBytesPtr();
                            row.getKey(ptr);
                            mutations.put(ptr,null);
                            if (mutations.size() > maxSize) {
                                throw new IllegalArgumentException("MutationState size of " + mutations.size() + " is bigger than max allowed size of " + maxSize);
                            }
                            rowCount++;
                            // Commit a batch if auto commit is true and we're at our batch size
                            if (isAutoCommit && rowCount % batchSize == 0) {
                                MutationState state = new MutationState(tableRef, mutations, 0, maxSize, connection);
                                connection.getMutationState().join(state);
                                connection.commit();
                                mutations.clear();
                            }
                        }
                        // If auto commit is true, this last batch will be committed upon return
                        return new MutationState(tableRef,mutations, rowCount / batchSize * batchSize, maxSize, connection);
                    } finally {
                        iterator.close();
                    }
                }

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    List<String> queryPlanSteps =  plan.getExplainPlan().getPlanSteps();
                    List<String> planSteps = Lists.newArrayListWithExpectedSize(queryPlanSteps.size()+1);
                    planSteps.add("DELETE ROWS");
                    planSteps.addAll(queryPlanSteps);
                    return new ExplainPlan(planSteps);
                }
            };
        }
       
    }
}
