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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.execute.AggregatePlan;
import com.salesforce.phoenix.execute.MutationState;
import com.salesforce.phoenix.execute.ScanPlan;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.iterate.ParallelIterators.ParallelIteratorFactory;
import com.salesforce.phoenix.iterate.ResultIterator;
import com.salesforce.phoenix.iterate.SpoolingResultIterator.SpoolingResultIteratorFactory;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.DeleteStatement;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.query.QueryServicesOptions;
import com.salesforce.phoenix.query.Scanner;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.PTableType;
import com.salesforce.phoenix.schema.ReadOnlyTableException;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.IndexUtil;

public class DeleteCompiler {
    private final PhoenixConnection connection;
    
    public DeleteCompiler(PhoenixConnection connection) {
        this.connection = connection;
    }
    
    private static MutationState deleteRows(PhoenixConnection connection, TableRef tableRef, ResultIterator iterator) throws SQLException {
        final boolean isAutoCommit = connection.getAutoCommit();
        ConnectionQueryServices services = connection.getQueryServices();
        final int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
        final int batchSize = Math.min(connection.getMutateBatchSize(), maxSize);
        Map<ImmutableBytesPtr,Map<PColumn,byte[]>> mutations = Maps.newHashMapWithExpectedSize(batchSize);
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
    
    private static class DeletingParallelIteratorFactory extends MutatingParallelIteratorFactory {
        
        private DeletingParallelIteratorFactory(PhoenixConnection connection, TableRef tableRef) {
            super(connection, tableRef);
        }
        
        @Override
        protected MutationState mutate(PhoenixConnection connection, ResultIterator iterator) throws SQLException {
            return deleteRows(connection, tableRef, iterator);
        }
        
    }
    
    private boolean hasImmutableIndex(TableRef tableRef) {
        return tableRef.getTable().isImmutableRows() && !tableRef.getTable().getIndexes().isEmpty();
    }
    
    private boolean hasImmutableIndexWithKeyValueColumns(TableRef tableRef) {
        if (!hasImmutableIndex(tableRef)) {
            return false;
        }
        for (PTable index : tableRef.getTable().getIndexes()) {
            for (PColumn column : index.getPKColumns()) {
                if (!IndexUtil.isDataPKColumn(column)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    public MutationPlan compile(DeleteStatement statement, List<Object> binds) throws SQLException {
        final boolean isAutoCommit = connection.getAutoCommit();
        final ConnectionQueryServices services = connection.getQueryServices();
        final ColumnResolver resolver = FromCompiler.getResolver(statement, connection);
        final TableRef tableRef = resolver.getTables().get(0);
        if (tableRef.getTable().getType() == PTableType.VIEW) {
            throw new ReadOnlyTableException("Mutations not allowed for a view (" + tableRef.getTable() + ")");
        }
        Scan scan = new Scan();
        final StatementContext context = new StatementContext(statement, connection, resolver, binds, scan);
        final Integer limit = LimitCompiler.compile(context, statement);
        final OrderBy orderBy = OrderByCompiler.compile(context, statement, GroupBy.EMPTY_GROUP_BY, limit); 
        Expression whereClause = WhereCompiler.compile(context, statement);
        final int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
 
        if (hasImmutableIndexWithKeyValueColumns(tableRef)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_DELETE_IF_IMMUTABLE_INDEX).setSchemaName(tableRef.getTable().getSchemaName().getString())
            .setTableName(tableRef.getTable().getTableName().getString()).build().buildException();
        }
        
        if (LiteralExpression.TRUE_EXPRESSION.equals(whereClause) && context.getScanRanges().isSingleRowScan()) {
            final ImmutableBytesPtr key = new ImmutableBytesPtr(scan.getStartRow());
            return new MutationPlan() {

                @Override
                public ParameterMetaData getParameterMetaData() {
                    return context.getBindManager().getParameterMetaData();
                }

                @Override
                public MutationState execute() {
                    Map<ImmutableBytesPtr,Map<PColumn,byte[]>> mutation = Maps.newHashMapWithExpectedSize(1);
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
        } else if (isAutoCommit && limit == null && !hasImmutableIndex(tableRef)) {
            // TODO: better abstraction - DeletePlan ?
            scan.setAttribute(UngroupedAggregateRegionObserver.DELETE_AGG, QueryConstants.TRUE);
            // Build an ungrouped aggregate query: select COUNT(*) from <table> where <where>
            // The coprocessor will delete each row returned from the scan
            // Ignoring ORDER BY, since with auto commit on and no limit makes no difference
            SelectStatement select = SelectStatement.create(SelectStatement.COUNT_ONE, statement.getHint());
            final RowProjector projector = ProjectionCompiler.compile(context, select, GroupBy.EMPTY_GROUP_BY);
            final QueryPlan plan = new AggregatePlan(context, select, tableRef, projector, null, OrderBy.EMPTY_ORDER_BY, null, GroupBy.EMPTY_GROUP_BY, null);
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
            final RowProjector projector = ProjectionCompiler.compile(context, SelectStatement.SELECT_ONE, GroupBy.EMPTY_GROUP_BY);
            // If there's no post processing (i.e. no limit), then we can issue the deletes in parallel as we get results back for the scan
            // Otherwise, we need to buffer the results and process afterwards.
            ParallelIteratorFactory parallelIteratorFactory = limit == null ? new DeletingParallelIteratorFactory(connection, tableRef) : new SpoolingResultIteratorFactory(services);
            final QueryPlan plan = new ScanPlan(context, statement, tableRef, projector, limit, orderBy, parallelIteratorFactory);
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
                    if (limit == null) {
                        Tuple tuple;
                        long totalRowCount = 0;
                        while ((tuple=iterator.next()) != null) {// Runs query
                            KeyValue kv = tuple.getValue(0);
                            totalRowCount += PDataType.LONG.getCodec().decodeLong(kv.getBuffer(), kv.getValueOffset(), null);
                        }
                        // Return total number of rows that have been delete. In the case of auto commit being off
                        // the mutations will all be in the mutation state of the current connection.
                        return new MutationState(maxSize, connection, totalRowCount);
                    } else {
                        return deleteRows(connection, tableRef, iterator);
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
