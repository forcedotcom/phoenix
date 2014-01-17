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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.cache.ServerCacheClient.ServerCache;
import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.execute.AggregatePlan;
import com.salesforce.phoenix.execute.MutationState;
import com.salesforce.phoenix.index.IndexMetaDataCacheClient;
import com.salesforce.phoenix.index.PhoenixIndexCodec;
import com.salesforce.phoenix.iterate.ResultIterator;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.jdbc.PhoenixResultSet;
import com.salesforce.phoenix.jdbc.PhoenixStatement;
import com.salesforce.phoenix.optimize.QueryOptimizer;
import com.salesforce.phoenix.parse.AliasedNode;
import com.salesforce.phoenix.parse.DeleteStatement;
import com.salesforce.phoenix.parse.HintNode;
import com.salesforce.phoenix.parse.HintNode.Hint;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.parse.ParseNodeFactory;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.query.QueryServicesOptions;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.PTableType;
import com.salesforce.phoenix.schema.ReadOnlyTableException;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.IndexUtil;

public class DeleteCompiler {
    private static ParseNodeFactory FACTORY = new ParseNodeFactory();
    
    private final PhoenixStatement statement;
    
    public DeleteCompiler(PhoenixStatement statement) {
        this.statement = statement;
    }
    
    private static MutationState deleteRows(PhoenixStatement statement, TableRef tableRef, ResultIterator iterator, RowProjector projector) throws SQLException {
        PhoenixConnection connection = statement.getConnection();
        final boolean isAutoCommit = connection.getAutoCommit();
        ConnectionQueryServices services = connection.getQueryServices();
        final int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
        final int batchSize = Math.min(connection.getMutateBatchSize(), maxSize);
        Map<ImmutableBytesPtr,Map<PColumn,byte[]>> mutations = Maps.newHashMapWithExpectedSize(batchSize);
        try {
            PTable table = tableRef.getTable();
            List<PColumn> pkColumns = table.getPKColumns();
            int offset = table.getBucketNum() == null ? 0 : 1; // Take into account salting
            byte[][] values = new byte[pkColumns.size()][];
            ResultSet rs = new PhoenixResultSet(iterator, projector, statement);
            int rowCount = 0;
            while (rs.next()) {
                for (int i = offset; i < values.length; i++) {
                    byte[] byteValue = rs.getBytes(i+1-offset);
                    // The ResultSet.getBytes() call will have inverted it - we need to invert it back.
                    // TODO: consider going under the hood and just getting the bytes
                    if (pkColumns.get(i).getColumnModifier() == ColumnModifier.SORT_DESC) {
                        byte[] tempByteValue = Arrays.copyOf(byteValue, byteValue.length);
                        byteValue = ColumnModifier.SORT_DESC.apply(byteValue, 0, tempByteValue, 0, byteValue.length);
                    }
                    values[i] = byteValue;
                }
                ImmutableBytesPtr ptr = new ImmutableBytesPtr();
                table.newKey(ptr, values);
                mutations.put(ptr, null);
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
            return new MutationState(tableRef, mutations, rowCount / batchSize * batchSize, maxSize, connection);
        } finally {
            iterator.close();
        }
    }
    
    private static class DeletingParallelIteratorFactory extends MutatingParallelIteratorFactory {
        private RowProjector projector;
        
        private DeletingParallelIteratorFactory(PhoenixConnection connection, TableRef tableRef) {
            super(connection, tableRef);
        }
        
        @Override
        protected MutationState mutate(PhoenixConnection connection, ResultIterator iterator) throws SQLException {
            PhoenixStatement statement = new PhoenixStatement(connection);
            return deleteRows(statement, tableRef, iterator, projector);
        }
        
        public void setRowProjector(RowProjector projector) {
            this.projector = projector;
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
    
    public MutationPlan compile(DeleteStatement delete) throws SQLException {
        final PhoenixConnection connection = statement.getConnection();
        final boolean isAutoCommit = connection.getAutoCommit();
        final ConnectionQueryServices services = connection.getQueryServices();
        final ColumnResolver resolver = FromCompiler.getResolver(delete, connection);
        final TableRef tableRef = resolver.getTables().get(0);
        PTable table = tableRef.getTable();
        if (table.getType() == PTableType.VIEW && table.getViewType().isReadOnly()) {
            throw new ReadOnlyTableException(table.getSchemaName().getString(),table.getTableName().getString());
        }
        
        final boolean hasLimit = delete.getLimit() != null;
        boolean runOnServer = isAutoCommit && !hasLimit && !hasImmutableIndex(tableRef);
        HintNode hint = delete.getHint();
        if (runOnServer && !delete.getHint().hasHint(Hint.USE_INDEX_OVER_DATA_TABLE)) {
            hint = HintNode.create(hint, Hint.USE_DATA_OVER_INDEX_TABLE);
        }

        List<AliasedNode> aliasedNodes = Lists.newArrayListWithExpectedSize(table.getPKColumns().size());
        for (int i = table.getBucketNum() == null ? 0 : 1; i < table.getPKColumns().size(); i++) {
            PColumn column = table.getPKColumns().get(i);
            String name = column.getName().getString();
            aliasedNodes.add(FACTORY.aliasedNode(null, FACTORY.column(null, name, name)));
        }
        SelectStatement select = FACTORY.select(
                Collections.singletonList(delete.getTable()), 
                hint, false, aliasedNodes, delete.getWhere(), 
                Collections.<ParseNode>emptyList(), null, 
                delete.getOrderBy(), delete.getLimit(),
                delete.getBindCount(), false);
        DeletingParallelIteratorFactory parallelIteratorFactory = hasLimit ? null : new DeletingParallelIteratorFactory(connection, tableRef);
        final QueryPlan plan = new QueryOptimizer(services).optimize(select, statement, Collections.<PColumn>emptyList(), parallelIteratorFactory);
        runOnServer &= plan.getTableRef().equals(tableRef);
        
        final int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
 
        if (hasImmutableIndexWithKeyValueColumns(tableRef)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_DELETE_IF_IMMUTABLE_INDEX).setSchemaName(tableRef.getTable().getSchemaName().getString())
            .setTableName(tableRef.getTable().getTableName().getString()).build().buildException();
        }
        
        final StatementContext context = plan.getContext();
        // If we're doing a query for a single row with no where clause, then we don't need to contact the server at all.
        // A simple check of the none existence of a where clause in the parse node is not sufficient, as the where clause
        // may have been optimized out.
        if (runOnServer && context.isSingleRowScan()) {
            final ImmutableBytesPtr key = new ImmutableBytesPtr(context.getScan().getStartRow());
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
        } else if (runOnServer) {
            // TODO: better abstraction
            Scan scan = context.getScan();
            scan.setAttribute(UngroupedAggregateRegionObserver.DELETE_AGG, QueryConstants.TRUE);

            // Build an ungrouped aggregate query: select COUNT(*) from <table> where <where>
            // The coprocessor will delete each row returned from the scan
            // Ignoring ORDER BY, since with auto commit on and no limit makes no difference
            SelectStatement aggSelect = SelectStatement.create(SelectStatement.COUNT_ONE, delete.getHint());
            final RowProjector projector = ProjectionCompiler.compile(context, aggSelect, GroupBy.EMPTY_GROUP_BY);
            final QueryPlan aggPlan = new AggregatePlan(context, select, tableRef, projector, null, OrderBy.EMPTY_ORDER_BY, null, GroupBy.EMPTY_GROUP_BY, null);
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
                    // TODO: share this block of code with UPSERT SELECT
                    ImmutableBytesWritable ptr = context.getTempPtr();
                    tableRef.getTable().getIndexMaintainers(ptr);
                    ServerCache cache = null;
                    try {
                        if (ptr.getLength() > 0) {
                            IndexMetaDataCacheClient client = new IndexMetaDataCacheClient(connection, tableRef);
                            cache = client.addIndexMetadataCache(context.getScanRanges(), ptr);
                            byte[] uuidValue = cache.getId();
                            context.getScan().setAttribute(PhoenixIndexCodec.INDEX_UUID, uuidValue);
                        }
                        ResultIterator iterator = aggPlan.iterator();
                        try {
                            Tuple row = iterator.next();
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
                    } finally {
                        if (cache != null) {
                            cache.close();
                        }
                    }
                }

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    List<String> queryPlanSteps =  aggPlan.getExplainPlan().getPlanSteps();
                    List<String> planSteps = Lists.newArrayListWithExpectedSize(queryPlanSteps.size()+1);
                    planSteps.add("DELETE ROWS");
                    planSteps.addAll(queryPlanSteps);
                    return new ExplainPlan(planSteps);
                }
            };
        } else {
            if (parallelIteratorFactory != null) {
                parallelIteratorFactory.setRowProjector(plan.getProjector());
            }
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
                    ResultIterator iterator = plan.iterator();
                    if (!hasLimit) {
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
                        return deleteRows(statement, tableRef, iterator, plan.getProjector());
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
