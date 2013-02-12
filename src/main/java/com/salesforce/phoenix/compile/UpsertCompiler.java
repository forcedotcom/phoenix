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

import java.sql.*;
import java.util.*;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.execute.AggregatePlan;
import com.salesforce.phoenix.execute.MutationState;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.expression.function.CountAggregateFunction;
import com.salesforce.phoenix.iterate.ResultIterator;
import com.salesforce.phoenix.jdbc.*;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.query.Scanner;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ImmutableBytesPtr;
import com.salesforce.phoenix.util.SchemaUtil;

public class UpsertCompiler {
    private static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();

    private final PhoenixStatement statement;
    
    public UpsertCompiler(PhoenixStatement statement) {
        this.statement = statement;
    }
    
    private static void setValues(byte[][] values, int[] pkSlotIndex, int[] columnIndexes, PTable table, Map<ImmutableBytesPtr,Map<PColumn,byte[]>> mutation) {
        Map<PColumn,byte[]> columnValues = Maps.newHashMapWithExpectedSize(columnIndexes.length);
        byte[][] pkValues = new byte[table.getPKColumns().size()][];
        for (int i = 0; i < values.length; i++) {
            byte[] value = values[i];
            PColumn column = table.getColumns().get(columnIndexes[i]);
            if (SchemaUtil.isPKColumn(column)) {
                pkValues[pkSlotIndex[i]] = value;
            } else {
                columnValues.put(column, value);
            }
        }
        ImmutableBytesPtr ptr = new ImmutableBytesPtr();
        table.newKey(ptr, pkValues);
        mutation.put(ptr, columnValues);
    }
    
    public MutationPlan compile(UpsertStatement upsert, List<Object> binds) throws SQLException {
        final PhoenixConnection connection = statement.getConnection();
        ConnectionQueryServices services = connection.getQueryServices();
        final int maxSize = services.getConfig().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
        final ColumnResolver resolver = FromCompiler.getResolver(upsert, connection);
        final TableRef tableRef = resolver.getTables().get(0);
        PTable table = tableRef.getTable();
        if (table.getType() == PTableType.VIEW) {
            throw new ReadOnlyTableException("Mutations not allowed for a view (" + tableRef + ")");
        }
        Scan scan = new Scan();
        final StatementContext context = new StatementContext(connection, resolver, binds, upsert.getBindCount(), scan);
        // Setup array of column indexes parallel to values that are going to be set
        List<ParseNode> columnNodes = upsert.getColumns();
        List<PColumn> allColumns = table.getColumns();
        int[] columnIndexesToBe;
        int[] pkSlotIndexesToBe;
        PColumn[] targetColumns;
        if (columnNodes.isEmpty()) {
            columnIndexesToBe = new int[allColumns.size()];
            pkSlotIndexesToBe = new int[columnIndexesToBe.length];
            targetColumns = new PColumn[columnIndexesToBe.length];
            for (int i = 0, j = 0; i < allColumns.size() ; i++) {
                columnIndexesToBe[i] = i;
                targetColumns[i] = allColumns.get(i);
                if (SchemaUtil.isPKColumn(allColumns.get(i))) {
                    pkSlotIndexesToBe[i] = j++;
                }
            }
        } else {
            columnIndexesToBe = new int[columnNodes.size()];
            pkSlotIndexesToBe = new int[columnIndexesToBe.length];
            targetColumns = new PColumn[columnIndexesToBe.length];
            Arrays.fill(columnIndexesToBe, -1); // TODO: necessary? So we'll get an AIOB exception if it's not replaced
            Arrays.fill(pkSlotIndexesToBe, -1); // TODO: necessary? So we'll get an AIOB exception if it's not replaced
            ColumnUpsertCompiler expressionBuilder = new ColumnUpsertCompiler(context, columnIndexesToBe, pkSlotIndexesToBe);
            BitSet pkColumnsSet = new BitSet(table.getPKColumns().size());
            for (int i =0; i < columnNodes.size(); i++) {
                ParseNode colNode = columnNodes.get(i);
                expressionBuilder.setNodeIndex(i);
                colNode.accept(expressionBuilder);
                PColumn col = allColumns.get(columnIndexesToBe[i]);
                targetColumns[i] = col;
                if (SchemaUtil.isPKColumn(col)) {
                    pkColumnsSet.set(pkSlotIndexesToBe[i]);
                }
            }
            for (int i = 0; i < table.getPKColumns().size(); i++) {
                PColumn pkCol = table.getPKColumns().get(i);
                if (!pkColumnsSet.get(i)) {
                    if (!pkCol.isNullable()) {
                        throw new ConstraintViolationException(table.getName().getString() + "." + pkCol.getName().getString() + " may not be null");
                    }
                }
            }
        }
        
        List<ParseNode> valueNodes = upsert.getValues();
        QueryPlan plan = null;
        RowProjector projector = null;
        int nValuesToSet;
        boolean sameTable = false;
        if (valueNodes == null) {
            SelectStatement select = upsert.getSelect();
            assert(select != null);
            TableRef selectTableRef = FromCompiler.getResolver(select, connection).getTables().get(0);
            sameTable = tableRef.equals(selectTableRef);
            // Pass scan through if same table in upsert and select so that projection is computed correctly
            QueryCompiler compiler = new QueryCompiler(connection, 0, sameTable? scan : new Scan(), targetColumns);
            plan = compiler.compile(select, binds);
            projector = plan.getProjector();
            nValuesToSet = projector.getColumnCount();
        } else {
            nValuesToSet = valueNodes.size();
        }
        final QueryPlan queryPlan = plan;
        // Resize down to allow a subset of columns to be specifiable
        if (columnNodes.isEmpty()) {
            columnIndexesToBe = Arrays.copyOf(columnIndexesToBe, nValuesToSet);
            pkSlotIndexesToBe = Arrays.copyOf(pkSlotIndexesToBe, nValuesToSet);
        }
        
        if (nValuesToSet != columnIndexesToBe.length) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.UPSERT_COLUMN_NUMBERS_MISMATCH)
                .setMessage("Numbers of columns: " + columnIndexesToBe.length + ". Number of values: " + nValuesToSet)
                .build().buildException();
        }
        
        final int[] columnIndexes = columnIndexesToBe;
        final int[] pkSlotIndexes = pkSlotIndexesToBe;
        if (valueNodes == null) { // UPSERT SELECT
            /* We can run the upsert in a coprocessor if:
             * 1) the into table matches from table
             * 2) the select query isn't doing aggregation
             * 3) autoCommit is on
             * Otherwise, run the query to pull the data from the server
             * and populate the MutationState (upto a limit).
            */
            final boolean isAutoCommit = connection.getAutoCommit();
            if (isAutoCommit && !plan.isAggregate() && sameTable) { // UPSERT SELECT run server-side
                // At most this array will grow bigger my the number of PK columns
                int[] allColumnsIndexes = Arrays.copyOf(columnIndexes, columnIndexes.length + nValuesToSet);
                int[] reverseColumnIndexes = new int[table.getColumns().size()];
                List<Expression> projectedExpressions = Lists.newArrayListWithExpectedSize(reverseColumnIndexes.length);
                Arrays.fill(reverseColumnIndexes, -1);
                for (int i =0; i < nValuesToSet; i++) {
                    projectedExpressions.add(projector.getColumnProjector(i).getExpression());
                    reverseColumnIndexes[columnIndexes[i]] = i;
                }
                /*
                 * Order projected columns and projected expressions with PK columns
                 * leading order by slot position
                 */
                for (int i = 0; i < table.getPKColumns().size(); i++) {
                    PColumn column = table.getPKColumns().get(i);
                    int pos = reverseColumnIndexes[column.getPosition()];
                    if (pos == -1) {
                        // Last PK column may be fixed width and nullable
                        // We don't want to insert a null expression b/c
                        // it's not valid to set a fixed width type to null.
                        if (column.getDataType().isFixedWidth()) {
                            continue;
                        }
                        // Add literal null for missing PK columns
                        pos = projectedExpressions.size();
                        Expression literalNull = LiteralExpression.newConstant(null, column.getDataType());
                        projectedExpressions.add(literalNull);
                        allColumnsIndexes[pos] = column.getPosition();
                    } 
                    // Swap select expression at pos with i
                    Collections.swap(projectedExpressions, i, pos);
                    // Swap column indexes and reverse column indexes too
                    int tempPos = allColumnsIndexes[i];
                    allColumnsIndexes[i] = allColumnsIndexes[pos];
                    allColumnsIndexes[pos] = tempPos;
                    reverseColumnIndexes[tempPos] = reverseColumnIndexes[i];
                    reverseColumnIndexes[i] = i;
                }
                // Iterate through columns being projected
                List<PColumn> projectedColumns = Lists.newArrayListWithExpectedSize(projectedExpressions.size());
                for (int i = 0; i < projectedExpressions.size(); i++) {
                    // Must make new column if position has changed
                    PColumn column = allColumns.get(allColumnsIndexes[i]);
                    projectedColumns.add(column.getPosition() == i ? column : new PColumnImpl(column, i));
                }
                // Build table from projectedColumns
                PTable projectedTable = new PTableImpl(table.getName(), table.getType(), table.getTimeStamp(), table.getSequenceNumber(), table.getPKName(), projectedColumns);
                
                List<AliasedParseNode> select = Collections.<AliasedParseNode>singletonList(
                        NODE_FACTORY.aliasedNode(null, 
                                NODE_FACTORY.function(CountAggregateFunction.NORMALIZED_NAME, LiteralParseNode.STAR)));
                // Ignore order by - it has no impact
                final RowProjector aggProjector = ProjectionCompiler.getRowProjector(context, select, GroupBy.EMPTY_GROUP_BY, OrderBy.EMPTY_ORDER_BY, null);
                /*
                 * Transfer over PTable representing subset of columns selected, but all PK columns.
                 * Move columns setting PK first in pkSlot order, adding LiteralExpression of null for any missing ones.
                 * Transfer over List<Expression> for projection.
                 * In region scan, evaluate expressions in order, collecting first n columns for PK and collection non PK in mutation Map
                 * Create the PRow and get the mutations, adding them to the batch
                 */
                scan.setAttribute(UngroupedAggregateRegionObserver.UPSERT_SELECT_TABLE, UngroupedAggregateRegionObserver.serialize(projectedTable));
                scan.setAttribute(UngroupedAggregateRegionObserver.UPSERT_SELECT_EXPRS, UngroupedAggregateRegionObserver.serialize(projectedExpressions));
                final QueryPlan aggPlan = new AggregatePlan(context, tableRef, projector, plan.getLimit(), GroupBy.EMPTY_GROUP_BY, null, OrderBy.EMPTY_ORDER_BY, 0);
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
                        Scanner scanner = aggPlan.getScanner();
                        ResultIterator iterator = scanner.iterator();
                        try {
                            Tuple row = iterator.next();
                            ImmutableBytesWritable ptr = context.getTempPtr();
                            final long mutationCount = (Long)aggProjector.getColumnProjector(0).getValue(row, PDataType.LONG, ptr);
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
                        List<String> queryPlanSteps =  aggPlan.getExplainPlan().getPlanSteps();
                        List<String> planSteps = Lists.newArrayListWithExpectedSize(queryPlanSteps.size()+1);
                        planSteps.add("UPSERT ROWS");
                        planSteps.addAll(queryPlanSteps);
                        return new ExplainPlan(planSteps);
                    }
                };
            } else { // UPSERT SELECT run client-side
                final int batchSize = Math.min(connection.getUpsertBatchSize(), maxSize);
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
                        byte[][] values = new byte[columnIndexes.length][];
                        Scanner scanner = queryPlan.getScanner();
                        int estSize = scanner.getEstimatedSize();
                        int rowCount = 0;
                        Map<ImmutableBytesPtr,Map<PColumn,byte[]>> mutation = Maps.newHashMapWithExpectedSize(estSize);
                        ResultSet rs = new PhoenixResultSet(scanner, statement);
                        while (rs.next()) {
                            for (int i = 0; i < values.length; i++) {
                                values[i] = rs.getBytes(i+1);
                            }
                            setValues(values, pkSlotIndexes, columnIndexes, tableRef.getTable(), mutation);
                            rowCount++;
                            // Commit a batch if auto commit is true and we're at our batch size
                            if (isAutoCommit && rowCount % batchSize == 0) {
                                MutationState state = new MutationState(tableRef, mutation, 0, maxSize, connection);
                                connection.getMutationState().join(state);
                                connection.commit();
                                mutation.clear();
                            }
                        }
                        // If auto commit is true, this last batch will be committed upon return
                        return new MutationState(tableRef, mutation, rowCount / batchSize * batchSize, maxSize, connection);
                    }
        
                    @Override
                    public ExplainPlan getExplainPlan() throws SQLException {
                        List<String> queryPlanSteps =  queryPlan.getExplainPlan().getPlanSteps();
                        List<String> planSteps = Lists.newArrayListWithExpectedSize(queryPlanSteps.size()+1);
                        planSteps.add("UPSERT SELECT");
                        planSteps.addAll(queryPlanSteps);
                        return new ExplainPlan(planSteps);
                    }
                    
                };
            } 
        } else { // UPSERT VALUES
            int nodeIndex = 0;
            // Allocate array based on size of all columns in table,
            // since some values may not be set (if they're nullable).
            UpsertValuesCompiler expressionBuilder = new UpsertValuesCompiler(context);
            final byte[][] values = new byte[nValuesToSet][];
            for (ParseNode valueNode : valueNodes) {
                if (!valueNode.isConstant()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.VALUE_IN_UPSERT_NOT_CONSTANT).build().buildException();
                }
                PColumn column = allColumns.get(columnIndexes[nodeIndex]);
                expressionBuilder.setColumn(column);
                LiteralExpression literalExpression = (LiteralExpression)valueNode.accept(expressionBuilder);
                if (literalExpression.getDataType() != null && !literalExpression.getDataType().isCoercibleTo(column.getDataType(), literalExpression.getValue(), literalExpression.getBytes())) {
                    throw new TypeMismatchException(literalExpression.getDataType(), column.getDataType(), "expression: " + literalExpression.toString() + " in column " + column);
                }
                byte[] byteValue = column.getDataType().coerceBytes(literalExpression.getBytes(), literalExpression.getValue(), literalExpression.getDataType());
                values[nodeIndex] = byteValue;
                nodeIndex++;
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
                public MutationState execute() {
                    Map<ImmutableBytesPtr,Map<PColumn,byte[]>> mutation = Maps.newHashMapWithExpectedSize(1);           
                    setValues(values, pkSlotIndexes, columnIndexes, tableRef.getTable(), mutation);
                    return new MutationState(tableRef, mutation, 0, maxSize, connection);
                }
    
                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("PUT SINGLE ROW"));
                }
                
            };
        }
    }
    
    private static final class ColumnUpsertCompiler extends ExpressionCompiler {
        private final int[] columnIndex;
        private final int[] pkSlotIndex;
        private int nodeIndex;
        
        private ColumnUpsertCompiler(StatementContext context, int[] columnIndex, int[] pkSlotIndex) {
            super(context);
            this.columnIndex = columnIndex;
            this.pkSlotIndex = pkSlotIndex;
        }

        public void setNodeIndex(int nodeIndex) {
            this.nodeIndex = nodeIndex;
        }
        
        @Override
        protected ColumnRef resolveColumn(ColumnParseNode node) throws SQLException {
            ColumnRef ref = super.resolveColumn(node);
            this.columnIndex[this.nodeIndex] = ref.getColumnPosition();
            if (SchemaUtil.isPKColumn(ref.getColumn())) {
                pkSlotIndex[this.nodeIndex] = ref.getPKSlotPosition();
            }
            return ref;
        }
    }
    
    private static final class UpsertValuesCompiler extends ExpressionCompiler {
        private PColumn column;
        
        private UpsertValuesCompiler(StatementContext context) {
            super(context);
        }

        public void setColumn(PColumn column) {
            this.column = column;
        }
        
        @Override
        public Expression visit(BindParseNode node) throws SQLException {
            if (isTopLevel()) {
                context.getBindManager().addParamMetaData(node, column);
                Object value = context.getBindManager().getBindValue(node);
                return LiteralExpression.newConstant(value, column.getDataType());
            }
            return super.visit(node);
        }    
        
        @Override
        public Expression visit(LiteralParseNode node) throws SQLException {
            if (isTopLevel()) {
                return LiteralExpression.newConstant(node.getValue(), column.getDataType());
            }
            return super.visit(node);
        }
    }
}
