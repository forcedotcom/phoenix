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

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;

import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.JoinCompiler.JoinSpec;
import com.salesforce.phoenix.compile.JoinCompiler.JoinTable;
import com.salesforce.phoenix.compile.JoinCompiler.JoinedTableColumnResolver;
import com.salesforce.phoenix.compile.JoinCompiler.PTableWrapper;
import com.salesforce.phoenix.compile.JoinCompiler.ProjectedPTableWrapper;
import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.execute.AggregatePlan;
import com.salesforce.phoenix.execute.BasicQueryPlan;
import com.salesforce.phoenix.execute.DegenerateQueryPlan;
import com.salesforce.phoenix.execute.HashJoinPlan;
import com.salesforce.phoenix.execute.ScanPlan;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.iterate.ParallelIterators.ParallelIteratorFactory;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData;
import com.salesforce.phoenix.jdbc.PhoenixStatement;
import com.salesforce.phoenix.join.HashCacheClient;
import com.salesforce.phoenix.join.HashJoinInfo;
import com.salesforce.phoenix.join.ScanProjector;
import com.salesforce.phoenix.parse.JoinTableNode.JoinType;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.AmbiguousColumnException;
import com.salesforce.phoenix.schema.ColumnNotFoundException;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PIndexState;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.PTableType;
import com.salesforce.phoenix.schema.TableRef;



/**
 * 
 * Class used to build an executable query plan
 *
 * @author jtaylor
 * @since 0.1
 */
public class QueryCompiler {
    /* 
     * Not using Scan.setLoadColumnFamiliesOnDemand(true) because we don't 
     * want to introduce a dependency on 0.94.5 (where this feature was
     * introduced). This will do the same thing. Once we do have a 
     * dependency on 0.94.5 or above, switch this around.
     */
    private static final String LOAD_COLUMN_FAMILIES_ON_DEMAND_ATTR = "_ondemand_";
    private final PhoenixStatement statement;
    private final Scan scan;
    private final Scan scanCopy;
    private final List<PColumn> targetColumns;
    private final ParallelIteratorFactory parallelIteratorFactory;

    public QueryCompiler(PhoenixStatement statement) throws SQLException {
        this(statement, Collections.<PColumn>emptyList(), null);
    }
    
    public QueryCompiler(PhoenixStatement statement, List<PColumn> targetColumns, ParallelIteratorFactory parallelIteratorFactory) throws SQLException {
        this.statement = statement;
        this.scan = new Scan();
        this.targetColumns = targetColumns;
        this.parallelIteratorFactory = parallelIteratorFactory;
        if (statement.getConnection().getQueryServices().getLowestClusterHBaseVersion() >= PhoenixDatabaseMetaData.ESSENTIAL_FAMILY_VERSION_THRESHOLD) {
            this.scan.setAttribute(LOAD_COLUMN_FAMILIES_ON_DEMAND_ATTR, QueryConstants.TRUE);
        }
        try {
            this.scanCopy = new Scan(scan);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Builds an executable query plan from a parsed SQL statement
     * @param select parsed SQL statement
     * @return executable query plan
     * @throws SQLException if mismatched types are found, bind value do not match binds,
     * or invalid function arguments are encountered.
     * @throws SQLFeatureNotSupportedException if an unsupported construct is encountered
     * @throws TableNotFoundException if table name not found in schema
     * @throws ColumnNotFoundException if column name could not be resolved
     * @throws AmbiguousColumnException if an unaliased column name is ambiguous across multiple tables
     */
    public QueryPlan compile(SelectStatement select) throws SQLException{
        return compile(select, scan, false);
    }
    
    protected QueryPlan compile(SelectStatement select, Scan scan, boolean asSubquery) throws SQLException{        
        PhoenixConnection connection = statement.getConnection();
        List<Object> binds = statement.getParameters();
        ColumnResolver resolver = FromCompiler.getMultiTableResolver(select, connection);
        select = StatementNormalizer.normalize(select, resolver);
        
        if (select.getFrom().size() == 1) {
            StatementContext context = new StatementContext(select, connection, resolver, binds, scan);
            return compileSingleQuery(context, select, binds);
        }
        
        StatementContext context = new StatementContext(select, connection, resolver, binds, scan, new HashCacheClient(connection));
        JoinSpec join = JoinCompiler.getJoinSpec(context, select);
        return compileJoinQuery(context, select, binds, join, asSubquery);
    }
    
    @SuppressWarnings("unchecked")
    protected QueryPlan compileJoinQuery(StatementContext context, SelectStatement select, List<Object> binds, JoinSpec join, boolean asSubquery) throws SQLException {
    	PhoenixConnection connection = statement.getConnection();
    	byte[] emptyByteArray = new byte[0];
        List<JoinTable> joinTables = join.getJoinTables();
        if (joinTables.isEmpty()) {
            ProjectedPTableWrapper projectedTable = join.createProjectedTable(join.getMainTable(), !asSubquery);
            ScanProjector.serializeProjectorIntoScan(context.getScan(), JoinCompiler.getScanProjector(projectedTable));
            context.setCurrentTable(join.getMainTable());
            context.setResolver(JoinCompiler.getColumnResolver(projectedTable));
            join.projectColumns(context.getScan(), join.getMainTable());
            return compileSingleQuery(context, select, binds);
        }
        
        boolean[] starJoinVector = JoinCompiler.getStarJoinVector(join);
        if (starJoinVector != null) {
        	ProjectedPTableWrapper initialProjectedTable = join.createProjectedTable(join.getMainTable(), !asSubquery);
        	PTableWrapper projectedTable = initialProjectedTable;
            int count = joinTables.size();
            ImmutableBytesPtr[] joinIds = new ImmutableBytesPtr[count];
            List<Expression>[] joinExpressions = new List[count];
            List<Expression>[] hashExpressions = new List[count];
            JoinType[] joinTypes = new JoinType[count];
            PTable[] tables = new PTable[count];
            int[] fieldPositions = new int[count];
            QueryPlan[] joinPlans = new QueryPlan[count];
            fieldPositions[0] = projectedTable.getTable().getColumns().size() - projectedTable.getTable().getPKColumns().size();
            for (int i = 0; i < count; i++) {
                JoinTable joinTable = joinTables.get(i);
                SelectStatement subStatement = joinTable.getAsSubquery();
                if (subStatement.getFrom().size() > 1)
                	throw new SQLFeatureNotSupportedException("Sub queries not supported.");
                ProjectedPTableWrapper subProjTable = join.createProjectedTable(joinTable.getTable(), false);
                tables[i] = subProjTable.getTable();
                ColumnResolver resolver = JoinCompiler.getColumnResolver(subProjTable);
                try {
                    Scan subScan = new Scan(scanCopy);
                    ScanProjector.serializeProjectorIntoScan(subScan, JoinCompiler.getScanProjector(subProjTable));
                    StatementContext subContext = new StatementContext(subStatement, connection, resolver, binds, subScan);
                    subContext.setCurrentTable(joinTable.getTable());
                    join.projectColumns(subScan, joinTable.getTable());
                    joinPlans[i] = compileSingleQuery(subContext, subStatement, binds);
                } catch (IOException e) {
                    throw new SQLException(e);
                }
                projectedTable = JoinCompiler.mergeProjectedTables(projectedTable, subProjTable, joinTable.getType() == JoinType.Inner);
                ColumnResolver leftResolver = JoinCompiler.getColumnResolver(starJoinVector[i] ? initialProjectedTable : projectedTable);
                joinIds[i] = new ImmutableBytesPtr(emptyByteArray); // place-holder
                Pair<List<Expression>, List<Expression>> joinConditions = joinTable.compileJoinConditions(context, leftResolver, resolver);
                joinExpressions[i] = joinConditions.getFirst();
                hashExpressions[i] = joinConditions.getSecond();
                joinTypes[i] = joinTable.getType();
                if (i < count - 1) {
                	fieldPositions[i + 1] = fieldPositions[i] + (tables[i].getColumns().size() - tables[i].getPKColumns().size());
                }
            }
            ScanProjector.serializeProjectorIntoScan(context.getScan(), JoinCompiler.getScanProjector(initialProjectedTable));
            context.setCurrentTable(join.getMainTable());
            context.setResolver(JoinCompiler.getColumnResolver(projectedTable));
            join.projectColumns(context.getScan(), join.getMainTable());
            BasicQueryPlan plan = compileSingleQuery(context, JoinCompiler.getSubqueryWithoutJoin(select, join), binds);
            Expression postJoinFilterExpression = join.compilePostFilterExpression(context);
            HashJoinInfo joinInfo = new HashJoinInfo(projectedTable.getTable(), joinIds, joinExpressions, joinTypes, starJoinVector, tables, fieldPositions, postJoinFilterExpression);
            return new HashJoinPlan(plan, joinInfo, hashExpressions, joinPlans);
        }
        
        JoinTable lastJoinTable = joinTables.get(joinTables.size() - 1);
        JoinType type = lastJoinTable.getType();
        if (type == JoinType.Full)
            throw new SQLFeatureNotSupportedException("Full joins not supported.");
        
        if (type == JoinType.Right || type == JoinType.Inner) {
            SelectStatement lhs = JoinCompiler.getSubQueryWithoutLastJoin(select, join);
            SelectStatement rhs = JoinCompiler.getSubqueryForLastJoinTable(select, join);
            JoinSpec lhsJoin = JoinCompiler.getSubJoinSpecWithoutPostFilters(join);
            Scan subScan;
            try {
                subScan = new Scan(scanCopy);
            } catch (IOException e) {
                throw new SQLException(e);
            }
            StatementContext lhsCtx = new StatementContext(select, connection, context.getResolver(), binds, subScan, context.getHashClient());
            QueryPlan lhsPlan = compileJoinQuery(lhsCtx, lhs, binds, lhsJoin, true);
            ColumnResolver lhsResolver = lhsCtx.getResolver();
            PTableWrapper lhsProjTable = ((JoinedTableColumnResolver) (lhsResolver)).getPTableWrapper();
            ProjectedPTableWrapper rhsProjTable = join.createProjectedTable(lastJoinTable.getTable(), !asSubquery);
            ColumnResolver rhsResolver = JoinCompiler.getColumnResolver(rhsProjTable);
            ImmutableBytesPtr[] joinIds = new ImmutableBytesPtr[] {new ImmutableBytesPtr(emptyByteArray)};
            Pair<List<Expression>, List<Expression>> joinConditions = lastJoinTable.compileJoinConditions(context, lhsResolver, rhsResolver);
            List<Expression> joinExpressions = joinConditions.getSecond();
            List<Expression> hashExpressions = joinConditions.getFirst();
            int fieldPosition = rhsProjTable.getTable().getColumns().size() - rhsProjTable.getTable().getPKColumns().size();
            PTableWrapper projectedTable = JoinCompiler.mergeProjectedTables(rhsProjTable, lhsProjTable, type == JoinType.Inner);
            ScanProjector.serializeProjectorIntoScan(context.getScan(), JoinCompiler.getScanProjector(rhsProjTable));
            context.setCurrentTable(lastJoinTable.getTable());
            context.setResolver(JoinCompiler.getColumnResolver(projectedTable));
            join.projectColumns(context.getScan(), lastJoinTable.getTable());
            BasicQueryPlan rhsPlan = compileSingleQuery(context, rhs, binds);
            Expression postJoinFilterExpression = join.compilePostFilterExpression(context);
            HashJoinInfo joinInfo = new HashJoinInfo(projectedTable.getTable(), joinIds, new List[] {joinExpressions}, new JoinType[] {type == JoinType.Inner ? type : JoinType.Left}, new boolean[] {true}, new PTable[] {lhsProjTable.getTable()}, new int[] {fieldPosition}, postJoinFilterExpression);
            return new HashJoinPlan(rhsPlan, joinInfo, new List[] {hashExpressions}, new QueryPlan[] {lhsPlan});
        }
        
        // Do not support queries like "A right join B left join C" with hash-joins.
        throw new SQLFeatureNotSupportedException("Joins with pattern 'A right join B left join C' not supported.");
    }
    
    protected BasicQueryPlan compileSingleQuery(StatementContext context, SelectStatement select, List<Object> binds) throws SQLException{
    	PhoenixConnection connection = statement.getConnection();
    	ColumnResolver resolver = context.getResolver();
        TableRef tableRef = context.getCurrentTable();
        // Short circuit out if we're compiling an index query and the index isn't active.
        // We must do this after the ColumnResolver resolves the table, as we may be updating the local
        // cache of the index table and it may now be inactive.
        if (tableRef.getTable().getType() == PTableType.INDEX && tableRef.getTable().getIndexState() != PIndexState.ACTIVE) {
            return new DegenerateQueryPlan(context, select, tableRef);
        }
        Integer limit = LimitCompiler.compile(context, select);

        GroupBy groupBy = GroupByCompiler.compile(context, select);
        // Optimize the HAVING clause by finding any group by expressions that can be moved
        // to the WHERE clause
        select = HavingCompiler.rewrite(context, select, groupBy);
        Expression having = HavingCompiler.compile(context, select, groupBy);
        // Don't pass groupBy when building where clause expression, because we do not want to wrap these
        // expressions as group by key expressions since they're pre, not post filtered.
        context.setResolver(FromCompiler.getResolver(select, connection));
        WhereCompiler.compile(context, select);
        context.setResolver(resolver); // recover resolver
        OrderBy orderBy = OrderByCompiler.compile(context, select, groupBy, limit); 
        RowProjector projector = ProjectionCompiler.compile(context, select, groupBy, targetColumns);
        
        // Final step is to build the query plan
        int maxRows = statement.getMaxRows();
        if (maxRows > 0) {
            if (limit != null) {
                limit = Math.min(limit, maxRows);
            } else {
                limit = maxRows;
            }
        }
        if (select.isAggregate() || select.isDistinct()) {
            return new AggregatePlan(context, select, tableRef, projector, limit, orderBy, parallelIteratorFactory, groupBy, having);
        } else {
            return new ScanPlan(context, select, tableRef, projector, limit, orderBy, parallelIteratorFactory);
        }
    }
}
