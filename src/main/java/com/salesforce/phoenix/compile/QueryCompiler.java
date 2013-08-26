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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Scan;
import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.JoinCompiler.JoinSpec;
import com.salesforce.phoenix.compile.JoinCompiler.JoinTable;
import com.salesforce.phoenix.compile.JoinCompiler.StarJoinType;
import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.execute.*;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.iterate.ParallelIterators.ParallelIteratorFactory;
import com.salesforce.phoenix.iterate.SpoolingResultIterator.SpoolingResultIteratorFactory;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.join.HashCacheClient;
import com.salesforce.phoenix.join.HashJoinInfo;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.parse.JoinTableNode.JoinType;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.util.ImmutableBytesPtr;



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
    private final PhoenixConnection connection;
    private final Scan scan;
    private final Scan scanCopy;
    private final int maxRows;
    private final PColumn[] targetColumns;
    private final ParallelIteratorFactory parallelIteratorFactory;

    public QueryCompiler(PhoenixConnection connection, int maxRows) throws SQLException {
        this(connection, maxRows, new Scan());
    }
    
    public QueryCompiler(PhoenixConnection connection, int maxRows, Scan scan) throws SQLException {
        this(connection, maxRows, scan, null, new SpoolingResultIteratorFactory(connection.getQueryServices()));
    }
    
    public QueryCompiler(PhoenixConnection connection, int maxRows, PColumn[] targetDatums, ParallelIteratorFactory parallelIteratorFactory) throws SQLException {
        this(connection, maxRows, new Scan(), targetDatums, parallelIteratorFactory);
    }

    public QueryCompiler(PhoenixConnection connection, int maxRows, Scan scan, PColumn[] targetDatums, ParallelIteratorFactory parallelIteratorFactory) throws SQLException {
        this.connection = connection;
        this.maxRows = maxRows;
        this.scan = scan;
        this.targetColumns = targetDatums;
        this.parallelIteratorFactory = parallelIteratorFactory;
        if (connection.getQueryServices().getLowestClusterHBaseVersion() >= PhoenixDatabaseMetaData.ESSENTIAL_FAMILY_VERSION_THRESHOLD) {
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
     * @param statement parsed SQL statement
     * @param binds values of bind variables
     * @return executable query plan
     * @throws SQLException if mismatched types are found, bind value do not match binds,
     * or invalid function arguments are encountered.
     * @throws SQLFeatureNotSupportedException if an unsupported construct is encountered
     * @throws TableNotFoundException if table name not found in schema
     * @throws ColumnNotFoundException if column name could not be resolved
     * @throws AmbiguousColumnException if an unaliased column name is ambiguous across multiple tables
     */
    public QueryPlan compile(SelectStatement statement, List<Object> binds) throws SQLException{
        return compile(statement, binds, scan);
    }
    
    protected QueryPlan compile(SelectStatement statement, List<Object> binds, Scan scan) throws SQLException{        
        assert(binds.size() == statement.getBindCount());
        
        statement = StatementNormalizer.normalize(statement);
        List<TableNode> fromNodes = statement.getFrom();
        if (fromNodes.size() == 1) {
            ColumnResolver resolver = FromCompiler.getResolver(statement, connection);
            StatementContext context = new StatementContext(statement, connection, resolver, binds, scan, null);
            return compileSingleQuery(context, statement, binds);
        }
        
        JoinSpec join = JoinCompiler.getJoinSpec(statement, connection);
        StatementContext context = new StatementContext(statement, connection, join.getColumnResolver(), binds, scan, new HashCacheClient(connection));
        return compileJoinQuery(context, statement, binds, join);
    }
    
    @SuppressWarnings("unchecked")
    protected QueryPlan compileJoinQuery(StatementContext context, SelectStatement statement, List<Object> binds, JoinSpec join) throws SQLException {
        List<JoinTable> joinTables = join.getJoinTables();
        if (joinTables.isEmpty())
            return compileSingleQuery(context, statement, binds);
        
        StarJoinType starJoin = JoinCompiler.getStarJoinType(join);
        if (starJoin == StarJoinType.BASIC || starJoin == StarJoinType.EXTENDED) {
            int count = joinTables.size();
            ImmutableBytesPtr[] joinIds = new ImmutableBytesPtr[count];
            List<Expression>[] joinExpressions = (List<Expression>[]) new List[count];
            List<Expression>[] hashExpressions = (List<Expression>[]) new List[count];
            JoinType[] joinTypes = new JoinType[count];
            QueryPlan[] joinPlans = new QueryPlan[count];
            for (int i = 0; i < count; i++) {
                JoinTable joinTable = joinTables.get(i);
                joinIds[i] = new ImmutableBytesPtr(); // place-holder
                if (!joinTable.isEquiJoin())
                    throw new UnsupportedOperationException("Do not support non equi-joins.");
                joinExpressions[i] = joinTable.getLeftTableConditions();
                hashExpressions[i] = joinTable.getRightTableConditions();
                joinTypes[i] = joinTable.getType();
                try {
                    joinPlans[i] = compile(joinTable.getAsSubquery(), binds, new Scan(scanCopy));
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }
            Expression postJoinFilterExpression = JoinCompiler.getPostJoinFilterExpression(join, null);
            HashJoinInfo joinInfo = new HashJoinInfo(joinIds, joinExpressions, joinTypes, postJoinFilterExpression);
            HashJoinInfo.serializeHashJoinIntoScan(context.getScan(), joinInfo);
            BasicQueryPlan plan = compileSingleQuery(context, JoinCompiler.getSubqueryWithoutJoin(statement, join), binds);
            return new HashJoinPlan(plan, joinIds, hashExpressions, joinPlans);
        }
        
        JoinTable lastJoinTable = joinTables.get(joinTables.size() - 1);
        JoinType type = lastJoinTable.getType();
        if (type == JoinType.Full)
            throw new UnsupportedOperationException("Does not support full join.");
        
        if (type == JoinType.Right
                || (type == JoinType.Inner && joinTables.size() > 1)) {
            SelectStatement lhs = JoinCompiler.getSubQueryWithoutLastJoin(statement, join);
            SelectStatement rhs = JoinCompiler.getSubqueryForLastJoinTable(statement, join);
            JoinSpec lhsJoin = JoinCompiler.getSubJoinSpec(join);
            StatementContext lhsCtx = new StatementContext(statement, connection, join.getColumnResolver(), binds, scan, context.getHashClient());
            QueryPlan lhsPlan = compileJoinQuery(lhsCtx, lhs, binds, lhsJoin);
            ImmutableBytesPtr[] joinIds = new ImmutableBytesPtr[] {new ImmutableBytesPtr()};
            if (!lastJoinTable.isEquiJoin())
                throw new UnsupportedOperationException("Do not support non equi-joins.");
            Expression postJoinFilterExpression = JoinCompiler.getPostJoinFilterExpression(join, lastJoinTable);
            HashJoinInfo joinInfo = new HashJoinInfo(joinIds, new List[] {lastJoinTable.getRightTableConditions()}, new JoinType[] {JoinType.Left}, postJoinFilterExpression);
            HashJoinInfo.serializeHashJoinIntoScan(context.getScan(), joinInfo);
            BasicQueryPlan rhsPlan = compileSingleQuery(context, rhs, binds);
            return new HashJoinPlan(rhsPlan, joinIds, new List[] {lastJoinTable.getLeftTableConditions()}, new QueryPlan[] {lhsPlan});
        }
        
        SelectStatement lhs = JoinCompiler.getSubQueryWithoutLastJoinAsFinalPlan(statement, join);
        SelectStatement rhs = lastJoinTable.getAsSubquery();
        QueryPlan rhsPlan;
        try {
            rhsPlan = compile(rhs, binds, new Scan(scanCopy));
        } catch (IOException e) {
            throw new SQLException(e);
        }
        ImmutableBytesPtr[] joinIds = new ImmutableBytesPtr[] {new ImmutableBytesPtr()};
        if (!lastJoinTable.isEquiJoin())
            throw new UnsupportedOperationException("Do not support non equi-joins.");
        Expression postJoinFilterExpression = JoinCompiler.getPostJoinFilterExpression(join, null);
        HashJoinInfo joinInfo = new HashJoinInfo(joinIds, new List[] {lastJoinTable.getLeftTableConditions()}, new JoinType[] {JoinType.Left}, postJoinFilterExpression);
        HashJoinInfo.serializeHashJoinIntoScan(context.getScan(), joinInfo);
        BasicQueryPlan lhsPlan = compileSingleQuery(context, lhs, binds);
        return new HashJoinPlan(lhsPlan, joinIds, new List[] {lastJoinTable.getRightTableConditions()}, new QueryPlan[] {rhsPlan});
    }
    
    protected BasicQueryPlan compileSingleQuery(StatementContext context, SelectStatement statement, List<Object> binds) throws SQLException{
        ColumnResolver resolver = context.getResolver();
        TableRef tableRef = resolver.getTables().get(0);
        // Short circuit out if we're compiling an index query and the index isn't active.
        // We must do this after the ColumnResolver resolves the table, as we may be updating the local
        // cache of the index table and it may now be inactive.
        if (tableRef.getTable().getType() == PTableType.INDEX && tableRef.getTable().getIndexState() != PIndexState.ACTIVE) {
            return new DegenerateQueryPlan(context, statement, tableRef);
        }
        Map<String, ParseNode> aliasMap = ProjectionCompiler.buildAliasMap(context, statement);
        Integer limit = LimitCompiler.compile(context, statement);

        GroupBy groupBy = GroupByCompiler.compile(context, statement, aliasMap);
        // Optimize the HAVING clause by finding any group by expressions that can be moved
        // to the WHERE clause
        statement = HavingCompiler.rewrite(context, statement, groupBy);
        Expression having = HavingCompiler.compile(context, statement, groupBy);
        // Don't pass groupBy when building where clause expression, because we do not want to wrap these
        // expressions as group by key expressions since they're pre, not post filtered.
        WhereCompiler.compile(context, statement);
        OrderBy orderBy = OrderByCompiler.compile(context, statement, aliasMap, groupBy, limit); 
        RowProjector projector = ProjectionCompiler.compile(context, statement, groupBy, targetColumns);
        
        // Final step is to build the query plan
        if (maxRows > 0) {
            if (limit != null) {
                limit = Math.min(limit, maxRows);
            } else {
                limit = maxRows;
            }
        }
        if (statement.isAggregate() || statement.isDistinct()) {
            return new AggregatePlan(context, statement, tableRef, projector, limit, orderBy, parallelIteratorFactory, groupBy, having);
        } else {
            return new ScanPlan(context, statement, tableRef, projector, limit, orderBy, parallelIteratorFactory);
        }
    }
}
