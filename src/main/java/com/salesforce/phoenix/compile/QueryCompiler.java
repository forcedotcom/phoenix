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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;

import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.JoinCompiler.JoinSpec;
import com.salesforce.phoenix.compile.JoinCompiler.JoinTable;
import com.salesforce.phoenix.compile.JoinCompiler.StarJoinType;
import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.execute.*;
import com.salesforce.phoenix.expression.Expression;
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

    public QueryCompiler(PhoenixConnection connection, int maxRows) throws SQLException {
        this(connection, maxRows, new Scan());
    }
    
    public QueryCompiler(PhoenixConnection connection, int maxRows, Scan scan) throws SQLException {
        this(connection, maxRows, scan, null);
    }
    
    public QueryCompiler(PhoenixConnection connection, int maxRows, PColumn[] targetDatums) throws SQLException {
        this(connection, maxRows, new Scan(), targetDatums);
    }

    public QueryCompiler(PhoenixConnection connection, int maxRows, Scan scan, PColumn[] targetDatums) throws SQLException {
        this.connection = connection;
        this.maxRows = maxRows;
        this.scan = scan;
        this.targetColumns = targetDatums;
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
        
        statement = RHSLiteralStatementRewriter.normalize(statement);
        List<TableNode> fromNodes = statement.getFrom();
        if (fromNodes.size() == 1) {
            ColumnResolver resolver = FromCompiler.getResolver(statement, connection);
            StatementContext context = new StatementContext(connection, resolver, binds, statement.getBindCount(), scan, statement.getHint());
            return compileSingleQuery(context, statement, binds);
        }
        
        JoinSpec join = JoinCompiler.getJoinSpec(statement, connection);
        StatementContext context = new StatementContext(connection, join.getColumnResolver(), binds, statement.getBindCount(), scan, statement.getHint(), new HashCacheClient(connection.getQueryServices(), connection.getTenantId()));
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
            ImmutableBytesWritable[] joinIds = new ImmutableBytesWritable[count];
            List<Expression>[] joinExpressions = (List<Expression>[]) new List[count];
            List<Expression>[] hashExpressions = (List<Expression>[]) new List[count];
            JoinType[] joinTypes = new JoinType[count];
            QueryPlan[] joinPlans = new QueryPlan[count];
            for (int i = 0; i < count; i++) {
                JoinTable joinTable = joinTables.get(i);
                joinIds[i] = new ImmutableBytesWritable(HashCacheClient.nextJoinId());
                Pair<List<Expression>, List<Expression>> splittedExpressions = JoinCompiler.splitEquiJoinConditions(joinTable);
                joinExpressions[i] = splittedExpressions.getFirst();
                hashExpressions[i] = splittedExpressions.getSecond();
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
            SelectStatement lhs = JoinCompiler.getSubQueryWithoutLastJoin(statement);
            SelectStatement rhs = JoinCompiler.getSubqueryForLastJoinTable(statement, join);
            JoinSpec lhsJoin = JoinCompiler.getSubJoinSpec(join);
            StatementContext lhsCtx = new StatementContext(connection, join.getColumnResolver(), binds, statement.getBindCount(), scan, statement.getHint(), new HashCacheClient(connection.getQueryServices(), connection.getTenantId()));
            QueryPlan lhsPlan = compileJoinQuery(lhsCtx, lhs, binds, lhsJoin);
            byte[] joinId = HashCacheClient.nextJoinId();
            ImmutableBytesWritable[] joinIds = new ImmutableBytesWritable[] {new ImmutableBytesWritable(joinId)};
            Pair<List<Expression>, List<Expression>> splittedExpressions = JoinCompiler.splitEquiJoinConditions(lastJoinTable);
            List<Expression> joinExpressions = splittedExpressions.getSecond();
            List<Expression> hashExpressions = splittedExpressions.getFirst();
            Expression postJoinFilterExpression = JoinCompiler.getPostJoinFilterExpression(join, lastJoinTable);
            HashJoinInfo joinInfo = new HashJoinInfo(joinIds, new List[] {joinExpressions}, new JoinType[] {JoinType.Left}, postJoinFilterExpression);
            HashJoinInfo.serializeHashJoinIntoScan(context.getScan(), joinInfo);
            BasicQueryPlan rhsPlan = compileSingleQuery(context, rhs, binds);
            return new HashJoinPlan(rhsPlan, joinIds, new List[] {hashExpressions}, new QueryPlan[] {lhsPlan});
        }
        
        SelectStatement lhs = JoinCompiler.getSubQueryWithoutLastJoinAsFinalPlan(statement);
        SelectStatement rhs = lastJoinTable.getAsSubquery();
        QueryPlan rhsPlan;
        try {
            rhsPlan = compile(rhs, binds, new Scan(scanCopy));
        } catch (IOException e) {
            throw new SQLException(e);
        }
        byte[] joinId = HashCacheClient.nextJoinId();
        ImmutableBytesWritable[] joinIds = new ImmutableBytesWritable[] {new ImmutableBytesWritable(joinId)};
        Pair<List<Expression>, List<Expression>> splittedExpressions = JoinCompiler.splitEquiJoinConditions(lastJoinTable);
        List<Expression> joinExpressions = splittedExpressions.getFirst();
        List<Expression> hashExpressions = splittedExpressions.getSecond();
        Expression postJoinFilterExpression = JoinCompiler.getPostJoinFilterExpression(join, null);
        HashJoinInfo joinInfo = new HashJoinInfo(joinIds, new List[] {joinExpressions}, new JoinType[] {JoinType.Left}, postJoinFilterExpression);
        HashJoinInfo.serializeHashJoinIntoScan(context.getScan(), joinInfo);
        BasicQueryPlan lhsPlan = compileSingleQuery(context, lhs, binds);
        return new HashJoinPlan(lhsPlan, joinIds, new List[] {hashExpressions}, new QueryPlan[] {rhsPlan});
    }
    
    protected BasicQueryPlan compileSingleQuery(StatementContext context, SelectStatement statement, List<Object> binds) throws SQLException{
        ColumnResolver resolver = context.getResolver();
        TableRef tableRef = resolver.getTables().get(0);
        PTable table = tableRef.getTable();
        if (table.getType() == PTableType.INDEX && table.getIndexState() != PIndexState.ACTIVE) {
            return new DegenerateQueryPlan(context, tableRef);
        }
        
        Map<String, ParseNode> aliasParseNodeMap = ProjectionCompiler.buildAliasParseNodeMap(context, statement.getSelect());
        Integer limit = LimitCompiler.getLimit(context, statement.getLimit());

        GroupBy groupBy = GroupByCompiler.getGroupBy(context, statement, aliasParseNodeMap);
        boolean isDistinct = statement.getGroupBy().isEmpty() && statement.isDistinct();
        // Optimize the HAVING clause by finding any group by expressions that can be moved
        // to the WHERE clause
        statement = HavingCompiler.moveToWhereClause(context, statement, groupBy);
        Expression having = HavingCompiler.getExpression(statement, context, groupBy);
        // Don't pass groupBy when building where clause expression, because we do not want to wrap these
        // expressions as group by key expressions since they're pre, not post filtered.
        WhereCompiler.getWhereClause(context, statement.getWhere());
        OrderBy orderBy = OrderByCompiler.getOrderBy(context, statement.getOrderBy(), groupBy, isDistinct, limit, aliasParseNodeMap); 
        RowProjector projector = ProjectionCompiler.getRowProjector(context, statement.getSelect(), statement.isDistinct(), groupBy, orderBy, targetColumns);
        
        // Final step is to build the query plan
        if (maxRows > 0) {
            if (limit != null) {
                limit = Math.min(limit, maxRows);
            } else {
                limit = maxRows;
            }
        }
        if (context.isAggregate()) {
            // We must add an extra dedup step if there's a group by and a select distinct
            boolean dedup = !statement.getGroupBy().isEmpty() && statement.isDistinct();
            return new AggregatePlan(context, tableRef, projector, limit, groupBy, dedup, having, orderBy);
        } else {
            return new ScanPlan(context, tableRef, projector, limit, orderBy);
        }
    }
}
