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

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;

import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.JoinCompiler.JoinSpec;
import com.salesforce.phoenix.compile.JoinCompiler.JoinTable;
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
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.join.HashCacheClient;
import com.salesforce.phoenix.join.HashJoinInfo;
import com.salesforce.phoenix.join.ScanProjector;
import com.salesforce.phoenix.parse.JoinTableNode.JoinType;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.AmbiguousColumnException;
import com.salesforce.phoenix.schema.ColumnNotFoundException;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PIndexState;
import com.salesforce.phoenix.schema.PTableType;
import com.salesforce.phoenix.schema.TableNotFoundException;
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
        this(connection, maxRows, scan, null, null);
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
        
        ColumnResolver resolver = FromCompiler.getResolver(statement, connection);
        statement = StatementNormalizer.normalize(statement, resolver);
        if (statement.getFrom().size() == 1) {
            StatementContext context = new StatementContext(statement, connection, resolver, binds, scan);
            return compileSingleQuery(context, statement, binds);
        }
        
        StatementContext context = new StatementContext(statement, connection, resolver, binds, scan, true, new HashCacheClient(connection));
        JoinSpec join = JoinCompiler.getJoinSpec(context, statement);
        return compileJoinQuery(context, statement, binds, join);
    }
    
    @SuppressWarnings("unchecked")
    protected QueryPlan compileJoinQuery(StatementContext context, SelectStatement statement, List<Object> binds, JoinSpec join) throws SQLException {
        byte[] emptyByteArray = new byte[0];
        List<JoinTable> joinTables = join.getJoinTables();
        if (joinTables.isEmpty()) {
            context.setCurrentTable(join.getMainTable());
            join.projectColumns(context.getScan(), join.getMainTable());
            ScanProjector.serializeProjectorIntoScan(context.getScan(), join.getScanProjector());
            return compileSingleQuery(context, statement, binds);
        }
        
        boolean[] starJoinVector = JoinCompiler.getStarJoinVector(join);
        if (starJoinVector != null) {
            context.setCurrentTable(context.getResolver().getTables().get(0));
            int count = joinTables.size();
            ImmutableBytesPtr[] joinIds = new ImmutableBytesPtr[count];
            List<Expression>[] joinExpressions = (List<Expression>[]) new List[count];
            List<Expression>[] hashExpressions = (List<Expression>[]) new List[count];
            JoinType[] joinTypes = new JoinType[count];
            QueryPlan[] joinPlans = new QueryPlan[count];
            for (int i = 0; i < count; i++) {
                JoinTable joinTable = joinTables.get(i);
                joinIds[i] = new ImmutableBytesPtr(emptyByteArray); // place-holder
                Pair<List<Expression>, List<Expression>> joinConditions = joinTable.compileJoinConditions(context);
                joinExpressions[i] = joinConditions.getFirst();
                hashExpressions[i] = joinConditions.getSecond();
                joinTypes[i] = joinTable.getType();
                try {
                    Scan subScan = new Scan(scanCopy);
                    join.projectColumns(subScan, joinTable.getTable());
                    ScanProjector.serializeProjectorIntoScan(subScan, joinTable.getScanProjector());
                    joinPlans[i] = compile(joinTable.getAsSubquery(), binds, subScan);
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }
            Expression postJoinFilterExpression = join.compilePostFilterExpression(context);
            HashJoinInfo joinInfo = new HashJoinInfo(joinIds, joinExpressions, joinTypes, starJoinVector, postJoinFilterExpression);
            join.projectColumns(context.getScan(), join.getMainTable());
            ScanProjector.serializeProjectorIntoScan(context.getScan(), join.getScanProjector());
            BasicQueryPlan plan = compileSingleQuery(context, JoinCompiler.getSubqueryWithoutJoin(statement, join), binds);
            return new HashJoinPlan(plan, joinInfo, hashExpressions, joinPlans);
        }
        
        JoinTable lastJoinTable = joinTables.get(joinTables.size() - 1);
        JoinType type = lastJoinTable.getType();
        if (type == JoinType.Full)
            throw new SQLFeatureNotSupportedException("Full joins not supported.");
        
        if (type == JoinType.Right
                || (type == JoinType.Inner && joinTables.size() > 1)) {
            SelectStatement lhs = JoinCompiler.getSubQueryWithoutLastJoin(statement, join);
            SelectStatement rhs = JoinCompiler.getSubqueryForLastJoinTable(statement, join);
            context.setCurrentTable(lastJoinTable.getTable());
            JoinSpec lhsJoin = JoinCompiler.getSubJoinSpecWithoutPostFilters(join);
            Scan subScan;
            try {
                subScan = new Scan(scanCopy);
            } catch (IOException e) {
                throw new SQLException(e);
            }
            StatementContext lhsCtx = new StatementContext(statement, connection, context.getResolver(), binds, subScan, true, context.getHashClient());
            QueryPlan lhsPlan = compileJoinQuery(lhsCtx, lhs, binds, lhsJoin);
            ImmutableBytesPtr[] joinIds = new ImmutableBytesPtr[] {new ImmutableBytesPtr(emptyByteArray)};
            Expression postJoinFilterExpression = join.compilePostFilterExpression(context);
            Pair<List<Expression>, List<Expression>> joinConditions = lastJoinTable.compileJoinConditions(context);
            List<Expression> joinExpressions = joinConditions.getSecond();
            List<Expression> hashExpressions = joinConditions.getFirst();
            HashJoinInfo joinInfo = new HashJoinInfo(joinIds, new List[] {joinExpressions}, new JoinType[] {type == JoinType.Inner ? type : JoinType.Left}, new boolean[] {true}, postJoinFilterExpression);
            join.projectColumns(context.getScan(), lastJoinTable.getTable());
            ScanProjector.serializeProjectorIntoScan(context.getScan(), lastJoinTable.getScanProjector());
            BasicQueryPlan rhsPlan = compileSingleQuery(context, rhs, binds);
            return new HashJoinPlan(rhsPlan, joinInfo, new List[] {hashExpressions}, new QueryPlan[] {lhsPlan});
        }
        
        SelectStatement lhs = JoinCompiler.getSubQueryWithoutLastJoinAsFinalPlan(statement, join);
        SelectStatement rhs = lastJoinTable.getAsSubquery();
        context.setCurrentTable(context.getResolver().getTables().get(0));
        Scan subScan;
        try {
            subScan = new Scan(scanCopy);
        } catch (IOException e) {
            throw new SQLException(e);
        }
        join.projectColumns(subScan, lastJoinTable.getTable());
        ScanProjector.serializeProjectorIntoScan(subScan, lastJoinTable.getScanProjector());
        QueryPlan rhsPlan = compile(rhs, binds, subScan);
        ImmutableBytesPtr[] joinIds = new ImmutableBytesPtr[] {new ImmutableBytesPtr(emptyByteArray)};
        Expression postJoinFilterExpression = join.compilePostFilterExpression(context);
        Pair<List<Expression>, List<Expression>> joinConditions = lastJoinTable.compileJoinConditions(context);
        List<Expression> joinExpressions = joinConditions.getFirst();
        List<Expression> hashExpressions = joinConditions.getSecond();
        HashJoinInfo joinInfo = new HashJoinInfo(joinIds, new List[] {joinExpressions}, new JoinType[] {JoinType.Left}, new boolean[] {true}, postJoinFilterExpression);
        join.projectColumns(context.getScan(), context.getResolver().getTables().get(0));
        ScanProjector.serializeProjectorIntoScan(context.getScan(), join.getScanProjector());
        BasicQueryPlan lhsPlan = compileSingleQuery(context, lhs, binds);
        return new HashJoinPlan(lhsPlan, joinInfo, new List[] {hashExpressions}, new QueryPlan[] {rhsPlan});
    }
    
    protected BasicQueryPlan compileSingleQuery(StatementContext context, SelectStatement statement, List<Object> binds) throws SQLException{
        ColumnResolver resolver = context.getResolver();
        TableRef tableRef = context.getCurrentTable();
        // Short circuit out if we're compiling an index query and the index isn't active.
        // We must do this after the ColumnResolver resolves the table, as we may be updating the local
        // cache of the index table and it may now be inactive.
        if (tableRef.getTable().getType() == PTableType.INDEX && tableRef.getTable().getIndexState() != PIndexState.ACTIVE) {
            return new DegenerateQueryPlan(context, statement, tableRef);
        }
        
        resolver.setDisambiguateWithTable(context.disambiguateWithTable());
        Integer limit = LimitCompiler.compile(context, statement);

        GroupBy groupBy = GroupByCompiler.compile(context, statement);
        // Optimize the HAVING clause by finding any group by expressions that can be moved
        // to the WHERE clause
        statement = HavingCompiler.rewrite(context, statement, groupBy);
        Expression having = HavingCompiler.compile(context, statement, groupBy);
        // Don't pass groupBy when building where clause expression, because we do not want to wrap these
        // expressions as group by key expressions since they're pre, not post filtered.
        resolver.setDisambiguateWithTable(false);
        WhereCompiler.compile(context, statement);
        resolver.setDisambiguateWithTable(context.disambiguateWithTable());
        OrderBy orderBy = OrderByCompiler.compile(context, statement, groupBy, limit); 
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
