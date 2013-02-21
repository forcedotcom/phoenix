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

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;

import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.execute.AggregatePlan;
import com.salesforce.phoenix.execute.ScanPlan;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.RHSLiteralStatementRewriter;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.schema.*;



/**
 * 
 * Class used to build an executable query plan
 *
 * @author jtaylor
 * @since 0.1
 */
public class QueryCompiler {
    private final PhoenixConnection connection;
    private final Scan scan;
    private final int maxRows;
    private final PColumn[] targetColumns;
    
    public QueryCompiler(PhoenixConnection connection, int maxRows) {
        this(connection, maxRows, new Scan());
    }
    
    public QueryCompiler(PhoenixConnection connection, int maxRows, Scan scan) {
        this(connection, maxRows, scan, null);
    }
    
    public QueryCompiler(PhoenixConnection connection, int maxRows, PColumn[] targetDatums) {
        this(connection, maxRows, new Scan(), targetDatums);
    }

    public QueryCompiler(PhoenixConnection connection, int maxRows, Scan scan, PColumn[] targetDatums) {
        this.connection = connection;
        this.maxRows = maxRows;
        this.scan = scan;
        this.targetColumns = targetDatums;
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
        
        assert(binds.size() == statement.getBindCount());
        
        statement = RHSLiteralStatementRewriter.normalizeWhereClause(statement);
        ColumnResolver resolver = FromCompiler.getResolver(statement, connection);
        StatementContext context = new StatementContext(connection, resolver, binds, statement.getBindCount(), scan);
        Integer limit = LimitCompiler.getLimit(context, statement.getLimit());

        GroupBy groupBy = GroupByCompiler.getGroupBy(statement, context);
        // Optimize the HAVING clause by finding any group by expressions that can be moved
        // to the WHERE clause
        statement = HavingCompiler.moveToWhereClause(statement, context, groupBy);
        Expression having = HavingCompiler.getExpression(statement, context, groupBy);
        // Don't pass groupBy when building where clause expression, because we do not want to wrap these
        // expressions as group by key expressions since they're pre, not post filtered.
        WhereCompiler.getWhereClause(context, statement.getWhere());
        OrderBy orderBy = OrderByCompiler.getOrderBy(statement, context, groupBy, limit); 
        RowProjector projector = ProjectionCompiler.getRowProjector(statement, context, groupBy, orderBy, limit, targetColumns);
        
        // Final step is to build the query plan
        TableRef table = resolver.getTables().get(0);
        if (context.isAggregate()) {
            return new AggregatePlan(context, table, projector, limit, groupBy, having, orderBy, maxRows);
        } else {
            if (maxRows > 0) {
                if (limit != null) {
                    limit = Math.min(limit, maxRows);
                } else {
                    limit = maxRows;
                }
            }
            return new ScanPlan(context, table, projector, limit, orderBy);
        }
    }
}
