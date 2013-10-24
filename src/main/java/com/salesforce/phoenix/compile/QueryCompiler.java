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
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;

import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.execute.AggregatePlan;
import com.salesforce.phoenix.execute.DegenerateQueryPlan;
import com.salesforce.phoenix.execute.ScanPlan;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.iterate.ParallelIterators.ParallelIteratorFactory;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData;
import com.salesforce.phoenix.jdbc.PhoenixStatement;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.AmbiguousColumnException;
import com.salesforce.phoenix.schema.ColumnNotFoundException;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PIndexState;
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
    public QueryPlan compile(SelectStatement select) throws SQLException {
        PhoenixConnection connection = statement.getConnection();
        List<Object> binds = statement.getParameters();
        ColumnResolver resolver = FromCompiler.getResolver(select, connection);
        select = StatementNormalizer.normalize(select, resolver);
        TableRef tableRef = resolver.getTables().get(0);
        StatementContext context = new StatementContext(select, connection, resolver, binds, scan);
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
        WhereCompiler.compile(context, select);
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
