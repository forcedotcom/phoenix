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

import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.junit.Test;

import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.filter.SkipScanFilter;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.query.BaseConnectionlessQueryTest;


/**
 * Test compilation of various statements with hints.
 */
public class StatementHintsCompilationTest extends BaseConnectionlessQueryTest {

    private static StatementContext compileStatement(String query, Scan scan, List<Object> binds) throws SQLException {
        return compileStatement(query, scan, binds, null, null);
    }

    private static StatementContext compileStatement(String query, Scan scan, List<Object> binds, Integer limit, Set<Expression> extractedNodes) throws SQLException {
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        statement = RHSLiteralStatementRewriter.normalize(statement);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(pconn, resolver, binds, statement.getBindCount(), scan, statement.getHint(), statement.isAggregate());
        Map<String, ParseNode> aliasParseNodeMap = ProjectionCompiler.buildAliasParseNodeMap(context, statement.getSelect());

        Integer actualLimit = LimitCompiler.getLimit(context, statement.getLimit());
        assertEquals(limit, actualLimit);
        GroupBy groupBy = GroupByCompiler.getGroupBy(context, statement, aliasParseNodeMap);
        statement = HavingCompiler.moveToWhereClause(context, statement, groupBy);
        WhereCompiler.compileWhereClause(context, statement.getWhere(), extractedNodes);
        return context;
    }

    @Test
    public void testSelectForceSkipScan() throws Exception {
        String id = "000000000000001";
        // A where clause without the first column usually compiles into a range scan.
        String query = "SELECT /*+ SKIP_SCAN */ * FROM atable WHERE entity_id='" + id + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        
        compileStatement(query, scan, binds);
        // Forced to using skip scan filter.
        Filter filter = scan.getFilter();
        if (filter instanceof FilterList) {
            filter = ((FilterList) filter).getFilters().get(0);
        }
        assertTrue("The first filter should be SkipScanFilter.", filter instanceof SkipScanFilter);
    }

    @Test
    public void testSelectForceRangeScan() throws Exception {
        String query = "SELECT /*+ RANGE_SCAN */ * FROM atable WHERE organization_id in (" +
                "'000000000000001', '000000000000002', '000000000000003', '000000000000004')";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        
        compileStatement(query, scan, binds);
        // Verify that it is not using SkipScanFilter.
        Filter filter = scan.getFilter();
        if (filter instanceof FilterList) {
            filter = ((FilterList) filter).getFilters().get(0);
        }
        assertFalse("The first filter should not be SkipScanFilter.", filter instanceof SkipScanFilter);
    }
}
