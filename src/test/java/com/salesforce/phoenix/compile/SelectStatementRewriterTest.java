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
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.junit.Test;

import com.salesforce.phoenix.expression.*;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.SQLParser;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.query.BaseConnectionlessQueryTest;



public class SelectStatementRewriterTest extends BaseConnectionlessQueryTest {
    private static Expression compileStatement(String query) throws SQLException {
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(pconn, resolver, binds, statement.getBindCount(), scan, null, statement.isAggregate()||statement.isDistinct());
        statement = RHSLiteralStatementRewriter.normalize(statement);
        Expression whereClause = WhereCompiler.getWhereClause(context, statement.getWhere());
        return WhereOptimizer.pushKeyExpressionsToScan(context, whereClause);
    }
    
    @Test
    public void testCollapseAnd() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_integer=0";
        Expression where = compileStatement(query);
        assertTrue(where instanceof ComparisonExpression);
        ComparisonExpression child = (ComparisonExpression)where;
        assertEquals(CompareOp.EQUAL, child.getFilterOp());
        assertTrue(child.getChildren().get(0) instanceof KeyValueColumnExpression);
        assertTrue(child.getChildren().get(1) instanceof LiteralExpression);
    }
    
    @Test
    public void testLHSLiteralCollapseAnd() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where '" + tenantId + "'=organization_id and 0=a_integer";
        Expression where = compileStatement(query);
        assertTrue(where instanceof ComparisonExpression);
        ComparisonExpression child = (ComparisonExpression)where;
        assertEquals(CompareOp.EQUAL, child.getFilterOp());
        assertTrue(child.getChildren().get(0) instanceof KeyValueColumnExpression);
        assertTrue(child.getChildren().get(1) instanceof LiteralExpression);
    }
    
    @Test
    public void testRewriteAnd() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_integer=0 and a_string='foo'";
        Expression where = compileStatement(query);
        
        assertTrue(where instanceof AndExpression);
        assertTrue(where.getChildren().size() == 2);
        assertTrue(where.getChildren().get(0) instanceof ComparisonExpression);
        assertEquals(CompareOp.EQUAL, ((ComparisonExpression)where.getChildren().get(0)).getFilterOp());
        assertTrue(where.getChildren().get(1) instanceof ComparisonExpression);
        assertEquals(CompareOp.EQUAL, ((ComparisonExpression)where.getChildren().get(1)).getFilterOp());
    }

    @Test
    public void testCollapseWhere() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(organization_id,1,3)='foo' LIMIT 2";
        Expression where = compileStatement(query);
        assertNull(where);
    }

    @Test
    public void testNoCollapse() throws SQLException {
        String query = "select * from atable where a_integer=0 and a_string='foo'";
        Expression where = compileStatement(query);
        assertEquals(2, where.getChildren().size());
        assertTrue(where.getChildren().get(0) instanceof ComparisonExpression);
        assertEquals(CompareOp.EQUAL, ((ComparisonExpression)where.getChildren().get(0)).getFilterOp());
        assertTrue(where.getChildren().get(1) instanceof ComparisonExpression);
        assertEquals(CompareOp.EQUAL, ((ComparisonExpression)where.getChildren().get(1)).getFilterOp());
    }
}
