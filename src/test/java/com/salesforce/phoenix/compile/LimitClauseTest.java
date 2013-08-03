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
import org.junit.Test;

import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.query.BaseConnectionlessQueryTest;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.ByteUtil;


public class LimitClauseTest extends BaseConnectionlessQueryTest {
    
    private static Integer compileStatement(String query, List<Object> binds, Scan scan) throws SQLException {
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        statement = RHSLiteralStatementRewriter.normalize(statement);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(pconn, resolver, binds, statement.getBindCount(), scan, null, statement.isAggregate()||statement.isDistinct());
        Map<String, ParseNode> aliasParseNodeMap = ProjectionCompiler.buildAliasParseNodeMap(context, statement.getSelect());

        Integer limit = LimitCompiler.getLimit(context, statement.getLimit());
        GroupBy groupBy = GroupByCompiler.getGroupBy(context, statement, aliasParseNodeMap);
        statement = HavingCompiler.moveToWhereClause(context, statement, groupBy);
        HavingCompiler.getExpression(statement, context, groupBy);
        Expression where = WhereCompiler.getWhereClause(context, statement.getWhere());
        assertNull(where);
        return limit;
    }
    
    @Test
    public void testLimit() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' limit 5";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        Integer limit = compileStatement(query, binds, scan);

        assertArrayEquals(PDataType.VARCHAR.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStopRow());
        assertEquals(limit,Integer.valueOf(5));
    }

    @Test
    public void testNoLimit() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        Integer limit = compileStatement(query, binds, scan);

        assertNull(limit);
        assertArrayEquals(PDataType.VARCHAR.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStopRow());
    }
    
    @Test
    public void testBoundLimit() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' limit ?";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(5);
        Integer limit = compileStatement(query, binds, scan);

        assertArrayEquals(PDataType.VARCHAR.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStopRow());
        assertEquals(limit,Integer.valueOf(5));
    }

    @Test
    public void testTypeMismatchBoundLimit() throws SQLException {
        String query = "select * from atable limit ?";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList("foo");
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(pconn, resolver, binds, statement.getBindCount(), scan, null, statement.isAggregate()||statement.isDistinct());
        try {
            LimitCompiler.getLimit(context, statement.getLimit());
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Type mismatch"));
        }
    }

    @Test
    public void testNegativeBoundLimit() throws SQLException {
        String query = "select * from atable limit ?";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(-1);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(pconn, resolver, binds, statement.getBindCount(), scan, null, statement.isAggregate()||statement.isDistinct());
        assertNull(LimitCompiler.getLimit(context, statement.getLimit()));
    }

    @Test
    public void testBindTypeMismatch() throws SQLException {
        Long tenantId = Long.valueOf(0);
        String keyPrefix = "002";
        List<Object> binds = Arrays.<Object>asList(tenantId,keyPrefix);
        String query = "select * from atable where organization_id=? and substr(entity_id,1,3)=?";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(pconn, resolver, binds, statement.getBindCount(), scan, null, statement.isAggregate()||statement.isDistinct());
        try {
            WhereCompiler.getWhereClause(context, statement.getWhere());
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 203 (22005): Type mismatch."));
        }
    }
}
