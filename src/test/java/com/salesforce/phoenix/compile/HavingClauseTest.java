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

import static com.salesforce.phoenix.util.TestUtil.*;
import static org.junit.Assert.*;

import java.sql.*;
import java.sql.Date;
import java.util.*;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.junit.Test;

import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.expression.function.CountAggregateFunction;
import com.salesforce.phoenix.expression.function.RoundFunction;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.query.BaseConnectionlessQueryTest;


public class HavingClauseTest extends BaseConnectionlessQueryTest {
    private static StatementContext context;
    
    private static class Expressions {
        private Expression whereClause;
        private Expression havingClause;
        
        private Expressions(Expression whereClause, Expression havingClause) {
            this.whereClause = whereClause;
            this.havingClause = havingClause;
        }
    }
    
    private static Expressions compileStatement(String query, List<Object> binds) throws SQLException {
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        statement = StatementNormalizer.normalize(statement);
        Map<String, ParseNode> aliasParseNodeMap = ProjectionCompiler.buildAliasMap(context, statement);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        context = new StatementContext(statement, pconn, resolver, binds, scan);

        GroupBy groupBy = GroupByCompiler.compile(context, statement, aliasParseNodeMap);
        // Optimize the HAVING clause by finding any group by expressions that can be moved
        // to the WHERE clause
        statement = HavingCompiler.rewrite(context, statement, groupBy);
        Expression having = HavingCompiler.compile(context, statement, groupBy);
        Expression where = WhereCompiler.compile(context, statement);
        where = WhereOptimizer.pushKeyExpressionsToScan(context, statement, where);
        return new Expressions(where,having);
    }
    
    @Test
    public void testHavingToWhere() throws SQLException {
        String query = "select count(1) from atable group by a_string having a_string = 'foo'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression w = constantComparison(CompareOp.EQUAL, BaseConnectionlessQueryTest.A_STRING,"foo");
        assertEquals(w, expressions.whereClause);
        assertNull(expressions.havingClause);
    }
    
    @Test
    public void testHavingFuncToWhere() throws SQLException {
        // TODO: confirm that this is a valid optimization
        String query = "select count(1) from atable group by a_date having round(a_date,'hour') > ?";
        Date date = new Date(System.currentTimeMillis());
        List<Object> binds = Arrays.<Object>asList(date);
        Expressions expressions = compileStatement(query,binds);
        Expression w = constantComparison(CompareOp.GREATER, new RoundFunction(Arrays.asList(kvColumn(BaseConnectionlessQueryTest.A_DATE),LiteralExpression.newConstant("hour"),LiteralExpression.newConstant(1))), date);
        assertEquals(w, expressions.whereClause);
        assertNull(expressions.havingClause);
    }
    
    @Test
    public void testHavingToAndWhere() throws SQLException {
        String query = "select count(1) from atable where b_string > 'bar' group by a_string having a_string = 'foo'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression w = and(constantComparison(CompareOp.GREATER, BaseConnectionlessQueryTest.B_STRING,"bar"),constantComparison(CompareOp.EQUAL, BaseConnectionlessQueryTest.A_STRING,"foo"));
        assertEquals(w, expressions.whereClause);
        assertNull(expressions.havingClause);
    }

    
    @Test
    public void testAndHavingToAndWhere() throws SQLException {
        String query = "select count(1) from atable where b_string > 'bar' group by a_string having count(1) >= 1 and a_string = 'foo'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression h = constantComparison(CompareOp.GREATER_OR_EQUAL, new CountAggregateFunction(),1L);
        Expression w = and(constantComparison(CompareOp.GREATER, BaseConnectionlessQueryTest.B_STRING,"bar"),constantComparison(CompareOp.EQUAL, BaseConnectionlessQueryTest.A_STRING,"foo"));
        assertEquals(w, expressions.whereClause);
        assertEquals(h, expressions.havingClause);
    }
    
    @Test
    public void testAndHavingToWhere() throws SQLException {
        String query = "select count(1) from atable group by a_string having count(1) >= 1 and a_string = 'foo'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression h = constantComparison(CompareOp.GREATER_OR_EQUAL, new CountAggregateFunction(),1L);
        Expression w = constantComparison(CompareOp.EQUAL, BaseConnectionlessQueryTest.A_STRING,"foo");
        assertEquals(w, expressions.whereClause);
        assertEquals(h, expressions.havingClause);
    }
    
    @Test
    public void testAggFuncInHaving() throws SQLException {
        String query = "select count(1) from atable group by a_string having count(a_string) >= 1";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression h = constantComparison(CompareOp.GREATER_OR_EQUAL, new CountAggregateFunction(Arrays.asList(kvColumn(BaseConnectionlessQueryTest.A_STRING))),1L);
        assertNull(expressions.whereClause);
        assertEquals(h, expressions.havingClause);
    }
    
    @Test
    public void testOrAggFuncInHaving() throws SQLException {
        String query = "select count(1) from atable group by a_string having count(1) >= 1 or a_string = 'foo'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression h = or(constantComparison(CompareOp.GREATER_OR_EQUAL, new CountAggregateFunction(),1L),constantComparison(CompareOp.EQUAL, pkColumn(BaseConnectionlessQueryTest.A_STRING,Arrays.asList(BaseConnectionlessQueryTest.A_STRING)),"foo"));
        assertNull(expressions.whereClause);
        assertEquals(h, expressions.havingClause);
    }
    
    @Test
    public void testAndAggColsInHaving() throws SQLException {
        String query = "select count(1) from atable group by a_string,b_string having a_string = 'a' and b_string = 'b'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression w = and(constantComparison(CompareOp.EQUAL, BaseConnectionlessQueryTest.A_STRING,"a"),constantComparison(CompareOp.EQUAL, BaseConnectionlessQueryTest.B_STRING,"b"));
        assertEquals(w, expressions.whereClause);
        assertNull(expressions.havingClause);
    }
    
    @Test
    public void testOrAggColsInHaving() throws SQLException {
        String query = "select count(1) from atable group by a_string,b_string having a_string = 'a' or b_string = 'b'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression w = or(constantComparison(CompareOp.EQUAL, BaseConnectionlessQueryTest.A_STRING,"a"),constantComparison(CompareOp.EQUAL, BaseConnectionlessQueryTest.B_STRING,"b"));
        assertEquals(w, expressions.whereClause);
        assertNull(expressions.havingClause);
    }
    
    @Test
    public void testNonAggColInHaving() throws SQLException {
        String query = "select count(1) from atable group by a_string having b_string = 'bar'";
        List<Object> binds = Collections.emptyList();
        try {
            compileStatement(query,binds);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1019 (42Y26): Only aggregate maybe used in the HAVING clause."));
        }
    }
}
