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
import static java.util.Collections.emptyList;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.Format;
import java.util.*;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.salesforce.phoenix.expression.*;
import com.salesforce.phoenix.expression.function.SubstrFunction;
import com.salesforce.phoenix.filter.RowKeyComparisonFilter;
import com.salesforce.phoenix.filter.SkipScanFilter;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.SQLParser;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.RowKeyValueAccessor;
import com.salesforce.phoenix.util.*;


public class WhereClauseFilterTest extends BaseConnectionlessQueryTest {

    private static SelectStatement compileStatement(StatementContext context, SelectStatement statement, ColumnResolver resolver, List<Object> binds, Scan scan, Integer expectedExtractedNodesSize, Integer expectedLimit) throws SQLException {
        statement = StatementNormalizer.normalize(statement);
        Integer limit = LimitCompiler.compile(context, statement);
        assertEquals(expectedLimit, limit);

        Set<Expression> extractedNodes = Sets.newHashSet();
        WhereCompiler.compileWhereClause(context, statement, extractedNodes);
        if (expectedExtractedNodesSize != null) {
            assertEquals(expectedExtractedNodesSize.intValue(), extractedNodes.size());
        }
        return statement;
    }

    @Test
    public void testSingleEqualFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_integer=0";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 1, null);
        Filter filter = scan.getFilter();
        assertEquals(
            singleKVFilter(constantComparison(
                CompareOp.EQUAL,
                BaseConnectionlessQueryTest.A_INTEGER,
                0)),
            filter);
    }

    @Test
    public void testMultiColumnEqualFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_string=b_string";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 1, null);
        Filter filter = scan.getFilter();
        assertEquals(
            multiKVFilter(columnComparison(
                CompareOp.EQUAL,
                BaseConnectionlessQueryTest.A_STRING,
                BaseConnectionlessQueryTest.B_STRING)),
            filter);
    }

    @Test
    public void testCollapseFunctionToNull() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,null) = 'foo'";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        // Nothing extracted, since the where clause becomes FALSE
        statement = compileStatement(context, statement, resolver, binds, scan, 0, null);
        Filter filter = scan.getFilter();
        assertNull(filter);

        assertArrayEquals(scan.getStartRow(),KeyRange.EMPTY_RANGE.getLowerRange());
        assertArrayEquals(scan.getStopRow(),KeyRange.EMPTY_RANGE.getUpperRange());
    }

    @Test
    public void testAndFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id=? and a_integer=0 and a_string='foo'";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(tenantId);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 1, null);
        Filter filter = scan.getFilter();
        
        assertEquals(
            multiKVFilter(and(
                constantComparison(
                    CompareOp.EQUAL,
                    BaseConnectionlessQueryTest.A_INTEGER,
                    0),
                constantComparison(
                    CompareOp.EQUAL,
                    BaseConnectionlessQueryTest.A_STRING,
                    "foo"))),
            filter);
    }

    @Test
    public void testRHSLiteral() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and 0 >= a_integer";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        List<Object> binds = Collections.emptyList();
        Scan scan = new Scan();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 1, null);
        Filter filter = scan.getFilter();
        assertEquals(
            singleKVFilter(constantComparison(
                CompareOp.LESS_OR_EQUAL,
                BaseConnectionlessQueryTest.A_INTEGER,
                0)),
            filter);
    }

    @Test
    public void testToDateFilter() throws Exception {
        String tenantId = "000000000000001";
        String dateStr = "2012-01-01 12:00:00";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_date >= to_date('" + dateStr + "')";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        List<Object> binds = Collections.emptyList();
        Scan scan = new Scan();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 1, null);
        Filter filter = scan.getFilter();

        Format format = DateUtil.getDateParser(DateUtil.DEFAULT_DATE_FORMAT);
        Object date = format.parseObject(dateStr);

        assertEquals(
            singleKVFilter(constantComparison(
                CompareOp.GREATER_OR_EQUAL,
                BaseConnectionlessQueryTest.A_DATE,
                date)),
            filter);
    }

    private void helpTestToNumberFilter(String toNumberClause, BigDecimal expectedDecimal) throws Exception {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and x_decimal >= " + toNumberClause;
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, emptyList(), scan);
        statement = compileStatement(context, statement, resolver, emptyList(), scan, 1, null);
        Filter filter = scan.getFilter();

        assertEquals(
            singleKVFilter(constantComparison(
                CompareOp.GREATER_OR_EQUAL,
                BaseConnectionlessQueryTest.X_DECIMAL,
                expectedDecimal)),
            filter);
}

    private void helpTestToNumberFilterWithNoPattern(String stringValue) throws Exception {
        String toNumberClause = "to_number('" + stringValue + "')";
        BigDecimal expectedDecimal = NumberUtil.normalize(new BigDecimal(stringValue));
        helpTestToNumberFilter(toNumberClause, expectedDecimal);
    }

    @Test
    public void testToNumberFilterWithInteger() throws Exception {
        String stringValue = "123";
        helpTestToNumberFilterWithNoPattern(stringValue);
    }

    @Test
    public void testToNumberFilterWithDecimal() throws Exception {
        String stringValue = "123.33";
        helpTestToNumberFilterWithNoPattern(stringValue);
    }

    @Test
    public void testToNumberFilterWithNegativeDecimal() throws Exception {
        String stringValue = "-123.33";
        helpTestToNumberFilterWithNoPattern(stringValue);
    }

    @Test
    public void testToNumberFilterWithPatternParam() throws Exception {
        String toNumberClause = "to_number('$1.23333E2', '\u00A40.00000E0')";
        BigDecimal expectedDecimal = NumberUtil.normalize(new BigDecimal("123.333"));
        helpTestToNumberFilter(toNumberClause, expectedDecimal);
    }

    @Test(expected=AssertionError.class) // compileStatement() fails because zero rows are found by to_number()
    public void testToNumberFilterWithPatternParamNegativeTest() throws Exception {
        String toNumberClause = "to_number('$123.33', '000.00')"; // no currency sign in pattern param
        BigDecimal expectedDecimal = NumberUtil.normalize(new BigDecimal("123.33"));
        helpTestToNumberFilter(toNumberClause, expectedDecimal);
    }

    @Test
    public void testRowKeyFilter() throws SQLException {
        String keyPrefix = "foo";
        String query = "select * from atable where substr(entity_id,1,3)=?";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        List<Object> binds = Arrays.<Object>asList(keyPrefix);
        Scan scan = new Scan();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 0, null);
        Filter filter = scan.getFilter();

        assertEquals(
            new RowKeyComparisonFilter(
                constantComparison(CompareOp.EQUAL,
                    new SubstrFunction(
                        Arrays.<Expression>asList(
                            new RowKeyColumnExpression(BaseConnectionlessQueryTest.ENTITY_ID,new RowKeyValueAccessor(BaseConnectionlessQueryTest.ATABLE.getPKColumns(),1)),
                            LiteralExpression.newConstant(1),
                            LiteralExpression.newConstant(3))
                        ),
                    keyPrefix), QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES),
            filter);
    }

    @Test
    public void testDegenerateRowKeyFilter() throws SQLException {
        String keyPrefix = "foobar";
        String query = "select * from atable where substr(entity_id,1,3)=?";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        List<Object> binds = Arrays.<Object>asList(keyPrefix);
        Scan scan = new Scan();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 0, null);
        // Degenerate b/c "foobar" is more than 3 characters
        assertDegenerate(context);
    }

    @Test
    public void testOrFilter() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "foo";
        int aInt = 2;
        String query = "select * from atable where organization_id=? and (substr(entity_id,1,3)=? or a_integer=?)";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        List<Object> binds = Arrays.<Object>asList(tenantId, keyPrefix, aInt);
        Scan scan = new Scan();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 1, null);
        Filter filter = scan.getFilter();
        assertEquals(
            singleKVFilter( // single b/c one column is a row key column
            or(
                constantComparison(
                    CompareOp.EQUAL,
                    new SubstrFunction(Arrays.<Expression> asList(
                        new RowKeyColumnExpression(
                            BaseConnectionlessQueryTest.ENTITY_ID,
                            new RowKeyValueAccessor(BaseConnectionlessQueryTest.ATABLE.getPKColumns(), 1)),
                        LiteralExpression.newConstant(1),
                        LiteralExpression.newConstant(3))),
                    keyPrefix),
                constantComparison(
                    CompareOp.EQUAL,
                    BaseConnectionlessQueryTest.A_INTEGER,
                    aInt))),
            filter);
    }

    @Test
    public void testTypeMismatch() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_integer > 'foo'";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        List<Object> binds = Collections.emptyList();
        Scan scan = new Scan();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);

        try {
            StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
            compileStatement(context, statement, resolver, binds, scan, 1, null);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Type mismatch"));
        }
    }

    @Test
    public void testAndFalseFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_integer=0 and 2=3";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 0, null);
        assertDegenerate(context);
    }

    @Test
    public void testFalseFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and 2=3";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 0, null);
        assertDegenerate(context);
    }

    @Test
    public void testTrueFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and 2<=2";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 1, null);
        assertNull(scan.getFilter());
        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = startRow;
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());
    }

    @Test
    public void testAndTrueFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_integer=0 and 2<3";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 1, null);
        Filter filter = scan.getFilter();
        assertEquals(
            singleKVFilter(constantComparison(
                CompareOp.EQUAL,
                BaseConnectionlessQueryTest.A_INTEGER,
                0)),
            filter);

        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = startRow;
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());
    }

    @Test
    public void testOrFalseFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and (a_integer=0 or 3!=3)";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 1, null);
        Filter filter = scan.getFilter();
        assertEquals(
            singleKVFilter(constantComparison(
                CompareOp.EQUAL,
                BaseConnectionlessQueryTest.A_INTEGER,
                0)),
            filter);
        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = startRow;
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());
    }

    @Test
    public void testOrTrueFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and (a_integer=0 or 3>2)";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 1, null);
        Filter filter = scan.getFilter();
        assertNull(filter);
        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = startRow;
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());
    }

    @Test
    public void testInFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_string IN ('a','b')";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 1, null);
        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = startRow;
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());

        Filter filter = scan.getFilter();
        assertEquals(
            singleKVFilter(in(
                kvColumn(BaseConnectionlessQueryTest.A_STRING),
                PDataType.VARCHAR,
                "a",
                "b")),
            filter);
    }

    @Test
    public void testInListFilter() throws SQLException {
        String tenantId1 = "000000000000001";
        String tenantId2 = "000000000000002";
        String tenantId3 = "000000000000003";
        String query = String.format("select * from %s where organization_id IN ('%s','%s','%s')",
                ATABLE_NAME, tenantId1, tenantId3, tenantId2);
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);

        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 1, null);
        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId1);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = PDataType.VARCHAR.toBytes(tenantId3);
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());

        Filter filter = scan.getFilter();
        assertEquals(
            new SkipScanFilter(
                ImmutableList.of(Arrays.asList(
                    pointRange(tenantId1),
                    pointRange(tenantId2),
                    pointRange(tenantId3))),
                context.getResolver().getTables().get(0).getTable().getRowKeySchema()),
            filter);
    }

    @Test @Ignore("OR not yet optimized")
    public void testOr2InFilter() throws SQLException {
        String tenantId1 = "000000000000001";
        String tenantId2 = "000000000000002";
        String tenantId3 = "000000000000003";
        String query = String.format("select * from %s where organization_id='%s' OR organization_id='%s' OR organization_id='%s'",
                ATABLE_NAME, tenantId1, tenantId3, tenantId2);
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);

        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 0, null);

        Filter filter = scan.getFilter();
        assertEquals(
            new SkipScanFilter(
                ImmutableList.of(Arrays.asList(
                    pointRange(tenantId1),
                    pointRange(tenantId2),
                    pointRange(tenantId3))),
                context.getResolver().getTables().get(0).getTable().getRowKeySchema()),
            filter);

        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId1);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = PDataType.VARCHAR.toBytes(tenantId3);
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());
    }

    @Test
    public void testSecondPkColInListFilter() throws SQLException {
        String tenantId = "000000000000001";
        String entityId1 = "00000000000000X";
        String entityId2 = "00000000000000Y";
        String query = String.format("select * from %s where organization_id='%s' AND entity_id IN ('%s','%s')",
                ATABLE_NAME, tenantId, entityId1, entityId2);
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);

        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 2, null);
        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId + entityId1);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = PDataType.VARCHAR.toBytes(tenantId + entityId2);
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());

        Filter filter = scan.getFilter();

        assertEquals(
            new SkipScanFilter(
                ImmutableList.of(
                    Arrays.asList(pointRange(tenantId)),
                    Arrays.asList(
                        pointRange(entityId1),
                        pointRange(entityId2))),
                context.getResolver().getTables().get(0).getTable().getRowKeySchema()),
            filter);
    }

    @Test
    public void testInListWithAnd1GTEFilter() throws SQLException {
        String tenantId1 = "000000000000001";
        String tenantId2 = "000000000000002";
        String tenantId3 = "000000000000003";
        String entityId1 = "00000000000000X";
        String entityId2 = "00000000000000Y";
        String query = String.format("select * from %s where organization_id IN ('%s','%s','%s') AND entity_id>='%s' AND entity_id<='%s'",
                ATABLE_NAME, tenantId1, tenantId3, tenantId2, entityId1, entityId2);
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);

        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 3, null);
        Filter filter = scan.getFilter();
        assertEquals(
            new SkipScanFilter(
                ImmutableList.of(
                    Arrays.asList(
                        pointRange(tenantId1),
                        pointRange(tenantId2),
                        pointRange(tenantId3)),
                    Arrays.asList(PDataType.CHAR.getKeyRange(
                        Bytes.toBytes(entityId1),
                        true,
                        Bytes.toBytes(entityId2),
                        true))),
                context.getResolver().getTables().get(0).getTable().getRowKeySchema()),
            filter);
    }
    @Test
    public void testInListWithAnd1Filter() throws SQLException {
        String tenantId1 = "000000000000001";
        String tenantId2 = "000000000000002";
        String tenantId3 = "000000000000003";
        String entityId = "00000000000000X";
        String query = String.format("select * from %s where organization_id IN ('%s','%s','%s') AND entity_id='%s'",
                ATABLE_NAME, tenantId1, tenantId3, tenantId2, entityId);
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);

        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 2, null);
        Filter filter = scan.getFilter();
        assertEquals(
            new SkipScanFilter(
                ImmutableList.of(
                    Arrays.asList(
                        pointRange(tenantId1),
                        pointRange(tenantId2),
                        pointRange(tenantId3)),
                    Arrays.asList(pointRange(entityId))),
                context.getResolver().getTables().get(0).getTable().getRowKeySchema()),
            filter);
    }
    @Test
    public void testInListWithAnd1FilterScankey() throws SQLException {
        String tenantId1 = "000000000000001";
        String tenantId2 = "000000000000002";
        String tenantId3 = "000000000000003";
        String entityId = "00000000000000X";
        String query = String.format("select * from %s where organization_id IN ('%s','%s','%s') AND entity_id='%s'",
                ATABLE_NAME, tenantId1, tenantId3, tenantId2, entityId);
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);

        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 2, null);
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId1), PDataType.VARCHAR.toBytes(entityId));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId3), PDataType.VARCHAR.toBytes(entityId));
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());
        // TODO: validate scan ranges
    }

    private static KeyRange pointRange(String id) {
        return pointRange(Bytes.toBytes(id));
    }
    private static KeyRange pointRange(byte[] bytes) {
        return KeyRange.POINT.apply(bytes);
    }

    @Test
    public void testInListWithAnd2Filter() throws SQLException {
        String tenantId1 = "000000000000001";
        String tenantId2 = "000000000000002";
        String tenantId3 = "000000000000003";
        String entityId1 = "00000000000000X";
        String entityId2 = "00000000000000Y";
        String query = String.format("select * from %s where organization_id IN ('%s','%s','%s') AND entity_id IN ('%s', '%s')",
                ATABLE_NAME, tenantId1, tenantId3, tenantId2, entityId1, entityId2);
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);

        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 2, null);

        Filter filter = scan.getFilter();
        assertEquals(
            new SkipScanFilter(
                ImmutableList.of(
                    Arrays.asList(
                        pointRange(tenantId1),
                        pointRange(tenantId2),
                        pointRange(tenantId3)),
                    Arrays.asList(
                        pointRange(entityId1),
                        pointRange(entityId2))),
                context.getResolver().getTables().get(0).getTable().getRowKeySchema()),
            filter);
    }

    @Test
    public void testPartialRangeFilter() throws SQLException {
        // I know these id's are ridiculous, but users can write queries that look like this
        String tenantId1 = "001";
        String tenantId2 = "02";
        String query = String.format("select * from %s where organization_id > '%s' AND organization_id < '%s'",
                ATABLE_NAME, tenantId1, tenantId2);
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);

        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 2, null);

        assertNull(scan.getFilter());
        byte[] wideLower = ByteUtil.nextKey(ByteUtil.fillKey(Bytes.toBytes(tenantId1), 15));
        byte[] wideUpper = ByteUtil.fillKey(Bytes.toBytes(tenantId2), 15);
        assertArrayEquals(wideLower, scan.getStartRow());
        assertArrayEquals(wideUpper, scan.getStopRow());
    }

    @Test
    public void testInListWithAnd2FilterScanKey() throws SQLException {
        String tenantId1 = "000000000000001";
        String tenantId2 = "000000000000002";
        String tenantId3 = "000000000000003";
        String entityId1 = "00000000000000X";
        String entityId2 = "00000000000000Y";
        String query = String.format("select * from %s where organization_id IN ('%s','%s','%s') AND entity_id IN ('%s', '%s')",
                ATABLE_NAME, tenantId1, tenantId3, tenantId2, entityId1, entityId2);
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);

        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 2, null);
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId1),PDataType.VARCHAR.toBytes(entityId1));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId3),PDataType.VARCHAR.toBytes(entityId2));
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());
        // TODO: validate scan ranges
    }
    
    @Test
    public void testBetweenFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_integer between 0 and 10";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 1, null);
        Filter filter = scan.getFilter();
        assertEquals(
                singleKVFilter(and(
                    constantComparison(
                        CompareOp.GREATER_OR_EQUAL,
                        BaseConnectionlessQueryTest.A_INTEGER,
                        0),
                    constantComparison(
                        CompareOp.LESS_OR_EQUAL,
                        BaseConnectionlessQueryTest.A_INTEGER,
                        10))),
                filter);
    }
    
    @Test
    public void testNotBetweenFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_integer not between 0 and 10";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        statement = compileStatement(context, statement, resolver, binds, scan, 1, null);
        Filter filter = scan.getFilter();
        assertEquals(
                singleKVFilter(not(and(
                    constantComparison(
                        CompareOp.GREATER_OR_EQUAL,
                        BaseConnectionlessQueryTest.A_INTEGER,
                        0),
                    constantComparison(
                        CompareOp.LESS_OR_EQUAL,
                        BaseConnectionlessQueryTest.A_INTEGER,
                        10)))).toString(),
                filter.toString());
    }
}
