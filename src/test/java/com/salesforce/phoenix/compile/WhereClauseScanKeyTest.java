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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.junit.Test;

import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.OrExpression;
import com.salesforce.phoenix.filter.RowKeyComparisonFilter;
import com.salesforce.phoenix.filter.SkipScanFilter;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.schema.ColumnNotFoundException;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.DateUtil;



public class WhereClauseScanKeyTest extends BaseConnectionlessQueryTest {

    private static StatementContext compileStatement(String query, Scan scan, List<Object> binds) throws SQLException {
        return compileStatement(query, scan, binds, null, null);
    }

    private static StatementContext compileStatement(String query, Scan scan, List<Object> binds, Integer limit) throws SQLException {
        return compileStatement(query, scan, binds, limit, null);
    }

    private static StatementContext compileStatement(String query, Scan scan, List<Object> binds, Set<Expression> extractedNodes) throws SQLException {
        return compileStatement(query, scan, binds, null, extractedNodes);
    }

    private static StatementContext compileStatement(String query, Scan scan, List<Object> binds, Integer limit, Set<Expression> extractedNodes) throws SQLException {
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        statement = StatementNormalizer.normalize(statement);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(statement, pconn, resolver, binds, scan);
        Map<String, ParseNode> aliasParseNodeMap = ProjectionCompiler.buildAliasMap(context, statement);

        Integer actualLimit = LimitCompiler.compile(context, statement);
        assertEquals(limit, actualLimit);
        GroupBy groupBy = GroupByCompiler.compile(context, statement, aliasParseNodeMap);
        statement = HavingCompiler.rewrite(context, statement, groupBy);
        WhereCompiler.compileWhereClause(context, statement, extractedNodes);
        return context;
    }

    @Test
    public void testSingleKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);

        compileStatement(query, scan, binds);
        assertArrayEquals(PDataType.VARCHAR.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStopRow());
    }

    @Test
    public void testReverseSingleKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where '" + tenantId + "' = organization_id";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);

        compileStatement(query, scan, binds);
        assertArrayEquals(PDataType.VARCHAR.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStopRow());
    }

    @Test
    public void testStartKeyStopKey() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE start_stop_test (pk char(2) not null primary key)");
        conn.close();

        String query = "select * from start_stop_test where pk >= 'EA' and pk < 'EZ'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);

        assertNull(scan.getFilter());
        assertArrayEquals(PDataType.VARCHAR.toBytes("EA"), scan.getStartRow());
        assertArrayEquals(PDataType.VARCHAR.toBytes("EZ"), scan.getStopRow());
    }

    @Test
    public void testConcatSingleKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id || 'foo' ='" + tenantId + "'||'foo'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);

        // The || operator cannot currently be used to form the start/stop key
        assertNotNull(scan.getFilter());
        assertEquals(0, scan.getStartRow().length);
        assertEquals(0, scan.getStopRow().length);
    }

    @Test
    public void testLiteralConcatExpression() throws SQLException {
        String query = "select * from atable where null||'foo'||'bar' = 'foobar'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);

        assertNull(scan.getFilter());
        assertEquals(0, scan.getStartRow().length);
        assertEquals(0, scan.getStopRow().length);
    }

    @Test
    public void testSingleKeyNotExpression() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where not organization_id='" + tenantId + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);
        assertNotNull(scan.getFilter());

        assertEquals(0, scan.getStartRow().length);
        assertEquals(0, scan.getStopRow().length);
    }

    @Test
    public void testMultiKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3)='" + keyPrefix + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix)), 15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testMultiKeyBindExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id=? and substr(entity_id,1,3)=?";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(tenantId,keyPrefix);
        compileStatement(query, scan, binds);

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testEqualRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date startDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date endDate = DateUtil.parseDate("2012-01-02 00:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')=?";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        compileStatement(query, scan, binds);

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.DATE.toBytes(startDate));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.DATE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testDegenerateRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date startDate = DateUtil.parseDate("2012-01-01 01:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')=?";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        compileStatement(query, scan, binds);
        assertDegenerate(scan);
    }

    @Test
    public void testBoundaryGreaterThanRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date startDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date endDate = DateUtil.parseDate("2012-01-02 00:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')>?";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        compileStatement(query, scan, binds);

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.DATE.toBytes(endDate));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.nextKey(ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testBoundaryGreaterThanOrEqualRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date startDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date endDate = DateUtil.parseDate("2012-01-01 00:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')>=?";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        compileStatement(query, scan, binds);

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.DATE.toBytes(endDate));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.nextKey(ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testGreaterThanRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date startDate = DateUtil.parseDate("2012-01-01 01:00:00");
        Date endDate = DateUtil.parseDate("2012-01-02 00:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')>?";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        compileStatement(query, scan, binds);

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.DATE.toBytes(endDate));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.nextKey(ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testLessThanRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date startDate = DateUtil.parseDate("2012-01-01 01:00:00");
        Date endDate = DateUtil.parseDate("2012-01-02 00:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')<?";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        compileStatement(query, scan, binds);

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.DATE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testBoundaryLessThanRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date startDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date endDate = DateUtil.parseDate("2012-01-01 00:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')<?";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        compileStatement(query, scan, binds);

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.DATE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testLessThanOrEqualRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date startDate = DateUtil.parseDate("2012-01-01 01:00:00");
        Date endDate = DateUtil.parseDate("2012-01-02 00:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')<=?";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        compileStatement(query, scan, binds);

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.DATE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testBoundaryLessThanOrEqualRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date startDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date endDate = DateUtil.parseDate("2012-01-02 00:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')<=?";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        compileStatement(query, scan, binds);

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.DATE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testOverlappingKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String entityId = "002333333333333";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3)='" + keyPrefix + "' and entity_id='" + entityId + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(entityId));
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(startRow), scan.getStopRow());
    }

    @Test
    public void testTrailingSubstrExpression() throws SQLException {
        String tenantId = "0xD000000000001";
        String entityId = "002333333333333";
        String query = "select * from atable where substr(organization_id,1,3)='" + tenantId.substring(0, 3) + "' and entity_id='" + entityId + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);
        assertNotNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(ByteUtil.fillKey(PDataType.VARCHAR.toBytes(tenantId.substring(0,3)),15),PDataType.VARCHAR.toBytes(entityId));
        assertArrayEquals(startRow, scan.getStartRow());
        // Even though the first slot is a non inclusive range, we need to do a next key
        // on the second slot because of the algorithm we use to seek to and terminate the
        // loop during skip scan. We could end up having a first slot just under the upper
        // limit of slot one and a value equal to the value in slot two and we need this to
        // be less than the upper range that would get formed.
        byte[] stopRow = ByteUtil.concat(ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId.substring(0,3))),15),ByteUtil.nextKey(PDataType.VARCHAR.toBytes(entityId)));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testBasicRangeExpression() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id <= '" + tenantId + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);
        assertNull(scan.getFilter());

        assertTrue(scan.getStartRow().length == 0);
        byte[] stopRow = ByteUtil.concat(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testKeyRangeExpression1() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "002";
        String keyPrefix2= "004";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) >= '" + keyPrefix1 + "' and substr(entity_id,1,3) < '" + keyPrefix2 + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix1),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix2),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testKeyRangeExpression2() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "002";
        String keyPrefix2= "004";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) >= '" + keyPrefix1 + "' and substr(entity_id,1,3) <= '" + keyPrefix2 + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix1),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix2)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testKeyRangeExpression3() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "002";
        String keyPrefix2= "004";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) > '" + keyPrefix1 + "' and substr(entity_id,1,3) <= '" + keyPrefix2 + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);

        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix1)),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix2)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testKeyRangeExpression4() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "002";
        String entityId= "002000000000002";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) > '" + keyPrefix1 + "' and substr(entity_id,1,3) = '" + entityId + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);
        assertDegenerate(scan);
    }

    @Test
    public void testKeyRangeExpression5() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "002";
        String entityId= "002000000000002";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) <= '" + keyPrefix1 + "' and entity_id = '" + entityId + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(entityId));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(entityId));
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());
    }

    @Test
    public void testKeyRangeExpression6() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "002";
        String entityId= "002000000000002";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) < '" + keyPrefix1 + "' and entity_id = '" + entityId + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);
        assertDegenerate(scan);
    }

    @Test
    public void testKeyRangeExpression7() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "002";
        String entityId= "002000000000002";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) < '" + keyPrefix1 + "' and entity_id < '" + entityId + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);
        assertNull(scan.getFilter());

        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(keyPrefix1),new byte[entityId.length() - keyPrefix1.length()]);
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testKeyRangeExpression8() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "001";
        String entityId= "002000000000002";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) > '" + keyPrefix1 + "' and entity_id = '" + entityId + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(entityId));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(entityId));
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());
    }

    @Test
    public void testKeyRangeExpression9() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "002";
        String keyPrefix2 = "0033";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) >= '" + keyPrefix1 + "' and substr(entity_id,1,4) <= '" + keyPrefix2 + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix1),15)); // extra byte is due to implicit internal padding
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix2)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    /**
     * This is testing the degenerate case where nothing will match because the overlapping keys (keyPrefix and entityId) don't match.
     * @throws SQLException
     */
    @Test
    public void testUnequalOverlappingKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String entityId = "001333333333333";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3)='" + keyPrefix + "' and entity_id='" + entityId + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);
        assertDegenerate(scan);
    }

    @Test
    public void testTopLevelOrKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' or a_integer=2";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);

        assertNotNull(scan.getFilter());
        assertEquals(0, scan.getStartRow().length);
        assertEquals(0, scan.getStopRow().length);
    }

    @Test
    public void testSiblingOrKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and (a_integer = 2 or a_integer = 3)";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);

        assertNotNull(scan.getFilter());
        assertArrayEquals(PDataType.VARCHAR.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStopRow());
    }

    @Test
    public void testColumnNotFound() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where bar='" + tenantId + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        try {
            compileStatement(query, scan, binds);
            fail();
        } catch (ColumnNotFoundException e) {
            // expected
        }
    }

    @Test
    public void testNotContiguousPkColumn() throws SQLException {
        String keyPrefix = "002";
        String query = "select * from atable where substr(entity_id,1,3)='" + keyPrefix + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);

        assertNotNull(scan.getFilter());
        assertEquals(0, scan.getStartRow().length);
        assertEquals(0, scan.getStopRow().length);
    }

    @Test
    public void testMultipleNonEqualitiesPkColumn() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id >= '" + tenantId + "' AND substr(entity_id,1,3) > '" + keyPrefix + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);

        assertNotNull(scan.getFilter());
        assertArrayEquals(PDataType.VARCHAR.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }

    @Test
    public void testRHSLiteral() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and 0 >= a_integer limit 1000";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds, 1000);

        assertNotNull(scan.getFilter());
        assertArrayEquals(PDataType.VARCHAR.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStopRow());
    }


    @Test
    public void testKeyTypeMismatch() {
        String query = "select * from atable where organization_id=5";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        try {
            compileStatement(query, scan, binds);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Type mismatch"));
        }
    }

    @Test
    public void testLikeExtractAllKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id = ? and entity_id  LIKE '" + keyPrefix + "%'";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(tenantId);
        compileStatement(query, scan, binds);

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testLikeExtractAllAsEqKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id LIKE ? and entity_id  LIKE '" + keyPrefix + "%'";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(tenantId);
        compileStatement(query, scan, binds);

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testDegenerateLikeNoWildcard() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id LIKE ? and entity_id  LIKE '" + keyPrefix + "'";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(tenantId);
        compileStatement(query, scan, binds);
        assertDegenerate(scan);
    }

    @Test
    public void testLikeExtractKeyExpression2() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        // TODO: verify that _ at end of like doesn't go to equals
        String query = "select * from atable where organization_id = ? and entity_id  LIKE '" + keyPrefix + "_'";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(tenantId);
        compileStatement(query, scan, binds);

        assertNotNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testLikeOptKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id = ? and entity_id  LIKE '" + keyPrefix + "%003%'";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(tenantId);
        Set<Expression>extractedNodes = new HashSet<Expression>();
        compileStatement(query, scan, binds, extractedNodes);

        assertNotNull(scan.getFilter());
        assertEquals(1, extractedNodes.size());

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testLikeOptKeyExpression2() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id = ? and substr(entity_id,1,10)  LIKE '" + keyPrefix + "%003%'";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(tenantId);
        Set<Expression>extractedNodes = new HashSet<Expression>();
        compileStatement(query, scan, binds, extractedNodes);

        assertNotNull(scan.getFilter());
        assertEquals(1, extractedNodes.size());


        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix),15));
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix)),15));
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testLikeNoOptKeyExpression3() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id = ? and substr(entity_id,4,10)  LIKE '" + keyPrefix + "%003%'";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(tenantId);
        Set<Expression>extractedNodes = new HashSet<Expression>();
        compileStatement(query, scan, binds, extractedNodes);

        assertNotNull(scan.getFilter());
        assertEquals(1, extractedNodes.size());
        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(startRow), scan.getStopRow());
    }

    @Test
    public void testLikeDegenerate() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id = ? and entity_id  LIKE '0000000000000012%003%'";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(tenantId);
        Set<Expression>extractedNodes = new HashSet<Expression>();
        compileStatement(query, scan, binds, extractedNodes);

        assertDegenerate(scan);
    }

    @Test
    public void testDegenerateDivision1() throws SQLException {
        String query = "select * from atable where a_integer = 3 / null";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList();
        Set<Expression>extractedNodes = new HashSet<Expression>();
        compileStatement(query, scan, binds, extractedNodes);

        assertDegenerate(scan);
    }

    @Test
    public void testDegenerateDivision2() throws SQLException {
        String query = "select * from atable where a_integer / null = 3";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList();
        Set<Expression>extractedNodes = new HashSet<Expression>();
        compileStatement(query, scan, binds, extractedNodes);

        assertDegenerate(scan);
    }

    @Test
    public void testDegenerateMult1() throws SQLException {
        String query = "select * from atable where a_integer = 3 * null";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList();
        Set<Expression>extractedNodes = new HashSet<Expression>();
        compileStatement(query, scan, binds, extractedNodes);

        assertDegenerate(scan);
    }

    @Test
    public void testDegenerateMult2() throws SQLException {
        String query = "select * from atable where a_integer * null = 3";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList();
        Set<Expression>extractedNodes = new HashSet<Expression>();
        compileStatement(query, scan, binds, extractedNodes);

        assertDegenerate(scan);
    }

    @Test
    public void testDegenerateAdd1() throws SQLException {
        String query = "select * from atable where a_integer = 3 + null";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList();
        Set<Expression>extractedNodes = new HashSet<Expression>();
        compileStatement(query, scan, binds, extractedNodes);

        assertDegenerate(scan);
    }

    @Test
    public void testDegenerateAdd2() throws SQLException {
        String query = "select * from atable where a_integer + null = 3";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList();
        Set<Expression>extractedNodes = new HashSet<Expression>();
        compileStatement(query, scan, binds, extractedNodes);

        assertDegenerate(scan);
    }

    @Test
    public void testDegenerateSub1() throws SQLException {
        String query = "select * from atable where a_integer = 3 - null";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList();
        Set<Expression>extractedNodes = new HashSet<Expression>();
        compileStatement(query, scan, binds, extractedNodes);

        assertDegenerate(scan);
    }

    @Test
    public void testDegenerateSub2() throws SQLException {
        String query = "select * from atable where a_integer - null = 3";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList();
        Set<Expression>extractedNodes = new HashSet<Expression>();
        compileStatement(query, scan, binds, extractedNodes);

        assertDegenerate(scan);
    }

    @Test
    public void testLikeNoOptKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id = ? and entity_id  LIKE '%001%" + keyPrefix + "%'";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(tenantId);
        Set<Expression>extractedNodes = new HashSet<Expression>();
        compileStatement(query, scan, binds, extractedNodes);

        assertNotNull(scan.getFilter());
        assertEquals(1, extractedNodes.size());
        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(startRow), scan.getStopRow());
    }

    @Test
    public void testLikeNoOptKeyExpression2() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id = ? and entity_id  NOT LIKE '" + keyPrefix + "%'";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(tenantId);
        Set<Expression>extractedNodes = new HashSet<Expression>();
        compileStatement(query, scan, binds, extractedNodes);

        assertNotNull(scan.getFilter());
        assertEquals(1, extractedNodes.size());
        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(startRow), scan.getStopRow());
    }
    /*
     * The following 5 tests are testing the comparison in where clauses under the case when the rhs
     * cannot be coerced into the lhs. We need to confirm the decision make by expression compilation
     * returns correct decisions.
     */
    @Test
    public void testValueComparisonInt() throws SQLException {
        ensureTableCreated(getUrl(),"PKIntValueTest");
        String query;
        // int <-> long
        // Case 1: int = long, comparison always false, key is degenerated.
        query = "SELECT * FROM PKintValueTest where pk = " + Long.MAX_VALUE;
        assertQueryConditionAlwaysFalse(query);
        // Case 2: int != long, comparison always true, no key set since we need to do a full
        // scan all the time.
        query = "SELECT * FROM PKintValueTest where pk != " + Long.MAX_VALUE;
        assertQueryConditionAlwaysTrue(query);
        // Case 3: int > positive long, comparison always false;
        query = "SELECT * FROM PKintValueTest where pk >= " + Long.MAX_VALUE;
        assertQueryConditionAlwaysFalse(query);
        // Case 4: int <= Integer.MAX_VALUE < positive long, always true;
        query = "SELECT * FROM PKintValueTest where pk <= " + Long.MAX_VALUE;
        assertQueryConditionAlwaysTrue(query);
        // Case 5: int >= Integer.MIN_VALUE > negative long, always true;
        query = "SELECT * FROM PKintValueTest where pk >= " + (Long.MIN_VALUE + 1);
        assertQueryConditionAlwaysTrue(query);
        // Case 6: int < negative long, comparison always false;
        query = "SELECT * FROM PKintValueTest where pk <= " + (Long.MIN_VALUE + 1);
        assertQueryConditionAlwaysFalse(query);
    }

    @Test
    public void testValueComparisonUnsignedInt() throws SQLException {
        ensureTableCreated(getUrl(), "PKUnsignedIntValueTest");
        String query;
        // unsigned_int <-> negative int/long
        // Case 1: unsigned_int = negative int, always false;
        query = "SELECT * FROM PKUnsignedIntValueTest where pk = -1";
        assertQueryConditionAlwaysFalse(query);
        // Case 2: unsigned_int != negative int, always true;
        query = "SELECT * FROM PKUnsignedIntValueTest where pk != -1";
        assertQueryConditionAlwaysTrue(query);
        // Case 3: unsigned_int > negative int, always true;
        query = "SELECT * FROM PKUnsignedIntValueTest where pk > " + (Long.MIN_VALUE + 1);
        assertQueryConditionAlwaysTrue(query);
        // Case 4: unsigned_int < negative int, always false;
        query = "SELECT * FROM PKUnsignedIntValueTest where pk < " + + (Long.MIN_VALUE + 1);
        assertQueryConditionAlwaysFalse(query);
        // unsigned_int <-> big positive long
        // Case 1: unsigned_int = big positive long, always false;
        query = "SELECT * FROM PKUnsignedIntValueTest where pk = " + Long.MAX_VALUE;
        assertQueryConditionAlwaysFalse(query);
        // Case 2: unsigned_int != big positive long, always true;
        query = "SELECT * FROM PKUnsignedIntValueTest where pk != " + Long.MAX_VALUE;
        assertQueryConditionAlwaysTrue(query);
        // Case 3: unsigned_int > big positive long, always false;
        query = "SELECT * FROM PKUnsignedIntValueTest where pk >= " + Long.MAX_VALUE;
        assertQueryConditionAlwaysFalse(query);
        // Case 4: unsigned_int < big positive long, always true;
        query = "SELECT * FROM PKUnsignedIntValueTest where pk <= " + Long.MAX_VALUE;
        assertQueryConditionAlwaysTrue(query);
    }

    @Test
    public void testValueComparisonUnsignedLong() throws SQLException {
        ensureTableCreated(getUrl(), "PKUnsignedLongValueTest");
        String query;
        // unsigned_long <-> positive int/long
        // Case 1: unsigned_long = negative int/long, always false;
        query = "SELECT * FROM PKUnsignedLongValueTest where pk = -1";
        assertQueryConditionAlwaysFalse(query);
        // Case 2: unsigned_long = negative int/long, always true;
        query = "SELECT * FROM PKUnsignedLongValueTest where pk != " + (Long.MIN_VALUE + 1);
        assertQueryConditionAlwaysTrue(query);
        // Case 3: unsigned_long > negative int/long, always true;
        query = "SELECT * FROM PKUnsignedLongValueTest where pk > -1";
        assertQueryConditionAlwaysTrue(query);
        // Case 4: unsigned_long < negative int/long, always false;
        query = "SELECT * FROM PKUnsignedLongValueTest where pk < " + (Long.MIN_VALUE + 1);
        assertQueryConditionAlwaysFalse(query);
    }

    private void assertQueryConditionAlwaysTrue(String query) throws SQLException {
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList();
        Set<Expression> extractedNodes = new HashSet<Expression>();
        compileStatement(query, scan, binds, extractedNodes);
        assertEmptyScanKey(scan);
    }

    private void assertQueryConditionAlwaysFalse(String query) throws SQLException {
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList();
        Set<Expression> extractedNodes = new HashSet<Expression>();
        compileStatement(query, scan, binds, extractedNodes);
        assertDegenerate(scan);
    }
    
    @Test
    public void testOrSameColExpression() throws SQLException {
        String tenantId1 = "000000000000001";
        String tenantId2 = "000000000000003";
        String query = "select * from atable where organization_id = ? or organization_id  = ?";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(tenantId1,tenantId2);
        Set<Expression>extractedNodes = new HashSet<Expression>();
        StatementContext context = compileStatement(query, scan, binds, extractedNodes);

        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertTrue(filter instanceof SkipScanFilter);
        ScanRanges scanRanges = context.getScanRanges();
        assertNotNull(scanRanges);
        List<List<KeyRange>> ranges = scanRanges.getRanges();
        assertEquals(1,ranges.size());
        List<List<KeyRange>> expectedRanges = Collections.singletonList(Arrays.asList(
                PDataType.CHAR.getKeyRange(PDataType.CHAR.toBytes(tenantId1), true, PDataType.CHAR.toBytes(tenantId1), true), 
                PDataType.CHAR.getKeyRange(PDataType.CHAR.toBytes(tenantId2), true, PDataType.CHAR.toBytes(tenantId2), true)));
        assertEquals(expectedRanges, ranges);
        assertEquals(1, extractedNodes.size());
        assertTrue(extractedNodes.iterator().next() instanceof OrExpression);
        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId1);
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId2)), scan.getStopRow());
    }
    
    @Test
    public void testAndOrExpression() throws SQLException {
        String tenantId1 = "000000000000001";
        String tenantId2 = "000000000000003";
        String entityId1 = "002333333333331";
        String entityId2 = "002333333333333";
        String query = "select * from atable where (organization_id = ? and entity_id  = ?) or (organization_id = ? and entity_id  = ?)";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(tenantId1,entityId1,tenantId2,entityId2);
        Set<Expression>extractedNodes = new HashSet<Expression>();
        StatementContext context = compileStatement(query, scan, binds, extractedNodes);

        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertTrue(filter instanceof RowKeyComparisonFilter);
        ScanRanges scanRanges = context.getScanRanges();
        assertEquals(ScanRanges.EVERYTHING,scanRanges);
        assertArrayEquals(HConstants.EMPTY_START_ROW, scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    @Test
    public void testOrDiffColExpression() throws SQLException {
        String tenantId1 = "000000000000001";
        String entityId1 = "002333333333331";
        String query = "select * from atable where organization_id = ? or entity_id  = ?";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(tenantId1,entityId1);
        Set<Expression>extractedNodes = new HashSet<Expression>();
        StatementContext context = compileStatement(query, scan, binds, extractedNodes);

        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertTrue(filter instanceof RowKeyComparisonFilter);
        ScanRanges scanRanges = context.getScanRanges();
        assertEquals(ScanRanges.EVERYTHING,scanRanges);
        assertArrayEquals(HConstants.EMPTY_START_ROW, scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    @Test
    public void testOrSameColRangeExpression() throws SQLException {
        String query = "select * from atable where substr(organization_id,1,3) = ? or organization_id LIKE 'foo%'";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList("00D");
        Set<Expression>extractedNodes = new HashSet<Expression>();
        StatementContext context = compileStatement(query, scan, binds, extractedNodes);

        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertTrue(filter instanceof SkipScanFilter);
        ScanRanges scanRanges = context.getScanRanges();
        assertNotNull(scanRanges);
        List<List<KeyRange>> ranges = scanRanges.getRanges();
        assertEquals(1,ranges.size());
        List<List<KeyRange>> expectedRanges = Collections.singletonList(Arrays.asList(
                PDataType.CHAR.getKeyRange(
                        ByteUtil.fillKey(PDataType.CHAR.toBytes("00D"),15), true, 
                        ByteUtil.fillKey(ByteUtil.nextKey(PDataType.CHAR.toBytes("00D")),15), false), 
                PDataType.CHAR.getKeyRange(
                        ByteUtil.fillKey(PDataType.CHAR.toBytes("foo"),15), true, 
                        ByteUtil.fillKey(ByteUtil.nextKey(PDataType.CHAR.toBytes("foo")),15), false)));
        assertEquals(expectedRanges, ranges);
        assertEquals(1, extractedNodes.size());
        assertTrue(extractedNodes.iterator().next() instanceof OrExpression);
    }
    
}
