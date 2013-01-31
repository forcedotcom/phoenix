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

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.query.BaseConnectionlessQueryTest;
import com.salesforce.phoenix.schema.ColumnNotFoundException;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.ByteUtil;



public class WhereClauseScanKeyTest extends BaseConnectionlessQueryTest {

    private static void compileStatement(String query, Scan scan, List<Object> binds) throws SQLException {
        compileStatement(query, scan, binds, null, null);
    }

    private static void compileStatement(String query, Scan scan, List<Object> binds, Integer limit) throws SQLException {
        compileStatement(query, scan, binds, limit, null);
    }

    private static void compileStatement(String query, Scan scan, List<Object> binds, Set<Expression> extractedNodes) throws SQLException {
        compileStatement(query, scan, binds, null, extractedNodes);
    }

    private static void compileStatement(String query, Scan scan, List<Object> binds, Integer limit, Set<Expression> extractedNodes) throws SQLException {
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        statement = RHSLiteralStatementRewriter.normalizeWhereClause(statement);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(pconn, resolver, binds, statement.getBindCount(), scan);

        Integer actualLimit = LimitCompiler.getLimit(context, statement.getLimit());
        assertEquals(limit, actualLimit);
        GroupBy groupBy = GroupByCompiler.getGroupBy(statement, context);
        statement = HavingCompiler.moveToWhereClause(statement, context, groupBy);
        WhereCompiler.compileWhereClause(context, statement.getWhere(), extractedNodes);
    }

    @Test
    public void testSingleKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds);
        
        assertNull(scan.getFilter());
        assertTrue(Bytes.compareTo(scan.getStartRow(), PDataType.VARCHAR.toBytes(tenantId)) == 0);
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId))) == 0);
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
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(keyPrefix));
        assertTrue(Bytes.compareTo(scan.getStartRow(), startRow) == 0);
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(startRow)) == 0);
    }

    @Test
    public void testMultiKeyBindExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id=? and substr(entity_id,1,3)=?";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(tenantId,keyPrefix);
        compileStatement(query, scan, binds);
        assertNull(scan.getFilter());
        
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(keyPrefix));
        assertTrue(Bytes.compareTo(scan.getStartRow(), startRow) == 0);
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(startRow)) == 0);
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
        assertNull(scan.getFilter());
        
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(entityId));
        assertTrue(Bytes.compareTo(scan.getStartRow(), startRow) == 0);
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(startRow)) == 0);
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

        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId.substring(0,3));
        assertTrue(Bytes.compareTo(scan.getStartRow(), startRow) == 0);
        byte[] stopRow = startRow;
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(stopRow)) == 0);
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

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(keyPrefix1));
        assertTrue(Bytes.compareTo(scan.getStartRow(), startRow) == 0);
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(keyPrefix2));
        assertTrue(Bytes.compareTo(scan.getStopRow(), stopRow) == 0);
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

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(keyPrefix1));
        assertTrue(Bytes.compareTo(scan.getStartRow(), startRow) == 0);
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(keyPrefix2));
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(stopRow)) == 0);
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

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(keyPrefix1));
        assertTrue(Bytes.compareTo(scan.getStartRow(), ByteUtil.nextKey(startRow)) == 0);
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(keyPrefix2));
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(stopRow)) == 0);
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
        assertTrue(Bytes.compareTo(scan.getStartRow(), startRow) == 0);
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(entityId));
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(stopRow)) == 0);
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
        assertTrue(Bytes.compareTo(scan.getStartRow(), startRow) == 0);
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(keyPrefix1),new byte[entityId.length() - keyPrefix1.length()]);
        assertTrue(Bytes.compareTo(scan.getStopRow(), stopRow) == 0);
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
        assertTrue(Bytes.compareTo(scan.getStartRow(), startRow) == 0);
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(entityId));
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(stopRow)) == 0);
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
        assertTrue(Bytes.compareTo(scan.getStartRow(), PDataType.VARCHAR.toBytes(tenantId)) == 0);
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId))) == 0);
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
        assertTrue(Bytes.compareTo(scan.getStartRow(), PDataType.VARCHAR.toBytes(tenantId)) == 0);
        assertTrue(Bytes.compareTo(scan.getStopRow(), HConstants.EMPTY_END_ROW) == 0);
    }

    @Test
    public void testRHSLiteral() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and 0 >= a_integer limit 1000";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, scan, binds, 1000);
        
        assertNotNull(scan.getFilter());
        assertTrue(Bytes.compareTo(scan.getStartRow(), PDataType.VARCHAR.toBytes(tenantId)) == 0);
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId))) == 0);
    }
    

    @Test
    public void testKeyTypeMismatch() throws SQLException {
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
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(keyPrefix));
        assertTrue(Bytes.compareTo(scan.getStartRow(), startRow) == 0);
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(startRow)) == 0);
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
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(keyPrefix));
        assertTrue(Bytes.compareTo(scan.getStartRow(), startRow) == 0);
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(startRow)) == 0);
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
        
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(keyPrefix));
        assertTrue(Bytes.compareTo(scan.getStartRow(), startRow) == 0);
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(startRow)) == 0);
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
        
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(keyPrefix));
        assertTrue(Bytes.compareTo(scan.getStartRow(), startRow) == 0);
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(startRow)) == 0);
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
        
        
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(keyPrefix));
        assertTrue(Bytes.compareTo(scan.getStartRow(), startRow) == 0);
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(startRow)) == 0);
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
        assertTrue(Bytes.compareTo(scan.getStartRow(), startRow) == 0);
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(startRow)) == 0);
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
        assertTrue(Bytes.compareTo(scan.getStartRow(), startRow) == 0);
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(startRow)) == 0);
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
        assertTrue(Bytes.compareTo(scan.getStartRow(), startRow) == 0);
        assertTrue(Bytes.compareTo(scan.getStopRow(), ByteUtil.nextKey(startRow)) == 0);
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
}
