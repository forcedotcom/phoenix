/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc. All rights reserved. Redistribution and use in source and binary forms, with
 * or without modification, are permitted provided that the following conditions are met: Redistributions of source code
 * must retain the above copyright notice, this list of conditions and the following disclaimer. Redistributions in
 * binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution. Neither the name of Salesforce.com nor the names
 * of its contributors may be used to endorse or promote products derived from this software without specific prior
 * written permission. THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.PHOENIX_JDBC_URL;
import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.*;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Before;
import org.junit.Test;

import com.salesforce.phoenix.exception.PhoenixParserException;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.ColumnFamilyNotFoundException;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.SchemaUtil;

/**
 * Basic tests for Phoenix dynamic family querying "cf.*"
 * 
 * @author nmaillard
 * @since 1.2
 */

@edu.umd.cs.findbugs.annotations.SuppressWarnings(
        value="RV_RETURN_VALUE_IGNORED", 
        justification="Designed to ignore.")
public class DynamicFamilyTest extends BaseHBaseManagedTimeTest {
    private static final String WEB_STATS = "WEB_STATS";
    private static final byte[] A_CF = Bytes.toBytes(SchemaUtil.normalizeIdentifier("A"));
    private static final byte[] B_CF = Bytes.toBytes(SchemaUtil.normalizeIdentifier("B"));
    private static final String USER_ID1 = "u0001";
    private static final String USER_ID2 = "u0002";
    private static final String USER_ID3 = "u0003";
    private static final byte[] USER_ID1_BYTES = Bytes.toBytes(USER_ID1);
    private static final byte[] USER_ID2_BYTES = Bytes.toBytes(USER_ID2);
    private static final byte[] USER_ID3_BYTES = Bytes.toBytes(USER_ID3);
    
    private static final String MAX_CLICK_COUNT_PREFIX = SchemaUtil.normalizeIdentifier("MaxClickCount_");
    private static final byte[] MAX_CLICK_COUNT_DYNCOL_PREFIX = Bytes.toBytes(MAX_CLICK_COUNT_PREFIX);
    private static final Integer ENTRY1_CLICK_COUNT = 12;
    private static final Integer ENTRY2_CLICK_COUNT = 34;
    private static final Integer ENTRY3_CLICK_COUNT = 56;
    
    private static final String LAST_LOGIN_TIME_PREFIX = SchemaUtil.normalizeIdentifier("LastLoginTime_");
    private static final byte[] LAST_LOGIN_TIME_DYNCOL_PREFIX = Bytes.toBytes(LAST_LOGIN_TIME_PREFIX);
    
    private static final Time ENTRY1_USER_ID1_LOGIN_TIME = new Time(System.currentTimeMillis()+60000);
    private static final Time ENTRY1_USER_ID2_LOGIN_TIME = new Time(System.currentTimeMillis()+120000);
    
    private static final Time ENTRY2_USER_ID2_LOGIN_TIME = new Time(System.currentTimeMillis()+180000);
    private static final Time ENTRY2_USER_ID3_LOGIN_TIME = new Time(System.currentTimeMillis()+240000);
    
    private static final Time ENTRY3_USER_ID1_LOGIN_TIME = new Time(System.currentTimeMillis()+300000);
    private static final Time ENTRY3_USER_ID2_LOGIN_TIME = new Time(System.currentTimeMillis()+360000);
    private static final Time ENTRY3_USER_ID3_LOGIN_TIME = new Time(System.currentTimeMillis()+420000);

    @Before
    public void doBeforeTestSetup() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "create table if not exists  " + WEB_STATS
                + "   (entry varchar not null primary key,"
                + "    a.dummy varchar," 
                + "    b.dummy varchar)";
        conn.createStatement().execute(ddl);
        conn.close();
        initTableValues();
    }

    private static void initTableValues() throws Exception {
        ConnectionQueryServices services = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES);
        HTableInterface hTable = services.getTable(SchemaUtil.getTableName(Bytes.toBytes(WEB_STATS)));
        try {
            // Insert rows using standard HBase mechanism with standard HBase "types"
            Put put;
            List<Row> mutations = new ArrayList<Row>();
            put = new Put(Bytes.toBytes("entry1"));
            put.add(A_CF, QueryConstants.EMPTY_COLUMN_BYTES, ByteUtil.EMPTY_BYTE_ARRAY);
            put.add(A_CF, ByteUtil.concat(MAX_CLICK_COUNT_DYNCOL_PREFIX, USER_ID2_BYTES), PDataType.INTEGER.toBytes(ENTRY1_CLICK_COUNT));
            put.add(B_CF, ByteUtil.concat(LAST_LOGIN_TIME_DYNCOL_PREFIX, USER_ID1_BYTES), PDataType.TIME.toBytes(ENTRY1_USER_ID1_LOGIN_TIME));
            put.add(B_CF, ByteUtil.concat(LAST_LOGIN_TIME_DYNCOL_PREFIX, USER_ID2_BYTES), PDataType.TIME.toBytes(ENTRY1_USER_ID2_LOGIN_TIME));
            mutations.add(put);
            
            put = new Put(Bytes.toBytes("entry2"));
            put.add(A_CF, QueryConstants.EMPTY_COLUMN_BYTES, ByteUtil.EMPTY_BYTE_ARRAY);
            put.add(A_CF, ByteUtil.concat(MAX_CLICK_COUNT_DYNCOL_PREFIX, USER_ID3_BYTES), PDataType.INTEGER.toBytes(ENTRY2_CLICK_COUNT));
            put.add(B_CF, ByteUtil.concat(LAST_LOGIN_TIME_DYNCOL_PREFIX, USER_ID2_BYTES), PDataType.TIME.toBytes(ENTRY2_USER_ID2_LOGIN_TIME));
            put.add(B_CF, ByteUtil.concat(LAST_LOGIN_TIME_DYNCOL_PREFIX, USER_ID3_BYTES), PDataType.TIME.toBytes(ENTRY2_USER_ID3_LOGIN_TIME));
            mutations.add(put);
            
            put = new Put(Bytes.toBytes("entry3"));
            put.add(A_CF, QueryConstants.EMPTY_COLUMN_BYTES, ByteUtil.EMPTY_BYTE_ARRAY);
            put.add(A_CF, ByteUtil.concat(MAX_CLICK_COUNT_DYNCOL_PREFIX, USER_ID1_BYTES), PDataType.INTEGER.toBytes(ENTRY3_CLICK_COUNT));
            put.add(B_CF, ByteUtil.concat(LAST_LOGIN_TIME_DYNCOL_PREFIX, USER_ID1_BYTES), PDataType.TIME.toBytes(ENTRY3_USER_ID1_LOGIN_TIME));
            put.add(B_CF, ByteUtil.concat(LAST_LOGIN_TIME_DYNCOL_PREFIX, USER_ID2_BYTES), PDataType.TIME.toBytes(ENTRY3_USER_ID2_LOGIN_TIME));
            put.add(B_CF, ByteUtil.concat(LAST_LOGIN_TIME_DYNCOL_PREFIX, USER_ID3_BYTES), PDataType.TIME.toBytes(ENTRY3_USER_ID3_LOGIN_TIME));
            mutations.add(put);

            hTable.batch(mutations);

        } finally {
            hTable.close();
        }
    }

    private static Pair<String,Integer> getMaxClickCountValue(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            String colName = rsmd.getColumnName(i);
            if (colName.startsWith(MAX_CLICK_COUNT_PREFIX)) {
                String userId = colName.substring(MAX_CLICK_COUNT_PREFIX.length());
                Integer clickCount = rs.getInt(colName);
                return new Pair<String,Integer>(userId,clickCount);
            }
        }
        return null;
    }
    
    private static Time getLastLoginTimeValue(ResultSet rs, String userId) throws SQLException {
        String colName = LAST_LOGIN_TIME_PREFIX + userId;
        try {
            return rs.getTime(colName);
        } catch (SQLException e) {
            // Ignore COLUMN_NOT_FOUND error b/c it means that this user didn't login
            if (e.getErrorCode() == SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode()) {
                return null;
            }
            throw e;
        }
    }
    
    /**
     * Should project all of column family A columns qualifiers. Should also automatically be case insensitive,
     * since it is a wildcard.
     * @throws Exception
     */
    // FIXME @Test
    public void testGetAllDynColsInFamily() throws Exception {
        String query = "SELECT A.* FROM WEB_STATS WHERE entry='entry1'";
        String url = PHOENIX_JDBC_URL + ";";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            Pair<String,Integer> maxClickCountUserIdAndValue = getMaxClickCountValue(rs);
            // This fails for two reasons: 1) all column qualifiers in column family A
            // are not returned in the result, and 2) the dynamic columns are not available
            // through ResultSetMetaData.
            assertEquals(USER_ID2_BYTES,maxClickCountUserIdAndValue.getFirst());
            assertEquals(ENTRY1_CLICK_COUNT,maxClickCountUserIdAndValue.getSecond());
            
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * Should project all of column family A columns qualifiers. Should also automatically be case insensitive,
     * since it is a wildcard.
     * @throws Exception
     */
    // FIXME @Test
    public void testGetAllDynCols() throws Exception {
        String query = "SELECT * FROM WEB_STATS WHERE entry='entry1'";
        String url = PHOENIX_JDBC_URL + ";";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            Pair<String,Integer> maxClickCountUserIdAndValue = getMaxClickCountValue(rs);
            // This fails because the dynamic columns are not available through ResultSetMetaData
            assertEquals(USER_ID2_BYTES,maxClickCountUserIdAndValue.getFirst());
            assertEquals(ENTRY1_CLICK_COUNT,maxClickCountUserIdAndValue.getSecond());
            
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * Since the dynamic columns are not in double quotes, the column name is normalized by being upper cased.
     * In this case, since USER_ID is case sensitive, it will not find the columns
     */
    @Test
    public void testGetCaseInsensitiveDynCol() throws Exception {
        String query = "SELECT B.* FROM WEB_STATS(" + 
                "B." + LAST_LOGIN_TIME_PREFIX + USER_ID2 + " TIME," + 
                "B." + LAST_LOGIN_TIME_PREFIX + USER_ID3 + " TIME) WHERE entry='entry2'";
        String url = PHOENIX_JDBC_URL + ";";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(null, getLastLoginTimeValue(rs, USER_ID2));
            assertEquals(null, getLastLoginTimeValue(rs, USER_ID3));
            assertEquals(null, getLastLoginTimeValue(rs, USER_ID1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * Since dynamic columns are not in double quotes, the column name is not normalized, but instead
     * is left as is. This should succeed, since the user ID case is matched
     */
    // FIXME @Test
    public void testGetCaseSensitiveDynCol() throws Exception {
        String query = "SELECT B.* FROM WEB_STATS(" + 
                "B.\"" + LAST_LOGIN_TIME_PREFIX + USER_ID2 + "\"" + " TIME," + 
                "B.\"" + LAST_LOGIN_TIME_PREFIX + USER_ID3 + "\"" + " TIME) WHERE entry='entry2'";
        String url = PHOENIX_JDBC_URL + ";";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ENTRY2_USER_ID2_LOGIN_TIME, getLastLoginTimeValue(rs, USER_ID2));
            assertEquals(ENTRY2_USER_ID3_LOGIN_TIME, getLastLoginTimeValue(rs, USER_ID3));
            assertEquals(null, getLastLoginTimeValue(rs, Bytes.toString(USER_ID1_BYTES)));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * We have to make sure that static columns that are projected are in the expected order.
     * Dynamic columns should be projected as well, but we cannot guarantee their order.
     * @throws Exception
     */
    // FIXME @Test
    public void testProjectStaticAndDynamic() throws Exception {
        String query = "SELECT ENTRY, A.DUMMY, B.DUMMY, A.*,B.* FROM WEB_STATS WHERE entry='entry3'";
        String url = PHOENIX_JDBC_URL + ";";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("entry3", rs.getString(1));
            assertEquals(null, rs.getString(2));
            assertEquals(null, rs.getString(3));
            Pair<String,Integer> clickCountUserIdAndValue = getMaxClickCountValue(rs);
            assertEquals(USER_ID1_BYTES,clickCountUserIdAndValue.getFirst());
            assertEquals(ENTRY3_CLICK_COUNT,clickCountUserIdAndValue.getSecond());
            
            assertEquals(ENTRY3_USER_ID1_LOGIN_TIME, getLastLoginTimeValue(rs, Bytes.toString(USER_ID1_BYTES)));
            assertEquals(ENTRY3_USER_ID2_LOGIN_TIME, getLastLoginTimeValue(rs, Bytes.toString(USER_ID2_BYTES)));
            assertEquals(ENTRY3_USER_ID3_LOGIN_TIME, getLastLoginTimeValue(rs, Bytes.toString(USER_ID3_BYTES)));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test(expected = ColumnFamilyNotFoundException.class)
    public void testDynamicFamilyException() throws Exception {
        String query = "SELECT C.* FROM WEB_STATS";
        String url = PHOENIX_JDBC_URL + ";";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
        } finally {
            conn.close();
        }
    }

    @Test(expected = PhoenixParserException.class)
    public void testDynamicFamilyFunctionException() throws Exception {
        String query = "SELECT count(C.*) FROM WEB_STATS";
        String url = PHOENIX_JDBC_URL + ";";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectEntireColumnFamily() throws Exception {
        ResultSet rs;
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(true);
        conn.createStatement().execute("CREATE TABLE TESTTABLE (Id VARCHAR NOT NULL PRIMARY KEY, COLFAM1.A VARCHAR, COLFAM1.B VARCHAR, COLFAM2.A VARCHAR )");
        conn.createStatement().execute("UPSERT INTO TESTTABLE (Id, COLFAM1.A, COLFAM1.B, COLFAM2.A) values ('row-2', '100', '200', '300')");
        rs = conn.createStatement().executeQuery("SELECT COLFAM1.A,COLFAM1.B FROM TESTTABLE");
        assertTrue(rs.next());
        assertEquals("100",rs.getString(1));
        assertEquals("200",rs.getString(2));
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("SELECT COLFAM1.* FROM TESTTABLE");
        assertTrue(rs.next());
        assertEquals("100",rs.getString(1));
        assertEquals("200",rs.getString(2));
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("SELECT COLFAM1.*,COLFAM1.A FROM TESTTABLE");
        assertTrue(rs.next());
        assertEquals("100",rs.getString(1));
        assertEquals("200",rs.getString(2));
        assertFalse(rs.next());
    }
}
