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
package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.*;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.*;
import java.text.Format;
import java.text.ParseException;
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.schema.ConstraintViolationException;
import com.salesforce.phoenix.util.DateUtil;
import com.salesforce.phoenix.util.PhoenixRuntime;


public class VariableLengthPKTest extends BaseClientMangedTimeTest {
    private static Format format = DateUtil.getDateParser(DateUtil.DEFAULT_DATE_FORMAT);
    private static final String DS1 = "1970-01-01 00:58:00";
    private static final Date D1 = toDate(DS1);

    private static Date toDate(String dateString) {
        try {
            return (Date)format.parseObject(dateString);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    protected static void initGroupByRowKeyColumns(long ts) throws Exception {
        ensureTableCreated(getUrl(),PTSDB_NAME, null, ts-2);

        // Insert all rows at ts
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " +
                "PTSDB(" +
                "    INST, " +
                "    HOST," +
                "    DATE)" +
                "VALUES (?, ?, CURRENT_DATE())");
        stmt.setString(1, "ab");
        stmt.setString(2, "a");
        stmt.execute();
        stmt.setString(1, "ac");
        stmt.setString(2, "b");
        stmt.execute();
        stmt.setString(1, "ad");
        stmt.setString(2, "a");
        stmt.execute();
        conn.commit();
        conn.close();
    }

    protected static void initTableValues(byte[][] splits, long ts) throws Exception {
        ensureTableCreated(getUrl(),PTSDB_NAME, splits, ts-2);

        // Insert all rows at ts
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " +
                "PTSDB(" +
                "    INST, " +
                "    HOST," +
                "    DATE," +
                "    VAL)" +
                "VALUES (?, ?, ?, ?)");
        stmt.setString(1, "abc");
        stmt.setString(2, "abc-def-ghi");
        stmt.setDate(3, new Date(System.currentTimeMillis()));
        stmt.setBigDecimal(4, new BigDecimal(.5));
        stmt.execute();

        ensureTableCreated(getUrl(),BTABLE_NAME, splits, ts-2);
        conn.setAutoCommit(false);

        // Insert all rows at ts
        stmt = conn.prepareStatement(
                "upsert into " +
                "BTABLE(" +
                "    A_STRING, " +
                "    A_ID," +
                "    B_STRING," +
                "    A_INTEGER," +
                "    B_INTEGER," +
                "    C_INTEGER," +
                "    D_STRING," +
                "    E_STRING)" +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
        stmt.setString(1, "abc");
        stmt.setString(2, "111");
        stmt.setString(3, "x");
        stmt.setInt(4, 1);
        stmt.setInt(5, 10);
        stmt.setInt(6, 1000);
        stmt.setString(7, null);
        stmt.setString(8, "0123456789");
        stmt.execute();

        stmt.setString(1, "abcd");
        stmt.setString(2, "222");
        stmt.setString(3, "xy");
        stmt.setInt(4, 2);
        stmt.setNull(5, Types.INTEGER);
        stmt.setNull(6, Types.INTEGER);
        stmt.execute();

        stmt.setString(3, "xyz");
        stmt.setInt(4, 3);
        stmt.setInt(5, 10);
        stmt.setInt(6, 1000);
        stmt.setString(7, "efg");
        stmt.execute();

        stmt.setString(3, "xyzz");
        stmt.setInt(4, 4);
        stmt.setInt(5, 40);
        stmt.setNull(6, Types.INTEGER);
        stmt.setString(7, null);
        stmt.execute();

        String ddl = "create table VarcharKeyTest" +
            "   (pk varchar not null primary key)";
        createTestTable(getUrl(), ddl, splits, ts-2);
        stmt = conn.prepareStatement(
                "upsert into " +
                "VarcharKeyTest(pk) " +
                "VALUES (?)");
        stmt.setString(1, "   def");
        stmt.execute();
        stmt.setString(1, "jkl   ");
        stmt.execute();
        stmt.setString(1, "   ghi   ");
        stmt.execute();

        conn.commit();
        conn.close();
    }

    @Test
    public void testSingleColumnScanKey() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT A_STRING,substr(a_id,1,1),B_STRING,A_INTEGER,B_INTEGER FROM BTABLE WHERE A_STRING=?";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, "abc");
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals("1", rs.getString(2));
            assertEquals("x", rs.getString(3));
            assertEquals(1, rs.getInt(4));
            assertEquals(10, rs.getInt(5));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSingleColumnGroupBy() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT INST FROM PTSDB GROUP BY INST";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testNonfirstColumnGroupBy() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT HOST FROM PTSDB WHERE INST='abc' GROUP BY HOST";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("abc-def-ghi", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testGroupByRowKeyColumns() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT SUBSTR(INST,1,1),HOST FROM PTSDB GROUP BY SUBSTR(INST,1,1),HOST";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initGroupByRowKeyColumns(ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("a", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("b", rs.getString(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSkipScan() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT HOST FROM PTSDB WHERE INST='abc' AND DATE>=TO_DATE('1970-01-01 00:00:00') AND DATE <TO_DATE('2015-01-01 00:00:00')";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("abc-def-ghi", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSkipMax() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT MAX(INST),MAX(DATE) FROM PTSDB WHERE INST='abc' AND DATE>=TO_DATE('1970-01-01 00:00:00') AND DATE <TO_DATE('2171-01-01 00:00:00')";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSkipMaxWithLimit() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT MAX(INST),MAX(DATE) FROM PTSDB WHERE INST='abc' AND DATE>=TO_DATE('1970-01-01 00:00:00') AND DATE <TO_DATE('2171-01-01 00:00:00') LIMIT 2";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSingleColumnKeyFilter() throws Exception {
        long ts = nextTimestamp();
        // Requires not null column to be projected, since the only one projected in the query is
        // nullable and will cause the no key value to be returned if it is the only one projected.
        String query = "SELECT A_STRING,substr(a_id,1,1),B_STRING,A_INTEGER,B_INTEGER FROM BTABLE WHERE B_STRING=?";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, "xy");
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("abcd", rs.getString(1));
            assertEquals("2", rs.getString(2));
            assertEquals("xy", rs.getString(3));
            assertEquals(2, rs.getInt(4));
            assertEquals(0, rs.getInt(5));
            assertTrue(rs.wasNull());
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testMultiColumnEqScanKey() throws Exception {
        long ts = nextTimestamp();
        // TODO: add compile test to confirm start/stop scan key
        String query = "SELECT A_STRING,substr(a_id,1,1),B_STRING,A_INTEGER,B_INTEGER FROM BTABLE WHERE A_STRING=? AND A_ID=? AND B_STRING=?";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, "abcd");
            statement.setString(2, "222");
            statement.setString(3, "xy");
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("abcd", rs.getString(1));
            assertEquals("2", rs.getString(2));
            assertEquals("xy", rs.getString(3));
            assertEquals(2, rs.getInt(4));
            assertEquals(0, rs.getInt(5));
            assertTrue(rs.wasNull());
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testMultiColumnGTScanKey() throws Exception {
        long ts = nextTimestamp();
        // TODO: add compile test to confirm start/stop scan key
        String query = "SELECT A_STRING,substr(a_id,1,1),B_STRING,A_INTEGER,B_INTEGER FROM BTABLE WHERE A_STRING=? AND A_ID=? AND B_STRING>?";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, "abcd");
            statement.setString(2, "222");
            statement.setString(3, "xy");
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("abcd", rs.getString(1));
            assertEquals("2", rs.getString(2));
            assertEquals("xyz", rs.getString(3));
            assertEquals(3, rs.getInt(4));
            assertEquals(10, rs.getInt(5));
            assertTrue(rs.next());
            assertEquals("abcd", rs.getString(1));
            assertEquals("2", rs.getString(2));
            assertEquals("xyzz", rs.getString(3));
            assertEquals(4, rs.getInt(4));
            assertEquals(40, rs.getInt(5));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testMultiColumnGTKeyFilter() throws Exception {
        long ts = nextTimestamp();
        // TODO: add compile test to confirm start/stop scan key
        String query = "SELECT A_STRING,substr(a_id,1,1),B_STRING,A_INTEGER,B_INTEGER FROM BTABLE WHERE A_STRING>? AND A_INTEGER>=?";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, "abc");
            statement.setInt(2, 4);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("abcd", rs.getString(1));
            assertEquals("2", rs.getString(2));
            assertEquals("xyzz", rs.getString(3));
            assertEquals(4, rs.getInt(4));
            assertEquals(40, rs.getInt(5));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testNullValueEqualityScan() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),PTSDB_NAME,null, ts-2);

        // Insert all rows at ts
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        PreparedStatement stmt = conn.prepareStatement("upsert into PTSDB VALUES ('', '', ?, 0.5)");
        stmt.setDate(1, D1);
        stmt.execute();
        conn.close();

        // Comparisons against null are always false.
        String query = "SELECT HOST,DATE FROM PTSDB WHERE HOST='' AND INST=''";
        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testVarLengthPKColScan() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),PTSDB_NAME,null, ts-2);

        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts; // Insert at timestamp 0
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        PreparedStatement stmt = conn.prepareStatement("upsert into PTSDB VALUES (?, 'y', ?, 0.5)");
        stmt.setString(1, "x");
        stmt.setDate(2, D1);
        stmt.execute();
        stmt.setString(1, "xy");
        stmt.execute();
        conn.close();

        String query = "SELECT HOST,DATE FROM PTSDB WHERE INST='x' AND HOST='y'";
        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(D1, rs.getDate(2));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testEscapedQuoteScan() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),PTSDB_NAME,null, ts-2);

        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts; // Insert at timestamp 0
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        PreparedStatement stmt = conn.prepareStatement("upsert into PTSDB VALUES (?, 'y', ?, 0.5)");
        stmt.setString(1, "x'y");
        stmt.setDate(2, D1);
        stmt.execute();
        stmt.setString(1, "x");
        stmt.execute();
        conn.close();

        String query1 = "SELECT INST,DATE FROM PTSDB WHERE INST='x''y'";
        String query2 = "SELECT INST,DATE FROM PTSDB WHERE INST='x\\\'y'";
        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query1);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("x'y", rs.getString(1));
            assertEquals(D1, rs.getDate(2));
            assertFalse(rs.next());

            statement = conn.prepareStatement(query2);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("x'y", rs.getString(1));
            assertEquals(D1, rs.getDate(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    private static void initPtsdbTableValues(long ts) throws Exception {
        ensureTableCreated(getUrl(),PTSDB_NAME,null, ts-2);

        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts; // Insert at timestamp 0
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        PreparedStatement stmt = conn.prepareStatement("upsert into PTSDB VALUES ('x', 'y', ?, 0.5)");
        stmt.setDate(1, D1);
        stmt.execute();
        conn.close();
    }

    @Test
    public void testToStringOnDate() throws Exception {
        long ts = nextTimestamp();
        initPtsdbTableValues(ts);

        String query = "SELECT HOST,DATE FROM PTSDB WHERE INST='x' AND HOST='y'";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(DateUtil.DEFAULT_DATE_FORMATTER.format(D1), rs.getString(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    private static void initPtsdbTableValues2(long ts, Date d) throws Exception {
        ensureTableCreated(getUrl(),PTSDB2_NAME,null, ts-2);

        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts; // Insert at timestamp 0
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        PreparedStatement stmt = conn.prepareStatement("upsert into "+PTSDB2_NAME+"(inst,date,val2) VALUES (?, ?, ?)");
        stmt.setString(1, "a");
        stmt.setDate(2, d);
        stmt.setDouble(3, 101.3);
        stmt.execute();
        stmt.setString(1, "a");
        stmt.setDate(2, new Date(d.getTime() + 1 * MILLIS_IN_DAY));
        stmt.setDouble(3, 99.7);
        stmt.execute();
        stmt.setString(1, "a");
        stmt.setDate(2, new Date(d.getTime() - 1 * MILLIS_IN_DAY));
        stmt.setDouble(3, 105.3);
        stmt.execute();
        stmt.setString(1, "b");
        stmt.setDate(2, d);
        stmt.setDouble(3, 88.5);
        stmt.execute();
        stmt.setString(1, "b");
        stmt.setDate(2, new Date(d.getTime() + 1 * MILLIS_IN_DAY));
        stmt.setDouble(3, 89.7);
        stmt.execute();
        stmt.setString(1, "b");
        stmt.setDate(2, new Date(d.getTime() - 1 * MILLIS_IN_DAY));
        stmt.setDouble(3, 94.9);
        stmt.execute();
        conn.close();
    }

    @Test
    public void testRoundOnDate() throws Exception {
        long ts = nextTimestamp();
        Date date = new Date(System.currentTimeMillis());
        initPtsdbTableValues2(ts, date);

        String query = "SELECT MAX(val2)"
        + " FROM "+PTSDB2_NAME
        + " WHERE inst='a'"
        + " GROUP BY ROUND(date,'day',1)"
        + " ORDER BY MAX(val2)"; // disambiguate row order
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(99.7, rs.getDouble(1), 1e-6);
            assertTrue(rs.next());
            assertEquals(101.3, rs.getDouble(1), 1e-6);
            assertTrue(rs.next());
            assertEquals(105.3, rs.getDouble(1), 1e-6);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testOrderBy() throws Exception {
        long ts = nextTimestamp();
        Date date = new Date(System.currentTimeMillis());
        initPtsdbTableValues2(ts, date);

        String query = "SELECT inst,MAX(val2),MIN(val2)"
        + " FROM "+PTSDB2_NAME
        + " GROUP BY inst,ROUND(date,'day',1)"
        + " ORDER BY inst,ROUND(date,'day',1)"
        ;
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals(105.3, rs.getDouble(2), 1e-6);
            assertEquals(105.3, rs.getDouble(3), 1e-6);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals(101.3, rs.getDouble(2), 1e-6);
            assertEquals(101.3, rs.getDouble(3), 1e-6);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals(99.7, rs.getDouble(2), 1e-6);
            assertEquals(99.7, rs.getDouble(3), 1e-6);
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertEquals(94.9, rs.getDouble(2), 1e-6);
            assertEquals(94.9, rs.getDouble(3), 1e-6);
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertEquals(88.5, rs.getDouble(2), 1e-6);
            assertEquals(88.5, rs.getDouble(3), 1e-6);
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertEquals(89.7, rs.getDouble(2), 1e-6);
            assertEquals(89.7, rs.getDouble(3), 1e-6);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectCount() throws Exception {
        long ts = nextTimestamp();
        Date date = new Date(System.currentTimeMillis());
        initPtsdbTableValues2(ts, date);

        String query = "SELECT COUNT(*)"
        + " FROM "+PTSDB2_NAME
        + " WHERE inst='a'";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testBatchUpsert() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),PTSDB2_NAME,null, ts-2);
        Date d = new Date(ts);
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        String query = "SELECT SUM(val1),SUM(val2),SUM(val3) FROM "+PTSDB2_NAME;
        String sql1 = "UPSERT INTO "+PTSDB2_NAME+"(inst,date,val1) VALUES (?, ?, ?)";
        String sql2 = "UPSERT INTO "+PTSDB2_NAME+"(inst,date,val2) VALUES (?, ?, ?)";
        String sql3 = "UPSERT INTO "+PTSDB2_NAME+"(inst,date,val3) VALUES (?, ?, ?)";
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.setAutoCommit(false);
        // conn.setAutoCommit(true);

        {
            // verify precondition: SUM(val{1,2,3}) are null
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertNull(rs.getBigDecimal(1));
            assertNull(rs.getBigDecimal(2));
            assertNull(rs.getBigDecimal(3));
            assertFalse(rs.next());
            statement.close();
        }

        {
            PreparedStatement s = conn.prepareStatement(sql1);
            s.setString(1, "a");
            s.setDate(2, d);
            s.setInt(3, 1);
            assertEquals(1, s.executeUpdate());
            s.close();
        }
        {
            PreparedStatement s = conn.prepareStatement(sql2);
            s.setString(1, "b");
            s.setDate(2, d);
            s.setInt(3, 1);
            assertEquals(1, s.executeUpdate());
            s.close();
        }
        {
            PreparedStatement s = conn.prepareStatement(sql3);
            s.setString(1, "c");
            s.setDate(2, d);
            s.setInt(3, 1);
            assertEquals(1, s.executeUpdate());
            s.close();
        }
        {
            PreparedStatement s = conn.prepareStatement(sql1);
            s.setString(1, "a");
            s.setDate(2, d);
            s.setInt(3, 5);
            assertEquals(1, s.executeUpdate());
            s.close();
        }
        {
            PreparedStatement s = conn.prepareStatement(sql1);
            s.setString(1, "b");
            s.setDate(2, d);
            s.setInt(3, 5);
            assertEquals(1, s.executeUpdate());
            s.close();
        }
        {
            PreparedStatement s = conn.prepareStatement(sql1);
            s.setString(1, "c");
            s.setDate(2, d);
            s.setInt(3, 5);
            assertEquals(1, s.executeUpdate());
            s.close();
        }
        conn.commit();
        conn.close();
        
        // Query at a time after the upsert to confirm they took place
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+1));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(15, rs.getDouble(1), 1e-6);
            assertEquals(1, rs.getDouble(2), 1e-6);
            assertEquals(1, rs.getDouble(3), 1e-6);
            assertFalse(rs.next());
            statement.close();
        }
    }

    @Test
    public void testSelectStar() throws Exception {
        long ts = nextTimestamp();
        initPtsdbTableValues(ts);

        String query = "SELECT * FROM PTSDB WHERE INST='x' AND HOST='y'";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("x",rs.getString("inst"));
            assertEquals("y",rs.getString("host"));
            assertEquals(D1, rs.getDate("date"));
            assertEquals(BigDecimal.valueOf(0.5), rs.getBigDecimal("val"));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testToCharOnDate() throws Exception {
        long ts = nextTimestamp();
        initPtsdbTableValues(ts);

        String query = "SELECT HOST,TO_CHAR(DATE) FROM PTSDB WHERE INST='x' AND HOST='y'";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(DateUtil.DEFAULT_DATE_FORMATTER.format(D1), rs.getString(2));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testToCharWithFormatOnDate() throws Exception {
        long ts = nextTimestamp();
        initPtsdbTableValues(ts);

        String format = "HH:mm:ss";
        Format dateFormatter = DateUtil.getDateFormatter(format);
        String query = "SELECT HOST,TO_CHAR(DATE,'" + format + "') FROM PTSDB WHERE INST='x' AND HOST='y'";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(dateFormatter.format(D1), rs.getString(2));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testToDateWithFormatOnDate() throws Exception {
        long ts = nextTimestamp();
        initPtsdbTableValues(ts);

        String format = "yyyy-MM-dd HH:mm:ss.S";
        Format dateFormatter = DateUtil.getDateFormatter(format);
        String query = "SELECT HOST,TO_CHAR(DATE,'" + format + "') FROM PTSDB WHERE INST='x' AND HOST='y' and DATE=TO_DATE(?,'" + format + "')";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, dateFormatter.format(D1));
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(dateFormatter.format(D1), rs.getString(2));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testMissingPKColumn() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),PTSDB_NAME,null, ts-2);

        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts; // Insert at timestamp 0
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        Statement stmt = conn.createStatement();
        try {
            stmt.execute("upsert into PTSDB(INST,HOST,VAL) VALUES ('abc', 'abc-def-ghi', 0.5)");
            fail();
        } catch (ConstraintViolationException e) {
            assertTrue(e.getMessage().contains("may not be null"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testNoKVColumn() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),BTABLE_NAME,null, ts-2);

        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts; // Insert at timestamp 0
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into BTABLE VALUES (?, ?, ?, ?, ?)");
        stmt.setString(1, "abc");
        stmt.setString(2, "123");
        stmt.setString(3, "x");
        stmt.setInt(4, 1);
        stmt.setString(5, "ab");
        // Succeeds since we have an empty KV
        stmt.execute();
    }

    // Broken, since we don't know if insert vs update. @Test
    public void testMissingKVColumn() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),BTABLE_NAME,null, ts-2);

        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts; // Insert at timestamp 0
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into BTABLE VALUES (?, ?, ?, ?, ?, ?)");
        stmt.setString(1, "abc");
        stmt.setString(2, "123");
        stmt.setString(3, "x");
        stmt.setInt(4, 1);
        stmt.setString(5, "ab");
        stmt.setInt(6, 1);
        try {
            stmt.execute();
            fail();
        } catch (ConstraintViolationException e) {
            // Non nullable key value E_STRING has no value
            assertTrue(e.getMessage().contains("may not be null"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTooShortKVColumn() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),BTABLE_NAME,null, ts-2);

        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts; // Insert at timestamp 0
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        // Insert all rows at ts
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " +
                "BTABLE(" +
                "    A_STRING, " +
                "    A_ID," +
                "    B_STRING," +
                "    A_INTEGER," +
                "    C_STRING," +
                "    E_STRING)" +
                "VALUES (?, ?, ?, ?, ?, ?)");
        stmt.setString(1, "abc");
        stmt.setString(2, "123");
        stmt.setString(3, "x");
        stmt.setInt(4, 1);
        stmt.setString(5, "ab");
        stmt.setString(6, "01234");

        try {
            stmt.execute();
        } catch (ConstraintViolationException e) {
            fail("Constraint voilation Exception should not be thrown, the characters have to be padded");
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTooShortPKColumn() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),BTABLE_NAME,null, ts-2);

        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts; // Insert at timestamp 0
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        // Insert all rows at ts
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " +
                "BTABLE(" +
                "    A_STRING, " +
                "    A_ID," +
                "    B_STRING," +
                "    A_INTEGER," +
                "    C_STRING," +
                "    E_STRING)" +
                "VALUES (?, ?, ?, ?, ?, ?)");
        stmt.setString(1, "abc");
        stmt.setString(2, "12");
        stmt.setString(3, "x");
        stmt.setInt(4, 1);
        stmt.setString(5, "ab");
        stmt.setString(6, "0123456789");

        try {
            stmt.execute();
        } catch (ConstraintViolationException e) {
            fail("Constraint voilation Exception should not be thrown, the characters have to be padded");
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTooLongPKColumn() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),BTABLE_NAME,null, ts-2);

        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts; // Insert at timestamp 0
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        // Insert all rows at ts
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " +
                "BTABLE(" +
                "    A_STRING, " +
                "    A_ID," +
                "    B_STRING," +
                "    A_INTEGER," +
                "    C_STRING," +
                "    E_STRING)" +
                "VALUES (?, ?, ?, ?, ?, ?)");
        stmt.setString(1, "abc");
        stmt.setString(2, "123");
        stmt.setString(3, "x");
        stmt.setInt(4, 1);
        stmt.setString(5, "abc");
        stmt.setString(6, "0123456789");

        try {
            stmt.execute();
            fail();
        } catch (ConstraintViolationException e) {
            assertTrue(e.getMessage().contains(" may not exceed 2 bytes"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTooLongKVColumn() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),BTABLE_NAME,null, ts-2);

        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts; // Insert at timestamp 0
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        // Insert all rows at ts
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " +
                "BTABLE(" +
                "    A_STRING, " +
                "    A_ID," +
                "    B_STRING," +
                "    A_INTEGER," +
                "    C_STRING," +
                "    D_STRING," +
                "    E_STRING)" +
                "VALUES (?, ?, ?, ?, ?, ?, ?)");
        stmt.setString(1, "abc");
        stmt.setString(2, "123");
        stmt.setString(3, "x");
        stmt.setInt(4, 1);
        stmt.setString(5, "ab");
        stmt.setString(6,"abcd");
        stmt.setString(7, "0123456789");

        try {
            stmt.execute();
            fail();
        } catch (ConstraintViolationException e) {
            assertTrue(e.getMessage().contains(" may not exceed 3 bytes"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testMultiFixedLengthNull() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT B_INTEGER,C_INTEGER,COUNT(1) FROM BTABLE GROUP BY C_INTEGER,B_INTEGER";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            assertTrue(rs.wasNull());
            assertEquals(0, rs.getInt(2));
            assertTrue(rs.wasNull());
            assertEquals(1, rs.getLong(3));

            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            assertEquals(1000, rs.getInt(2));
            assertEquals(2, rs.getLong(3));

            assertTrue(rs.next());
            assertEquals(40, rs.getInt(1));
            assertEquals(0, rs.getInt(2));
            assertTrue(rs.wasNull());
            assertEquals(1, rs.getLong(3));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSingleFixedLengthNull() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT C_INTEGER,COUNT(1) FROM BTABLE GROUP BY C_INTEGER";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            assertTrue(rs.wasNull());
            assertEquals(2, rs.getLong(2));

            assertTrue(rs.next());
            assertEquals(1000, rs.getInt(1));
            assertEquals(2, rs.getLong(2));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testMultiMixedTypeGroupBy() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT A_ID, E_STRING, D_STRING, C_INTEGER, COUNT(1) FROM BTABLE GROUP BY A_ID, E_STRING, D_STRING, C_INTEGER";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("111", rs.getString(1));
            assertEquals("0123456789", rs.getString(2));
            assertEquals(null, rs.getString(3));
            assertEquals(1000, rs.getInt(4));
            assertEquals(1, rs.getInt(5));

            assertTrue(rs.next());
            assertEquals("222", rs.getString(1));
            assertEquals("0123456789", rs.getString(2));
            assertEquals(null, rs.getString(3));
            assertEquals(0, rs.getInt(4));
            assertTrue(rs.wasNull());
            assertEquals(2, rs.getInt(5));

            assertTrue(rs.next());
            assertEquals("222", rs.getString(1));
            assertEquals("0123456789", rs.getString(2));
            assertEquals("efg", rs.getString(3));
            assertEquals(1000, rs.getInt(4));
            assertEquals(1, rs.getInt(5));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSubstrFunction() throws Exception {
        long ts = nextTimestamp();
        String query[] = {
            "SELECT substr('ABC',-1,1) FROM BTABLE LIMIT 1",
            "SELECT substr('ABC',-4,1) FROM BTABLE LIMIT 1",
            "SELECT substr('ABC',2,4) FROM BTABLE LIMIT 1",
            "SELECT substr('ABC',1,1) FROM BTABLE LIMIT 1",
            "SELECT substr('ABC',0,1) FROM BTABLE LIMIT 1",
            // Test for multibyte characters support.
            "SELECT substr('ĎďĒ',0,1) FROM BTABLE LIMIT 1",
            "SELECT substr('ĎďĒ',0,2) FROM BTABLE LIMIT 1",
            "SELECT substr('ĎďĒ',1,1) FROM BTABLE LIMIT 1",
            "SELECT substr('ĎďĒ',1,2) FROM BTABLE LIMIT 1",
            "SELECT substr('ĎďĒ',2,1) FROM BTABLE LIMIT 1",
            "SELECT substr('ĎďĒ',2,2) FROM BTABLE LIMIT 1",
            "SELECT substr('ĎďĒ',-1,1) FROM BTABLE LIMIT 1",
            "SELECT substr('Ďďɚʍ',2,4) FROM BTABLE LIMIT 1",
            "SELECT pk FROM VarcharKeyTest WHERE substr(pk, 0, 3)='jkl'",
        };
        String result[] = {
            "C",
            null,
            "BC",
            "A",
            "A",
            "Ď",
            "Ďď",
            "Ď",
            "Ďď",
            "ď",
            "ďĒ",
            "Ē",
            "ďɚʍ",
            "jkl   ",
        };
        assertEquals(query.length,result.length);
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            for (int i = 0; i < query.length; i++) {
                PreparedStatement statement = conn.prepareStatement(query[i]);
                ResultSet rs = statement.executeQuery();
                assertTrue(rs.next());
                assertEquals(result[i], rs.getString(1));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRegexReplaceFunction() throws Exception {
        long ts = nextTimestamp();
        // NOTE: we need to double escape the "\\" here because conn.prepareStatement would
        // also try to evaluate the escaping. As a result, to represent what normally would be
        // a "\d" in this test, it would become "\\\\d".
        String query[] = {
            "SELECT regexp_replace('', '') FROM BTABLE LIMIT 1",
            "SELECT regexp_replace('', 'abc', 'def') FROM BTABLE LIMIT 1",
            "SELECT regexp_replace('123abcABC', '[a-z]+') FROM BTABLE LIMIT 1",
            "SELECT regexp_replace('123-abc-ABC', '-[a-zA-Z-]+') FROM BTABLE LIMIT 1",
            "SELECT regexp_replace('abcABC123', '\\\\d+', '') FROM BTABLE LIMIT 1",
            "SELECT regexp_replace('abcABC123', '\\\\D+', '') FROM BTABLE LIMIT 1",
            "SELECT regexp_replace('abc', 'abc', 'def') FROM BTABLE LIMIT 1",
            "SELECT regexp_replace('abc123ABC', '\\\\d+', 'def') FROM BTABLE LIMIT 1",
            "SELECT regexp_replace('abc123ABC', '[0-9]+', '#') FROM BTABLE LIMIT 1",
            "SELECT CASE WHEN regexp_replace('abcABC123', '[a-zA-Z]+') = '123' THEN '1' ELSE '2' END FROM BTABLE LIMIT 1",
            "SELECT A_STRING FROM BTABLE WHERE A_ID = regexp_replace('abcABC111', '[a-zA-Z]+') LIMIT 1", // 111
            // Test for multibyte characters support.
            "SELECT regexp_replace('Ďď Ēĕ ĜĞ ϗϘϛϢ', '[a-zA-Z]+') from BTABLE LIMIT 1",
            "SELECT regexp_replace('Ďď Ēĕ ĜĞ ϗϘϛϢ', '[Ď-ě]+', '#') from BTABLE LIMIT 1",
            "SELECT regexp_replace('Ďď Ēĕ ĜĞ ϗϘϛϢ', '.+', 'replacement') from BTABLE LIMIT 1",
            "SELECT regexp_replace('Ďď Ēĕ ĜĞ ϗϘϛϢ', 'Ďď', 'DD') from BTABLE LIMIT 1",
        };
        String result[] = {
            null,
            null,
            "123ABC",
            "123",
            "abcABC",
            "123",
            "def",
            "abcdefABC",
            "abc#ABC",
            "1",
            "abc", // the first column
            "Ďď Ēĕ ĜĞ ϗϘϛϢ",
            "# # ĜĞ ϗϘϛϢ",
            "replacement",
            "DD Ēĕ ĜĞ ϗϘϛϢ",
        };
        assertEquals(query.length,result.length);
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            for (int i = 0; i < query.length; i++) {
                PreparedStatement statement = conn.prepareStatement(query[i]);
                ResultSet rs = statement.executeQuery();
                assertTrue(rs.next());
                assertEquals(result[i], rs.getString(1));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRegexpSubstrFunction() throws Exception {
        long ts = nextTimestamp();
        String query[] = {
            "SELECT regexp_substr('', '', 0) FROM BTABLE LIMIT 1",
            "SELECT regexp_substr('', '', 1) FROM BTABLE LIMIT 1",
            "SELECT regexp_substr('', 'abc', 0) FROM BTABLE LIMIT 1",
            "SELECT regexp_substr('abc', '', 0) FROM BTABLE LIMIT 1",
            "SELECT regexp_substr('123', '123', 3) FROM BTABLE LIMIT 1",
            "SELECT regexp_substr('123', '123', -4) FROM BTABLE LIMIT 1",
            "SELECT regexp_substr('123ABC', '[a-z]+', 0) FROM BTABLE LIMIT 1",
            "SELECT regexp_substr('123ABC', '[0-9]+', 4) FROM BTABLE LIMIT 1",
            "SELECT regexp_substr('123ABCabc', '\\\\d+', 0) FROM BTABLE LIMIT 1",
            "SELECT regexp_substr('123ABCabc', '\\\\D+', 0) FROM BTABLE LIMIT 1",
            "SELECT regexp_substr('123ABCabc', '\\\\D+', 4) FROM BTABLE LIMIT 1",
            "SELECT regexp_substr('123ABCabc', '\\\\D+', 7) FROM BTABLE LIMIT 1",
            "SELECT regexp_substr('na11-app5-26-sjl', '[^-]+', 0) FROM BTABLE LIMIT 1",
            "SELECT regexp_substr('na11-app5-26-sjl', '[^-]+') FROM BTABLE LIMIT 1",
            // Test for multibyte characters support.
            "SELECT regexp_substr('ĎďĒĕĜĞ', '.+') from BTABLE LIMIT 1",
            "SELECT regexp_substr('ĎďĒĕĜĞ', '.+', 3) from BTABLE LIMIT 1",
            "SELECT regexp_substr('ĎďĒĕĜĞ', '[a-zA-Z]+', 0) from BTABLE LIMIT 1",
            "SELECT regexp_substr('ĎďĒĕĜĞ', '[Ď-ě]+', 3) from BTABLE LIMIT 1",
        };
        String result[] = {
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            "123",
            "ABCabc",
            "ABCabc",
            "abc",
            "na11",
            "na11",
            "ĎďĒĕĜĞ",
            "ĒĕĜĞ",
            null,
            "Ēĕ",
        };
        assertEquals(query.length,result.length);
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            for (int i = 0; i < query.length; i++) {
                PreparedStatement statement = conn.prepareStatement(query[i]);
                ResultSet rs = statement.executeQuery();
                assertTrue(rs.next());
                assertEquals(result[i], rs.getString(1));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testLikeConstant() throws Exception {
        long ts = nextTimestamp();
        String query[] = {
            "SELECT CASE WHEN 'ABC' LIKE '' THEN '1' ELSE '2' END FROM BTABLE LIMIT 1",
            "SELECT CASE WHEN 'ABC' LIKE 'A_' THEN '1' ELSE '2' END FROM BTABLE LIMIT 1",
            "SELECT CASE WHEN 'ABC' LIKE 'A__' THEN '1' ELSE '2' END FROM BTABLE LIMIT 1",
            "SELECT CASE WHEN 'AB_C' LIKE 'AB\\_C' THEN '1' ELSE '2' END FROM BTABLE LIMIT 1",
            "SELECT CASE WHEN 'ABC%DE' LIKE 'ABC\\%D%' THEN '1' ELSE '2' END FROM BTABLE LIMIT 1",
        };
        String result[] = {
            "2",
            "2",
            "1",
            "1",
            "1",
        };
        assertEquals(query.length,result.length);
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            for (int i = 0; i < query.length; i++) {
                PreparedStatement statement = conn.prepareStatement(query[i]);
                ResultSet rs = statement.executeQuery();
                assertTrue(rs.next());
                assertEquals(query[i],result[i], rs.getString(1));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testInListConstant() throws Exception {
        long ts = nextTimestamp();
        String query[] = {
            "SELECT CASE WHEN 'a' IN (null,'a') THEN '1' ELSE '2' END FROM BTABLE LIMIT 1",
            "SELECT CASE WHEN NOT 'a' IN (null,'b') THEN '1' ELSE '2' END FROM BTABLE LIMIT 1",
            "SELECT CASE WHEN 'a' IN (null,'b') THEN '1' ELSE '2' END FROM BTABLE LIMIT 1",
            "SELECT CASE WHEN NOT 'a' IN ('c','b') THEN '1' ELSE '2' END FROM BTABLE LIMIT 1",
            "SELECT CASE WHEN 1 IN ('foo',2,1) THEN '1' ELSE '2' END FROM BTABLE LIMIT 1",
        };
        String result[] = {
            "1",
            "2",
            "2",
            "1",
            "1",
        };
        assertEquals(query.length,result.length);
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            for (int i = 0; i < query.length; i++) {
                PreparedStatement statement = conn.prepareStatement(query[i]);
                ResultSet rs = statement.executeQuery();
                assertTrue(rs.next());
                assertEquals(query[i],result[i], rs.getString(1));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testLikeOnColumn() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),PTSDB_NAME,null, ts-2);

        // Insert all rows at ts
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement stmt = conn.prepareStatement("upsert into PTSDB VALUES (?, ?, ?, 0.5)");
        stmt.setDate(3, D1);

        stmt.setString(1, "a");
        stmt.setString(2, "a");
        stmt.execute();

        stmt.setString(1, "x");
        stmt.setString(2, "a");
        stmt.execute();

        stmt.setString(1, "xy");
        stmt.setString(2, "b");
        stmt.execute();

        stmt.setString(1, "xyz");
        stmt.setString(2, "c");
        stmt.execute();

        stmt.setString(1, "xyza");
        stmt.setString(2, "d");
        stmt.execute();

        stmt.setString(1, "xyzab");
        stmt.setString(2, "e");
        stmt.execute();

        stmt.setString(1, "z");
        stmt.setString(2, "e");
        stmt.execute();

        conn.commit();
        conn.close();

        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        conn = DriverManager.getConnection(url, props);
        PreparedStatement statement;
        ResultSet rs;
        try {
            // Test 1
            statement = conn.prepareStatement("SELECT INST FROM PTSDB WHERE INST LIKE 'x%'");
            rs = statement.executeQuery();

            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));

            assertTrue(rs.next());
            assertEquals("xy", rs.getString(1));

            assertTrue(rs.next());
            assertEquals("xyz", rs.getString(1));

            assertTrue(rs.next());
            assertEquals("xyza", rs.getString(1));

            assertTrue(rs.next());
            assertEquals("xyzab", rs.getString(1));

            assertFalse(rs.next());

            // Test 2
            statement = conn.prepareStatement("SELECT INST FROM PTSDB WHERE INST LIKE 'xy_a%'");
            rs = statement.executeQuery();

            assertTrue(rs.next());
            assertEquals("xyza", rs.getString(1));

            assertTrue(rs.next());
            assertEquals("xyzab", rs.getString(1));

            assertFalse(rs.next());

            // Test 3
            statement = conn.prepareStatement("SELECT INST FROM PTSDB WHERE INST NOT LIKE 'xy_a%'");
            rs = statement.executeQuery();

            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));

            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));

            assertTrue(rs.next());
            assertEquals("xy", rs.getString(1));

            assertTrue(rs.next());
            assertEquals("xyz", rs.getString(1));

            assertTrue(rs.next());
            assertEquals("z", rs.getString(1));

            assertFalse(rs.next());

            // Test 4
            statement = conn.prepareStatement("SELECT INST FROM PTSDB WHERE 'xzabc' LIKE 'xy_a%'");
            rs = statement.executeQuery();
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }

    @Test
    public void testIsNullInPK() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),PTSDB_NAME,null, ts-2);

        // Insert all rows at ts
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        PreparedStatement stmt = conn.prepareStatement("upsert into PTSDB VALUES ('', '', ?, 0.5)");
        stmt.setDate(1, D1);
        stmt.execute();
        conn.close();

        String query = "SELECT HOST,INST,DATE FROM PTSDB WHERE HOST IS NULL AND INST IS NULL AND DATE=?";
        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setDate(1, D1);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertNull(rs.getString(1));
            assertNull(rs.getString(2));
            assertEquals(D1, rs.getDate(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testLengthFunction() throws Exception {
        long ts = nextTimestamp();
        String query[] = {
            "SELECT length('') FROM BTABLE LIMIT 1",
            "SELECT length(' ') FROM BTABLE LIMIT 1",
            "SELECT length('1') FROM BTABLE LIMIT 1",
            "SELECT length('1234') FROM BTABLE LIMIT 1",
            "SELECT length('ɚɦɰɸ') FROM BTABLE LIMIT 1",
            "SELECT length('ǢǛǟƈ') FROM BTABLE LIMIT 1",
            "SELECT length('This is a test!') FROM BTABLE LIMIT 1",
            "SELECT A_STRING FROM BTABLE WHERE length(A_STRING)=3",
        };
        String result[] = {
            null,
            "1",
            "1",
            "4",
            "4",
            "4",
            "15",
            "abc",
        };
        assertEquals(query.length,result.length);
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            for (int i = 0; i < query.length; i++) {
                PreparedStatement statement = conn.prepareStatement(query[i]);
                ResultSet rs = statement.executeQuery();
                assertTrue(rs.next());
                assertEquals(query[i],result[i], rs.getString(1));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpperFunction() throws Exception {
        long ts = nextTimestamp();
        String query[] = {
                "SELECT upper('abc') FROM BTABLE LIMIT 1",
                "SELECT upper('Abc') FROM BTABLE LIMIT 1",
                "SELECT upper('ABC') FROM BTABLE LIMIT 1",
                "SELECT upper('ĎďĒ') FROM BTABLE LIMIT 1",
                "SELECT upper('ß') FROM BTABLE LIMIT 1",
        };
        String result[] = {
                "ABC",
                "ABC",
                "ABC",
                "ĎĎĒ",
                "SS",
        };
        assertEquals(query.length, result.length);
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            for (int i = 0; i < query.length; i++) {
                PreparedStatement statement = conn.prepareStatement(query[i]);
                ResultSet rs = statement.executeQuery();
                assertTrue(rs.next());
                assertEquals(query[i],result[i], rs.getString(1));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testLowerFunction() throws Exception {
        long ts = nextTimestamp();
        String query[] = {
                "SELECT lower('abc') FROM BTABLE LIMIT 1",
                "SELECT lower('Abc') FROM BTABLE LIMIT 1",
                "SELECT lower('ABC') FROM BTABLE LIMIT 1",
                "SELECT lower('ĎďĒ') FROM BTABLE LIMIT 1",
                "SELECT lower('ß') FROM BTABLE LIMIT 1",
                "SELECT lower('SS') FROM BTABLE LIMIT 1",
        };
        String result[] = {
                "abc",
                "abc",
                "abc",
                "ďďē",
                "ß",
                "ss",
        };
        assertEquals(query.length, result.length);
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            for (int i = 0; i < query.length; i++) {
                PreparedStatement statement = conn.prepareStatement(query[i]);
                ResultSet rs = statement.executeQuery();
                assertTrue(rs.next());
                assertEquals(query[i],result[i], rs.getString(1));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRTrimFunction() throws Exception {
        long ts = nextTimestamp();
        String query[] = {
            "SELECT rtrim('') FROM BTABLE LIMIT 1",
            "SELECT rtrim(' ') FROM BTABLE LIMIT 1",
            "SELECT rtrim('   ') FROM BTABLE LIMIT 1",
            "SELECT rtrim('abc') FROM BTABLE LIMIT 1",
            "SELECT rtrim('abc   ') FROM BTABLE LIMIT 1",
            "SELECT rtrim('abc   def') FROM BTABLE LIMIT 1",
            "SELECT rtrim('abc   def   ') FROM BTABLE LIMIT 1",
            "SELECT rtrim('ĎďĒ   ') FROM BTABLE LIMIT 1",
            "SELECT pk FROM VarcharKeyTest WHERE rtrim(pk)='jkl' LIMIT 1",
        };
        String result[] = {
            null,
            null,
            null,
            "abc",
            "abc",
            "abc   def",
            "abc   def",
            "ĎďĒ",
            "jkl   ",
        };
        assertEquals(query.length, result.length);
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            for (int i = 0; i < query.length; i++) {
                PreparedStatement statement = conn.prepareStatement(query[i]);
                ResultSet rs = statement.executeQuery();
                assertTrue(rs.next());
                assertEquals(query[i],result[i], rs.getString(1));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testLTrimFunction() throws Exception {
        long ts = nextTimestamp();
        String query[] = {
            "SELECT ltrim('') FROM BTABLE LIMIT 1",
            "SELECT ltrim(' ') FROM BTABLE LIMIT 1",
            "SELECT ltrim('   ') FROM BTABLE LIMIT 1",
            "SELECT ltrim('abc') FROM BTABLE LIMIT 1",
            "SELECT ltrim('   abc') FROM BTABLE LIMIT 1",
            "SELECT ltrim('abc   def') FROM BTABLE LIMIT 1",
            "SELECT ltrim('   abc   def') FROM BTABLE LIMIT 1",
            "SELECT ltrim('   ĎďĒ') FROM BTABLE LIMIT 1",
            "SELECT pk FROM VarcharKeyTest WHERE ltrim(pk)='def' LIMIT 1",
        };
        String result[] = {
            null,
            null,
            null,
            "abc",
            "abc",
            "abc   def",
            "abc   def",
            "ĎďĒ",
            "   def",
        };
        assertEquals(query.length, result.length);
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            for (int i = 0; i < query.length; i++) {
                PreparedStatement statement = conn.prepareStatement(query[i]);
                ResultSet rs = statement.executeQuery();
                assertTrue(rs.next());
                assertEquals(query[i],result[i], rs.getString(1));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSubstrFunctionOnRowKeyInWhere() throws Exception {
        long ts = nextTimestamp();
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts);
        Connection conn = DriverManager.getConnection(url);
        conn.createStatement().execute("CREATE TABLE substr_test (s1 varchar not null, s2 varchar not null constraint pk primary key(s1,s2))");
        conn.close();

        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 2);
        conn = DriverManager.getConnection(url);
        conn.createStatement().execute("UPSERT INTO substr_test VALUES('abc','a')");
        conn.createStatement().execute("UPSERT INTO substr_test VALUES('abcd','b')");
        conn.createStatement().execute("UPSERT INTO substr_test VALUES('abce','c')");
        conn.createStatement().execute("UPSERT INTO substr_test VALUES('abcde','d')");
        conn.commit();
        conn.close();

        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5);
        conn = DriverManager.getConnection(url);
        ResultSet rs = conn.createStatement().executeQuery("SELECT s1 from substr_test where substr(s1,1,4) = 'abcd'");
        assertTrue(rs.next());
        assertEquals("abcd",rs.getString(1));
        assertTrue(rs.next());
        assertEquals("abcde",rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testRTrimFunctionOnRowKeyInWhere() throws Exception {
        long ts = nextTimestamp();
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts);
        Connection conn = DriverManager.getConnection(url);
        conn.createStatement().execute("CREATE TABLE substr_test (s1 varchar not null, s2 varchar not null constraint pk primary key(s1,s2))");
        conn.close();

        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 2);
        conn = DriverManager.getConnection(url);
        conn.createStatement().execute("UPSERT INTO substr_test VALUES('abc','a')");
        conn.createStatement().execute("UPSERT INTO substr_test VALUES('abcd','b')");
        conn.createStatement().execute("UPSERT INTO substr_test VALUES('abcd ','c')");
        conn.createStatement().execute("UPSERT INTO substr_test VALUES('abcd  ','c')");
        conn.createStatement().execute("UPSERT INTO substr_test VALUES('abcd  a','c')"); // Need TRAVERSE_AND_LEAVE for cases like this
        conn.createStatement().execute("UPSERT INTO substr_test VALUES('abcde','d')");
        conn.commit();
        conn.close();

        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5);
        conn = DriverManager.getConnection(url);
        ResultSet rs = conn.createStatement().executeQuery("SELECT s1 from substr_test where rtrim(s1) = 'abcd'");
        assertTrue(rs.next());
        assertEquals("abcd",rs.getString(1));
        assertTrue(rs.next());
        assertEquals("abcd ",rs.getString(1));
        assertTrue(rs.next());
        assertEquals("abcd  ",rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testLikeFunctionOnRowKeyInWhere() throws Exception {
        long ts = nextTimestamp();
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts);
        Connection conn = DriverManager.getConnection(url);
        conn.createStatement().execute("CREATE TABLE substr_test (s1 varchar not null, s2 varchar not null constraint pk primary key(s1,s2))");
        conn.close();

        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 2);
        conn = DriverManager.getConnection(url);
        conn.createStatement().execute("UPSERT INTO substr_test VALUES('abc','a')");
        conn.createStatement().execute("UPSERT INTO substr_test VALUES('abcd','b')");
        conn.createStatement().execute("UPSERT INTO substr_test VALUES('abcd-','c')");
        conn.createStatement().execute("UPSERT INTO substr_test VALUES('abcd-1','c')");
        conn.createStatement().execute("UPSERT INTO substr_test VALUES('abce','d')");
        conn.commit();
        conn.close();

        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5);
        conn = DriverManager.getConnection(url);
        ResultSet rs = conn.createStatement().executeQuery("SELECT s1 from substr_test where s1 like 'abcd%1'");
        assertTrue(rs.next());
        assertEquals("abcd-1",rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testTrimFunction() throws Exception {
        long ts = nextTimestamp();
        String query[] = {
            "SELECT trim('') FROM BTABLE LIMIT 1",
            "SELECT trim(' ') FROM BTABLE LIMIT 1",
            "SELECT trim('   ') FROM BTABLE LIMIT 1",
            "SELECT trim('abc') FROM BTABLE LIMIT 1",
            "SELECT trim('   abc') FROM BTABLE LIMIT 1",
            "SELECT trim('abc   ') FROM BTABLE LIMIT 1",
            "SELECT trim('abc   def') FROM BTABLE LIMIT 1",
            "SELECT trim('   abc   def') FROM BTABLE LIMIT 1",
            "SELECT trim('abc   def   ') FROM BTABLE LIMIT 1",
            "SELECT trim('   abc   def   ') FROM BTABLE LIMIT 1",
            "SELECT trim('   ĎďĒ   ') FROM BTABLE LIMIT 1",
            "SELECT pk FROM VarcharKeyTest WHERE trim(pk)='ghi'",
        };
        String result[] = {
            null,
            null,
            null,
            "abc",
            "abc",
            "abc",
            "abc   def",
            "abc   def",
            "abc   def",
            "abc   def",
            "ĎďĒ",
            "   ghi   ",
        };
        assertEquals(query.length, result.length);
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            for (int i = 0; i < query.length; i++) {
                PreparedStatement statement = conn.prepareStatement(query[i]);
                ResultSet rs = statement.executeQuery();
                assertTrue(rs.next());
                assertEquals(query[i],result[i], rs.getString(1));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }
    }
}
