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
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.util.PhoenixRuntime;

public class DistinctCountTest extends BaseClientMangedTimeTest {

    @Test
    public void testDistinctCountOnColumn() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT count(DISTINCT A_STRING) FROM aTable";

        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(3, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDistinctCountOnRKColumn() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT count(DISTINCT ORGANIZATION_ID) FROM aTable";

        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDistinctCountWithGroupBy() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT A_STRING, count(DISTINCT B_STRING) FROM aTable group by A_STRING";

        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(A_VALUE, rs.getString(1));
            assertEquals(2, rs.getLong(2));
            assertTrue(rs.next());
            assertEquals(B_VALUE, rs.getString(1));
            assertEquals(1, rs.getLong(2));
            assertTrue(rs.next());
            assertEquals(C_VALUE, rs.getString(1));
            assertEquals(1, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDistinctCountWithGroupByOrdered() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String tenantId2 = "00D400000000XHP";
        initATableValues(tenantId, tenantId2, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT organization_id, count(DISTINCT A_STRING) FROM aTable group by organization_id";

        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(tenantId, rs.getString(1));
            assertEquals(3, rs.getLong(2));
            assertTrue(rs.next());
            assertEquals(tenantId2, rs.getString(1));
            assertEquals(1, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDistinctCountOn2Columns() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT count(DISTINCT A_STRING), count(DISTINCT B_STRING) FROM aTable";

        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(3, rs.getLong(1));
            assertEquals(2, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDistinctCountONE() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT count(DISTINCT 1) FROM aTable";

        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDistinctCountONEWithEmptyResult() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(null, null, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT count(DISTINCT 1) FROM aTable";

        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    protected static void initATableValues(String tenantId1, String tenantId2, byte[][] splits, Date date, Long ts) throws Exception {
        if (ts == null) {
            ensureTableCreated(getUrl(), ATABLE_NAME, splits);
        } else {
            ensureTableCreated(getUrl(), ATABLE_NAME, splits, ts-2);
        }
        
        Properties props = new Properties();
        if (ts != null) {
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, ts.toString());
        }
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " +
                    "ATABLE(" +
                    "    ORGANIZATION_ID, " +
                    "    ENTITY_ID, " +
                    "    A_STRING, " +
                    "    B_STRING, " +
                    "    A_INTEGER, " +
                    "    A_DATE, " +
                    "    X_DECIMAL, " +
                    "    X_LONG, " +
                    "    X_INTEGER," +
                    "    Y_INTEGER)" +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            if (tenantId1 != null) {
                stmt.setString(1, tenantId1);
                stmt.setString(2, ROW1);
                stmt.setString(3, A_VALUE);
                stmt.setString(4, B_VALUE);
                stmt.setInt(5, 1);
                stmt.setDate(6, date);
                stmt.setBigDecimal(7, null);
                stmt.setNull(8, Types.BIGINT);
                stmt.setNull(9, Types.INTEGER);
                stmt.setNull(10, Types.INTEGER);
                stmt.execute();

                stmt.setString(1, tenantId1);
                stmt.setString(2, ROW2);
                stmt.setString(3, A_VALUE);
                stmt.setString(4, C_VALUE);
                stmt.setInt(5, 2);
                stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 1));
                stmt.setBigDecimal(7, null);
                stmt.setNull(8, Types.BIGINT);
                stmt.setNull(9, Types.INTEGER);
                stmt.setNull(10, Types.INTEGER);
                stmt.execute();

                stmt.setString(1, tenantId1);
                stmt.setString(2, ROW3);
                stmt.setString(3, A_VALUE);
                stmt.setString(4, C_VALUE);
                stmt.setInt(5, 3);
                stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 2));
                stmt.setBigDecimal(7, null);
                stmt.setNull(8, Types.BIGINT);
                stmt.setNull(9, Types.INTEGER);
                stmt.setNull(10, Types.INTEGER);
                stmt.execute();

                stmt.setString(1, tenantId1);
                stmt.setString(2, ROW4);
                stmt.setString(3, A_VALUE);
                stmt.setString(4, B_VALUE);
                stmt.setInt(5, 4);
                stmt.setDate(6, date == null ? null : date);
                stmt.setBigDecimal(7, null);
                stmt.setNull(8, Types.BIGINT);
                stmt.setNull(9, Types.INTEGER);
                stmt.setNull(10, Types.INTEGER);
                stmt.execute();

                stmt.setString(1, tenantId1);
                stmt.setString(2, ROW5);
                stmt.setString(3, B_VALUE);
                stmt.setString(4, C_VALUE);
                stmt.setInt(5, 5);
                stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 1));
                stmt.setBigDecimal(7, null);
                stmt.setNull(8, Types.BIGINT);
                stmt.setNull(9, Types.INTEGER);
                stmt.setNull(10, Types.INTEGER);
                stmt.execute();

                stmt.setString(1, tenantId1);
                stmt.setString(2, ROW6);
                stmt.setString(3, B_VALUE);
                stmt.setString(4, C_VALUE);
                stmt.setInt(5, 6);
                stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 2));
                stmt.setBigDecimal(7, null);
                stmt.setNull(8, Types.BIGINT);
                stmt.setNull(9, Types.INTEGER);
                stmt.setNull(10, Types.INTEGER);
                stmt.execute();

                stmt.setString(1, tenantId1);
                stmt.setString(2, ROW7);
                stmt.setString(3, B_VALUE);
                stmt.setString(4, C_VALUE);
                stmt.setInt(5, 7);
                stmt.setDate(6, date == null ? null : date);
                stmt.setBigDecimal(7, BigDecimal.valueOf(0.1));
                stmt.setLong(8, 5L);
                stmt.setInt(9, 5);
                stmt.setNull(10, Types.INTEGER);
                stmt.execute();

                stmt.setString(1, tenantId1);
                stmt.setString(2, ROW8);
                stmt.setString(3, B_VALUE);
                stmt.setString(4, C_VALUE);
                stmt.setInt(5, 8);
                stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 1));
                stmt.setBigDecimal(7, BigDecimal.valueOf(3.9));
                long l = Integer.MIN_VALUE - 1L;
                assert (l < Integer.MIN_VALUE);
                stmt.setLong(8, l);
                stmt.setInt(9, 4);
                stmt.setNull(10, Types.INTEGER);
                stmt.execute();

                stmt.setString(1, tenantId1);
                stmt.setString(2, ROW9);
                stmt.setString(3, C_VALUE);
                stmt.setString(4, C_VALUE);
                stmt.setInt(5, 9);
                stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 2));
                stmt.setBigDecimal(7, BigDecimal.valueOf(3.3));
                l = Integer.MAX_VALUE + 1L;
                assert (l > Integer.MAX_VALUE);
                stmt.setLong(8, l);
                stmt.setInt(9, 3);
                stmt.setInt(10, 300);
                stmt.execute();
            }
            if (tenantId2 != null) {
                stmt.setString(1, tenantId2);
                stmt.setString(2, ROW1);
                stmt.setString(3, A_VALUE);
                stmt.setString(4, B_VALUE);
                stmt.setInt(5, 1);
                stmt.setDate(6, date);
                stmt.setBigDecimal(7, null);
                stmt.setNull(8, Types.BIGINT);
                stmt.setNull(9, Types.INTEGER);
                stmt.setNull(10, Types.INTEGER);
                stmt.execute();

                stmt.setString(1, tenantId2);
                stmt.setString(2, ROW2);
                stmt.setString(3, A_VALUE);
                stmt.setString(4, C_VALUE);
                stmt.setInt(5, 2);
                stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 1));
                stmt.setBigDecimal(7, null);
                stmt.setNull(8, Types.BIGINT);
                stmt.setNull(9, Types.INTEGER);
                stmt.setNull(10, Types.INTEGER);
                stmt.execute();
            }
            conn.commit();
        } finally {
            conn.close();
        }
    }
}
