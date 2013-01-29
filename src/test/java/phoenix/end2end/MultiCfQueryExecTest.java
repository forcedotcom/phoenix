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
package phoenix.end2end;

import static org.junit.Assert.*;
import static phoenix.util.TestUtil.PHOENIX_JDBC_URL;
import static phoenix.util.TestUtil.TEST_PROPERTIES;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;

import org.junit.Test;

import phoenix.util.PhoenixRuntime;

public class MultiCfQueryExecTest extends BaseClientMangedTimeTest {
    private static final String MULTI_CF = "MULTI_CF";
    
    protected static void initTableValues(long ts) throws Exception {
        ensureTableCreated(getUrl(),MULTI_CF,null, ts-2);
        
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        // Insert all rows at ts
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " +
                "MULTI_CF(" +
                "    ID, " +
                "    TRANSACTION_COUNT, " +
                "    CPU_UTILIZATION, " +
                "    DB_CPU_UTILIZATION," +
                "    UNIQUE_USER_COUNT," +
                "    F.RESPONSE_TIME," +
                "    G.RESPONSE_TIME)" +
                "VALUES (?, ?, ?, ?, ?, ?, ?)");
        stmt.setString(1, "000000000000001");
        stmt.setInt(2, 100);
        stmt.setBigDecimal(3, BigDecimal.valueOf(0.5));
        stmt.setBigDecimal(4, BigDecimal.valueOf(0.2));
        stmt.setInt(5, 1000);
        stmt.setLong(6, 11111);
        stmt.setLong(7, 11112);
        stmt.execute();
        stmt.setString(1, "000000000000002");
        stmt.setInt(2, 200);
        stmt.setBigDecimal(3, BigDecimal.valueOf(2.5));
        stmt.setBigDecimal(4, BigDecimal.valueOf(2.2));
        stmt.setInt(5, 2000);
        stmt.setLong(6, 2222);
        stmt.setLong(7, 22222);
        stmt.execute();
    }
    
    @Test
    public void testConstantCount() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT count(1) from multi_cf";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCFToDisambiguateInSelectOnly1() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from multi_cf where ID = '000000000000002'";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2222, rs.getLong(1));
            assertEquals(22222, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCFToDisambiguateInSelectOnly2() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from multi_cf where TRANSACTION_COUNT = 200";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2222, rs.getLong(1));
            assertEquals(22222, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCFToDisambiguate1() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from multi_cf where F.RESPONSE_TIME = 2222";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2222, rs.getLong(1));
            assertEquals(22222, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCFToDisambiguate2() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from multi_cf where G.RESPONSE_TIME-1 = F.RESPONSE_TIME";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(11111, rs.getLong(1));
            assertEquals(11112, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDefaultCFToDisambiguate() throws Exception {
        long ts = nextTimestamp();
        initTableValues(ts);
        
        String ddl = "ALTER TABLE multi_cf ADD response_time BIGINT";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 3);
        Connection conn = DriverManager.getConnection(url);
        conn.createStatement().execute(ddl);
        conn.close();
       
        String dml = "upsert into " +
        "MULTI_CF(" +
        "    ID, " +
        "    RESPONSE_TIME)" +
        "VALUES ('000000000000003', 333)";
        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 4); // Run query at timestamp 5
        conn = DriverManager.getConnection(url);
        conn.createStatement().execute(dml);
        conn.commit();
        conn.close();

        String query = "SELECT ID,RESPONSE_TIME from multi_cf where RESPONSE_TIME = 333";
        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        conn = DriverManager.getConnection(url);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("000000000000003", rs.getString(1));
            assertEquals(333, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
}
