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

import java.sql.*;
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.util.PhoenixRuntime;


/**
 * End-to-End tests on various statement hints.
 */
public class StatementHintsTest extends BaseClientMangedTimeTest {

    private static void initTableValues(byte[][] splits, long ts) throws Exception {
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(false);
        
        try {
            ensureTableCreated(getUrl(), SIMPLE_TABLE, splits, ts-2);
            String query;
            PreparedStatement stmt;
            
            query = "UPSERT INTO " + SIMPLE_TABLE
                    + "(a_integer, a_string, a_id, b_string, b_integer) "
                    + "VALUES(?,?,?,?,?)";
            stmt = conn.prepareStatement(query);
            
            stmt.setInt(1, 1);
            stmt.setString(2, "ab");
            stmt.setString(3, "123");
            stmt.setString(4, "abc");
            stmt.setInt(5, 111);
            stmt.execute();
            
            stmt.setInt(1, 1);
            stmt.setString(2, "abc");
            stmt.setString(3, "456");
            stmt.setString(4, "abc");
            stmt.setInt(5, 111);
            stmt.execute();
            
            stmt.setInt(1, 1);
            stmt.setString(2, "de");
            stmt.setString(3, "123");
            stmt.setString(4, "abc");
            stmt.setInt(5, 111);
            stmt.execute();
            
            stmt.setInt(1, 2);
            stmt.setString(2, "abc");
            stmt.setString(3, "123");
            stmt.setString(4, "def");
            stmt.setInt(5, 222);
            stmt.execute();

            stmt.setInt(1, 3);
            stmt.setString(2, "abc");
            stmt.setString(3, "123");
            stmt.setString(4, "ghi");
            stmt.setInt(5, 333);
            stmt.execute();

            stmt.setInt(1, 4);
            stmt.setString(2, "abc");
            stmt.setString(3, "123");
            stmt.setString(4, "jkl");
            stmt.setInt(5, 444);
            stmt.execute();
            conn.commit();
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectForceRangeScan() throws Exception {
        long ts = nextTimestamp();
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5);
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            String query = "SELECT /*+ RANGE_SCAN */ * FROM " + SIMPLE_TABLE + " WHERE a_integer in (1, 2, 3, 4)";
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("ab", rs.getString(2));
            assertEquals("123", rs.getString(3));
            
            assertTrue(rs.next());
            assertTrue(rs.next());
            
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("123", rs.getString(3));
            
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectForceSkipScan() throws Exception {
        long ts = nextTimestamp();
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5);
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            // second slot on the 
            String query = "SELECT /*+ SKIP_SCAN */ * FROM " + SIMPLE_TABLE + " WHERE a_string = 'abc'";
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("456", rs.getString(3));
            
            assertTrue(rs.next());
            assertTrue(rs.next());
            
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
