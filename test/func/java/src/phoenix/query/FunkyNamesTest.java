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
package phoenix.query;

import static org.junit.Assert.*;
import static phoenix.util.TestUtil.*;

import java.sql.*;
import java.util.Properties;

import org.junit.Test;

import phoenix.schema.ColumnNotFoundException;
import phoenix.util.PhoenixRuntime;

public class FunkyNamesTest extends BaseClientMangedTimeTest {

    protected static void initTableValues(byte[][] splits, long ts) throws Exception {
        ensureTableCreated(getUrl(),FUNKY_NAME,splits, ts-2);

        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        // Insert all rows at ts
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " +
                "FUNKY_NAMES(" +
                "    \"foo!\", " +
                "    \"#@$\", " +
                "    \"foo.bar-bas\", " +
                "    \"_blah^\"," +
                "    \"Value\", " +
                "    \"VALUE\", " +
                "    \"value\") " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)");
        stmt.setString(1, "a");
        stmt.setString(2, "b");
        stmt.setString(3, "c");
        stmt.setString(4, "d");
        stmt.setInt(5, 1);
        stmt.setInt(6, 2);
        stmt.setInt(7, 3);
        stmt.executeUpdate();
        conn.close();
    }

    @Test
    public void testUnaliasedFunkyNames() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT \"foo!\",\"#@$\",\"foo.bar-bas\",\"_blah^\" FROM FUNKY_NAMES";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("b", rs.getString(2));
            assertEquals("c", rs.getString(3));
            assertEquals("d", rs.getString(4));
            
            assertEquals("a", rs.getString("foo!"));
            assertEquals("b", rs.getString("#@$"));
            assertEquals("c", rs.getString("foo.bar-bas"));
            assertEquals("d", rs.getString("_blah^"));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCaseSensitive() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT \"Value\",\"VALUE\",\"value\" FROM FUNKY_NAMES";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(3, rs.getInt(3));
            
            assertEquals(1, rs.getInt("Value"));
            assertEquals(2, rs.getInt("VALUE"));
            assertEquals(3, rs.getInt("value"));
            try {
                rs.getInt("vAlue");
                fail();
            } catch (ColumnNotFoundException e) {
            }
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testAliasedFunkyNames() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT \"1-3.4$\".\"foo!\" as \"1-2\",\"#@$\" as \"[3]\",\"foo.bar-bas\" as \"$$$\",\"_blah^\" \"0\" FROM FUNKY_NAMES \"1-3.4$\"";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("a", rs.getString("1-2"));
            assertEquals("b", rs.getString("[3]"));
            assertEquals("c", rs.getString("$$$"));
            assertEquals("d", rs.getString("0"));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}

