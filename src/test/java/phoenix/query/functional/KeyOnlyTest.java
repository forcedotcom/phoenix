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
package phoenix.query.functional;

import static org.junit.Assert.*;
import static phoenix.util.TestUtil.*;

import java.sql.*;
import java.util.Properties;

import org.junit.Test;

import phoenix.util.PhoenixRuntime;

public class KeyOnlyTest extends BaseClientMangedTimeTest {
    @Test
    public void testKeyOnly() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),KEYONLY_NAME,null, ts);
        initTableValues(ts+1);
        Properties props = new Properties();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+5));
        Connection conn5 = DriverManager.getConnection(getUrl(), props);
        String query = "SELECT i1, i2 FROM KEYONLY";
        PreparedStatement statement = conn5.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals(4, rs.getInt(2));
        assertFalse(rs.next());
        conn5.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+6));
        Connection conn6 = DriverManager.getConnection(getUrl(), props);
        conn6.createStatement().execute("ALTER TABLE KEYONLY ADD cf(s1 varchar)");
        conn6.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+7));
        Connection conn7 = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn7.prepareStatement(
                "upsert into " +
                "KEYONLY VALUES (?, ?, ?)");
        stmt.setInt(1, 5);
        stmt.setInt(2, 6);
        stmt.setString(3, "foo");
        stmt.execute();
        conn7.commit();
        conn7.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+8));
        Connection conn8 = DriverManager.getConnection(getUrl(), props);
        query = "SELECT i1 FROM KEYONLY";
        statement = conn8.prepareStatement(query);
        rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertFalse(rs.next());
        
        query = "SELECT i1,s1 FROM KEYONLY";
        statement = conn8.prepareStatement(query);
        rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(null, rs.getString(2));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals(null, rs.getString(2));
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertEquals("foo", rs.getString(2));
        assertFalse(rs.next());

        conn8.close();
    }
    
    protected static void initTableValues(long ts) throws Exception {
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement stmt = conn.prepareStatement(
            "upsert into " +
            "KEYONLY VALUES (?, ?)");
        stmt.setInt(1, 1);
        stmt.setInt(2, 2);
        stmt.execute();
        
        stmt.setInt(1, 3);
        stmt.setInt(2, 4);
        stmt.execute();
        
        conn.commit();
        conn.close();
    }
        
}
