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


public class ReadIsolationLevelTest extends BaseClientMangedTimeTest {
    private static final String ENTITY_ID1= "000000000000001";
    private static final String ENTITY_ID2= "000000000000002";
    private static final String VALUE1 = "a";
    private static final String VALUE2= "b";

    protected static void initTableValues(long ts, byte[][] splits) throws Exception {
        String tenantId = getOrganizationId();
        ensureTableCreated(getUrl(),ATABLE_NAME,splits, ts-2);

        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection upsertConn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        // Insert all rows at ts
        PreparedStatement stmt = upsertConn.prepareStatement(
                "upsert into ATABLE VALUES (?, ?, ?)");
        stmt.setString(1, tenantId);
        stmt.setString(2, ENTITY_ID1);
        stmt.setString(3, VALUE1);
        stmt.execute(); // should commit too
        
        stmt.setString(2, ENTITY_ID2);
        stmt.setString(3, VALUE2);
        stmt.execute(); // should commit too

        upsertConn.commit();
        upsertConn.close();
    }

    @Test
    public void testStatementReadIsolationLevel() throws Exception {
        long ts = nextTimestamp();
        initTableValues(ts, null);
        String query = "SELECT A_STRING FROM ATABLE WHERE ORGANIZATION_ID=? AND ENTITY_ID=?";
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+1));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.setAutoCommit(true);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+2));
        Connection conn2 = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+1));
        Connection conn3 = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            String tenantId = getOrganizationId();
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, ENTITY_ID1);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(VALUE1, rs.getString(1));
            assertFalse(rs.next());

            // Locate existing row and reset one of it's KVs.
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement("upsert into ATABLE VALUES (?, ?, ?)");
            stmt.setString(1, tenantId);
            stmt.setString(2, ENTITY_ID1);
            stmt.setString(3, VALUE2);
            stmt.execute();
            
            PreparedStatement statement2 = conn2.prepareStatement(query);
            statement2.setString(1, tenantId);
            statement2.setString(2, ENTITY_ID1);
            // Run another query through same connection and make sure
            // you can find the new row
            rs = statement2.executeQuery();
            assertTrue(rs.next());
            assertEquals(VALUE2, rs.getString(1));
            assertFalse(rs.next());

            PreparedStatement statement3 = conn3.prepareStatement(query);
            statement3.setString(1, tenantId);
            statement3.setString(2, ENTITY_ID1);
            rs = statement3.executeQuery();
            assertTrue(rs.next());
            assertEquals(VALUE1, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
            conn2.close();
        }
    }

    @Test
    public void testConnectionReadIsolationLevel() throws Exception {
        long ts = nextTimestamp();
        initTableValues(ts, null);
        String query = "SELECT A_STRING FROM ATABLE WHERE ORGANIZATION_ID=? AND ENTITY_ID=?";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts+1);
        Connection conn = DriverManager.getConnection(url, TEST_PROPERTIES);
        conn.setAutoCommit(true);
        try {
            String tenantId = getOrganizationId();
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, ENTITY_ID1);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(VALUE1, rs.getString(1));
            assertFalse(rs.next());

            // Locate existing row and reset one of it's KVs.
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement("upsert into ATABLE VALUES (?, ?, ?)");
            stmt.setString(1, tenantId);
            stmt.setString(2, ENTITY_ID1);
            stmt.setString(3, VALUE2);
            stmt.execute();
            
            // Run another query through same connection and make sure
            // you can't find the new row
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(VALUE1, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
