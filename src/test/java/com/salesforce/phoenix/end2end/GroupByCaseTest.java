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


public class GroupByCaseTest extends BaseClientMangedTimeTest {

    private static String GROUPBY1 = "select " +
            "case when uri LIKE 'Report%' then 'Reports' else 'Other' END" +
            ", avg(appcpu) from " + GROUPBYTEST_NAME +
            " group by " + "case when uri LIKE 'Report%' then 'Reports' else 'Other' END";

    private static String GROUPBY2 = "select " +
            "case uri when 'Report%' then 'Reports' else 'Other' END" +
            ", avg(appcpu) from " + GROUPBYTEST_NAME +
            " group by appcpu, " + "case uri when 'Report%' then 'Reports' else 'Other' END";

    private static String GROUPBY3 = "select " +
            "case uri when 'Report%' then 'Reports' else 'Other' END" +
            ", avg(appcpu) from " + GROUPBYTEST_NAME +
            " group by avg(appcpu), " + "case uri when 'Report%' then 'Reports' else 'Other' END";
    
    private int id;

    private long createTable() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(), GROUPBYTEST_NAME, null, ts-2);
        return ts;
    }

    private void loadData(long ts) throws SQLException {
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        insertRow(conn, "Report1", 10);
        insertRow(conn, "Report2", 10);
        insertRow(conn, "Report3", 30);
        insertRow(conn, "Report4", 30);
        insertRow(conn, "SOQL1", 10);
        insertRow(conn, "SOQL2", 10);
        insertRow(conn, "SOQL3", 30);
        insertRow(conn, "SOQL4", 30);
        conn.commit();
        conn.close();
    }

    private void insertRow(Connection conn, String uri, int appcpu) throws SQLException {
        PreparedStatement statement = conn.prepareStatement("UPSERT INTO " + GROUPBYTEST_NAME + "(id, uri, appcpu) values (?,?,?)");
        statement.setString(1, "id" + id);
        statement.setString(2, uri);
        statement.setInt(3, appcpu);
        statement.executeUpdate();
        id++;
    }

    @Test
    public void testGroupByCase() throws Exception {
        GroupByCaseTest gbt = new GroupByCaseTest();
        long ts = gbt.createTable();
        gbt.loadData(ts);
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        gbt.executeQuery(conn,GROUPBY1);
        gbt.executeQuery(conn,GROUPBY2);
        // TODO: validate query results
        try {
            gbt.executeQuery(conn,GROUPBY3);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Aggregate expressions may not be used in GROUP BY"));
        }
        conn.close();
    }

    @Test
    public void testScanUri() throws Exception {
        GroupByCaseTest gbt = new GroupByCaseTest();
        long ts = gbt.createTable();
        gbt.loadData(ts);
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select uri from " + GROUPBYTEST_NAME);
        assertTrue(rs.next());
        assertEquals("Report1", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("Report2", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("Report3", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("Report4", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("SOQL1", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("SOQL2", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("SOQL3", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("SOQL4", rs.getString(1));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testCount() throws Exception {
        GroupByCaseTest gbt = new GroupByCaseTest();
        long ts = gbt.createTable();
        gbt.loadData(ts);
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(1) from " + GROUPBYTEST_NAME);
        assertTrue(rs.next());
        assertEquals(8, rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="RV_RETURN_VALUE_IGNORED",
            justification="Test code.")
    private void executeQuery(Connection conn, String query) throws SQLException {
        PreparedStatement st = conn.prepareStatement(query);
        st.executeQuery();
    }
}
