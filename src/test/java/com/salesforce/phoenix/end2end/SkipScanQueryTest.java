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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.salesforce.phoenix.util.PhoenixRuntime;

public class SkipScanQueryTest extends BaseHBaseManagedTimeTest {
    
    private void initIntInTable(Connection conn, List<Integer> data) throws SQLException {
        String ddl = "CREATE TABLE IF NOT EXISTS inTest (" + 
                     "  i INTEGER NOT NULL PRIMARY KEY)";
        conn.createStatement().executeUpdate(ddl);
        
        // Test upsert correct values 
        String query = "UPSERT INTO inTest VALUES(?)";
        PreparedStatement stmt = conn.prepareStatement(query);
        for (Integer i : data) {
            stmt.setInt(1, i);
            stmt.execute();
        }
        conn.commit();
    }
    
    private void initVarCharCrossProductInTable(Connection conn, List<String> c1, List<String> c2) throws SQLException {
        String ddl = "CREATE TABLE IF NOT EXISTS inVarTest (" + 
                     "  s1 VARCHAR, s2 VARCHAR CONSTRAINT pk PRIMARY KEY (s1,s2))";
        conn.createStatement().executeUpdate(ddl);
        
        // Test upsert correct values 
        String query = "UPSERT INTO inVarTest VALUES(?,?)";
        PreparedStatement stmt = conn.prepareStatement(query);
        for (String s1 : c1) {
            for (String s2 : c2) {
                stmt.setString(1, s1);
                stmt.setString(2, s2);
                stmt.execute();
            }
        }
        conn.commit();
    }
    
    private void initVarCharParallelListInTable(Connection conn, List<String> c1, List<String> c2) throws SQLException {
        String ddl = "CREATE TABLE IF NOT EXISTS inVarTest (" + 
                     "  s1 VARCHAR, s2 VARCHAR CONSTRAINT pk PRIMARY KEY (s1,s2))";
        conn.createStatement().executeUpdate(ddl);
        
        // Test upsert correct values 
        String query = "UPSERT INTO inVarTest VALUES(?,?)";
        PreparedStatement stmt = conn.prepareStatement(query);
        for (int i = 0; i < c1.size(); i++) {
            stmt.setString(1, c1.get(i));
            stmt.setString(2, i < c2.size() ? c2.get(i) : null);
            stmt.execute();
        }
        conn.commit();
    }
    
    private static final String UPSERT_SELECT_AFTER_UPSERT_STATEMENTS = 
    		"upsert into table1(c1, c2, c3, c4, v1, v2) values('1001', '91', 's1', '2013-09-26', 28397, 23541);\n" + 
    		"upsert into table1(c1, c2, c3, c4, v1, v2) values('1001', '91', 's2', '2013-09-23', 3369, null);\n";
    private void initSelectAfterUpsertTable(Connection conn) throws Exception {
        String ddl = "create table if not exists table1("
                + "c1 VARCHAR NOT NULL," + "c2 VARCHAR NOT NULL,"
                + "c3 VARCHAR NOT NULL," + "c4 VARCHAR NOT NULL,"
                + "v1 integer," + "v2 integer "
                + "CONSTRAINT PK PRIMARY KEY (c1, c2, c3, c4)" + ")";
        conn.createStatement().execute(ddl);

        // Test upsert correct values
        StringReader reader = new StringReader(UPSERT_SELECT_AFTER_UPSERT_STATEMENTS);
        PhoenixRuntime.executeStatements(conn, reader, Collections.emptyList());
        reader.close();
        conn.commit();
    }
    
    @Test
    public void testSelectAfterUpsertInQuery() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initSelectAfterUpsertTable(conn);
        try {
            String query;
            query = "SELECT case when sum(v2)*1.0/sum(v1) is null then 0 else sum(v2)*1.0/sum(v1) END AS val FROM table1 " +
            		"WHERE c1='1001' AND c2 = '91' " +
            		"AND c3 IN ('s1','s2') AND c4='2013-09-24'";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        } finally {
            conn.close();
        }
    }
    @Test
    public void testInQuery() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(false);
        initIntInTable(conn,Arrays.asList(2,7,10));
        try {
            String query;
            query = "SELECT i FROM inTest WHERE i IN (1,2,4,5,7,8,10)";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(7, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testVarCharParallelListInQuery() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(false);
        initVarCharParallelListInTable(conn,Arrays.asList("d","da","db"),Arrays.asList("m","mc","tt"));
        try {
            String query;
            query = "SELECT s1,s2 FROM inVarTest WHERE s1 IN ('a','b','da','db') AND s2 IN ('c','ma','m','mc','ttt','z')";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("da", rs.getString(1));
            assertEquals("mc", rs.getString(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testVarCharXInQuery() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(false);
        initVarCharCrossProductInTable(conn,Arrays.asList("d","da","db"),Arrays.asList("m","mc","tt"));
        try {
            String query;
            query = "SELECT s1,s2 FROM inVarTest WHERE s1 IN ('a','b','da','db') AND s2 IN ('c','ma','m','mc','ttt','z')";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("da", rs.getString(1));
            assertEquals("m", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("da", rs.getString(1));
            assertEquals("mc", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("db", rs.getString(1));
            assertEquals("m", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("db", rs.getString(1));
            assertEquals("mc", rs.getString(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testVarCharXIntInQuery() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(false);
        initVarCharCrossProductInTable(conn,Arrays.asList("d","da","db"),Arrays.asList("m","mc","tt"));
        try {
            String query;
            query = "SELECT s1,s2 FROM inVarTest " +
                    "WHERE s1 IN ('a','b','da','db') AND s2 IN ('c','ma','m','mc','ttt','z') " +
                    "AND s1 > 'd' AND s1 < 'db' AND s2 > 'm'";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("da", rs.getString(1));
            assertEquals("mc", rs.getString(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
}
