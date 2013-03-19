package com.salesforce.phoenix.end2end;

import static org.junit.Assert.*;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

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
