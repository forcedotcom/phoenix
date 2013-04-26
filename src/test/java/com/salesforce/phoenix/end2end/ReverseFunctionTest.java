package com.salesforce.phoenix.end2end;

import static org.junit.Assert.*;

import java.sql.*;

import org.junit.Test;

public class ReverseFunctionTest extends BaseHBaseManagedTimeTest {
    private void initTable(Connection conn, String sortOrder, String s) throws Exception {
        String ddl = "CREATE TABLE REVERSE_TEST (pk VARCHAR NOT NULL PRIMARY KEY " + sortOrder + ", kv VARCHAR)";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO REVERSE_TEST VALUES(?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setString(1, s);
        stmt.execute();
        conn.commit();        
    }
    
    private void testReverse(Connection conn, String s) throws Exception {
        StringBuilder buf = new StringBuilder(s);
        String reverse = buf.reverse().toString();
        
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT reverse(pk) FROM REVERSE_TEST");
        assertTrue(rs.next());
        assertEquals(reverse, rs.getString(1));
        assertFalse(rs.next());
        
        PreparedStatement stmt = conn.prepareStatement("SELECT pk FROM REVERSE_TEST WHERE pk=reverse(?)");
        stmt.setString(1, reverse);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(s, rs.getString(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testSingleByteReverseAscending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String s = "abc";
        initTable(conn, "ASC", s);
        testReverse(conn, s);
    }                                                           

    @Test
    public void testMultiByteReverseAscending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String s = "ɚɦɰɸ";
        initTable(conn, "DESC", s);
        testReverse(conn, s);
    }                                                           

    
    @Test
    public void testSingleByteReverseDecending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String s = "abc";
        initTable(conn, "DESC", s);
        testReverse(conn, s);
    }                                                           

    @Test
    public void testMultiByteReverseDecending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String s = "ɚɦɰɸ";
        initTable(conn, "ASC", s);
        testReverse(conn, s);
    }
    
    @Test
    public void testNullReverse() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String s = "abc";
        initTable(conn, "ASC", s);
        
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT reverse(kv) FROM REVERSE_TEST");
        assertTrue(rs.next());
        assertNull(rs.getString(1));
        assertFalse(rs.next());
    }                                                           

}
