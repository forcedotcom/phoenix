package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

import org.junit.Test;

public class BinaryRowKeyTest extends BaseHBaseManagedTimeTest {

    private static void initTableValues() throws SQLException {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        
        try {
            String ddl = "CREATE TABLE test_table" +
                    "   (a_binary binary(10) not null, \n" +
                    "    a_string varchar not null, \n" +
                    "    b_binary varbinary \n" +
                    "    CONSTRAINT pk PRIMARY KEY (a_binary, a_string))\n";
            createTestTable(getUrl(), ddl);
            
            String query;
            PreparedStatement stmt;
            
            query = "UPSERT INTO test_table"
                    + "(a_binary, a_string) "
                    + "VALUES(?,?)";
            stmt = conn.prepareStatement(query);
            
            stmt.setBytes(1, new byte[] {0,0,0,0,0,0,0,0,0,1});
            stmt.setString(2, "a");
            stmt.execute();
            
            stmt.setBytes(1, new byte[] {0,0,0,0,0,0,0,0,0,2});
            stmt.setString(2, "b");
            stmt.execute();
            conn.commit();
            
        } finally {
            conn.close();
        }
    }

    @Test
    public void testInsertBadBinaryValue() throws SQLException {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues();
            
            String query = "UPSERT INTO test_table"
                    + "(a_binary, a_string) "
                    + "VALUES(?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setBytes(1, new byte[] {0,0,0,0,0,0,0,0,1});
            stmt.setString(2, "a");
            stmt.execute();
            conn.commit();
            fail("Should have caught bad insert.");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("TEST_TABLE.A_BINARY must be 10 bytes"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectValues() throws SQLException {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        
        try {
            initTableValues();
            
            String query = "SELECT * FROM test_table";
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            
            assertTrue(rs.next());
            assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,1}, rs.getBytes(1));
            assertEquals("a", rs.getString(2));
            
            assertTrue(rs.next());
            assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,2}, rs.getBytes(1));
            assertEquals("b", rs.getString(2));
            
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpsertSelectValues() throws SQLException {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        
        try {
            initTableValues();
            
            String query = "UPSERT INTO test_table (a_binary, a_string, b_binary) "
                    + " SELECT a_binary, a_string, a_binary FROM test_table";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            
            query = "SELECT a_binary, b_binary FROM test_table";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            
            assertTrue(rs.next());
            assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,1}, rs.getBytes(1));
            assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,1}, rs.getBytes(2));
            
            assertTrue(rs.next());
            assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,2}, rs.getBytes(1));
            assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,2}, rs.getBytes(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
