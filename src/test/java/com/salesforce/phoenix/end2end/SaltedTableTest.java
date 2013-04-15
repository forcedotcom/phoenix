package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.*;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.util.PhoenixRuntime;


/**
 * Tests for table with transparent salting.
 */
public class SaltedTableTest extends BaseClientMangedTimeTest {

    private static void initTableValues(byte[][] splits, long ts) throws Exception {
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        
        try {
            ensureTableCreated(getUrl(), TABLE_WITH_SALTING, splits, ts-2);
            String query = "UPSERT INTO " + TABLE_WITH_SALTING + " VALUES(?,?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "abc");
            stmt.setString(2, "123");
            stmt.setInt(3, 1);
            stmt.setString(4, "abc");
            stmt.setInt(5, 123);
            stmt.execute();
            conn.commit();
            
            stmt.setString(1, "abc");
            stmt.setString(2, "123");
            stmt.setInt(3, 2);
            stmt.setString(4, "def");
            stmt.setInt(5, 456);
            stmt.execute();
            conn.commit();
            
            stmt.setString(1, "abc");
            stmt.setString(2, "123");
            stmt.setInt(3, 3);
            stmt.setString(4, "ghi");
            stmt.setInt(5, 789);
            stmt.execute();
            conn.commit();
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectValueNoWhereClause() throws Exception {
        long ts = nextTimestamp();
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5);
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            
            String query = "SELECT * FROM " + TABLE_WITH_SALTING + " ORDER BY a_integer ASC LIMIT 3";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals("123", rs.getString(2));
            assertEquals(1, rs.getInt(3));
            assertEquals("abc", rs.getString(4));
            assertEquals(123, rs.getInt(5));
            
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals("123", rs.getString(2));
            assertEquals(2, rs.getInt(3));
            assertEquals("def", rs.getString(4));
            assertEquals(456, rs.getInt(5));
            
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals("123", rs.getString(2));
            assertEquals(3, rs.getInt(3));
            assertEquals("ghi", rs.getString(4));
            assertEquals(789, rs.getInt(5));
            
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectValueWithWhereClause() throws Exception {
        long ts = nextTimestamp();
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5);
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            
            String query = "SELECT * FROM " + TABLE_WITH_SALTING + " WHERE a_integer = ?";
            PreparedStatement stmt = conn.prepareStatement(query);
            
            stmt.setInt(1, 1);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals("123", rs.getString(2));
            assertEquals(1, rs.getInt(3));
            assertEquals("abc", rs.getString(4));
            assertEquals(123, rs.getInt(5));
            assertFalse(rs.next());
            
            stmt.setInt(1, 2);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals("123", rs.getString(2));
            assertEquals(2, rs.getInt(3));
            assertEquals("def", rs.getString(4));
            assertEquals(456, rs.getInt(5));
            assertFalse(rs.next());
            
            stmt.setInt(1, 3);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals("123", rs.getString(2));
            assertEquals(3, rs.getInt(3));
            assertEquals("ghi", rs.getString(4));
            assertEquals(789, rs.getInt(5));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
