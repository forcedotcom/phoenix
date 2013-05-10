package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.*;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.util.PhoenixRuntime;

public class TopNTest extends BaseClientMangedTimeTest {

    @Test
    public void testMultiOrderByExpr() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable ORDER BY b_string, entity_id LIMIT 5";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW1);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW4);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW7);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW5);

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    

    @Test
    public void testDescMultiOrderByExpr() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable ORDER BY b_string || entity_id desc LIMIT 5";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW9);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW6);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW3);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW5);

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    

    @Test
    public void testTopNDeleteAutoCommitOn() throws Exception {
        testTopNDelete(true);
    }
    
    @Test
    public void testTopNDeleteAutoCommitOff() throws Exception {
        testTopNDelete(false);
    }
    
    private void testTopNDelete(boolean autoCommit) throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "DELETE FROM aTable ORDER BY b_string, entity_id LIMIT 5";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.setAutoCommit(autoCommit);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            assertEquals(5,statement.getUpdateCount());
            if (!autoCommit) {
                conn.commit();
            }
        } finally {
            conn.close();
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4)); // Execute at timestamp 4
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        query = "SELECT entity_id FROM aTable ORDER BY b_string, x_decimal nulls last, 8-a_integer LIMIT 5";
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW6, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW3, rs.getString(1));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    

}
