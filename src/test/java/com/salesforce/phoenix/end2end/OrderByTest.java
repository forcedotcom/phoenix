package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.*;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.util.PhoenixRuntime;

public class OrderByTest extends BaseClientMangedTimeTest {

    @Test
    public void testMultiOrderByExprNoSpool() throws Exception {
        testMultiOrderByExpr(1024 * 1024);
    }
    
    @Test
    public void testMultiOrderByExprWithSpool() throws Exception {
        testMultiOrderByExpr(100);
    }

    private void testMultiOrderByExpr(int thresholdBytes) throws Exception {
        Configuration config = driver.getQueryServices().getConfig();
        config.setInt(QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, thresholdBytes);
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable ORDER BY b_string, entity_id";
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
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW3);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW6);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW9);

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    

    @Test
    public void testDescMultiOrderByExprNoSpool() throws Exception {
        testDescMultiOrderByExpr(1024 * 1024);
    }

    @Test
    public void testDescMultiOrderByExprWithSpool() throws Exception {
        testDescMultiOrderByExpr(100);
    }

    private void testDescMultiOrderByExpr(int thresholdBytes) throws Exception {
        Configuration config = driver.getQueryServices().getConfig();
        config.setInt(QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, thresholdBytes);
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
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW7);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW4);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW1);

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
        

}
