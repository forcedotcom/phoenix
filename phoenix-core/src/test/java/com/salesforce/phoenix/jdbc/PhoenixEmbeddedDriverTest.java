package com.salesforce.phoenix.jdbc;

import static org.junit.Assert.*;

import java.sql.*;

import org.junit.Test;

import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;

public class PhoenixEmbeddedDriverTest {
    @Test
    public void testGetConnectionInfo() throws SQLException {
        String[] urls = new String[] {
            "jdbc:phoenix",
            "jdbc:phoenix;test=true",
            "jdbc:phoenix:localhost",
            "jdbc:phoenix:localhost:123",
            "jdbc:phoenix:localhost:123;foo=bar",
            "jdbc:phoenix:localhost:123:/hbase",
            "jdbc:phoenix:localhost:123:/hbase;foo=bas",
            "jdbc:phoenix:localhost:/hbase",
            "jdbc:phoenix:localhost:/hbase;test=true",
            "jdbc:phoenix:v1,v2,v3",
            "jdbc:phoenix:v1,v2,v3;",
            "jdbc:phoenix:v1,v2,v3;test=true",
            "jdbc:phoenix:v1,v2,v3:/hbase",
            "jdbc:phoenix:v1,v2,v3:/hbase;test=true",
            "jdbc:phoenix:v1,v2,v3:123:/hbase",
            "jdbc:phoenix:v1,v2,v3:123:/hbase;test=false",
        };
        ConnectionInfo[] infos = new ConnectionInfo[] {
            new ConnectionInfo(null,null,null),
            new ConnectionInfo(null,null,null),
            new ConnectionInfo("localhost",null,null),
            new ConnectionInfo("localhost",123,null),
            new ConnectionInfo("localhost",123,null),
            new ConnectionInfo("localhost",123,"/hbase"),
            new ConnectionInfo("localhost",123,"/hbase"),
            new ConnectionInfo("localhost",null,"/hbase"),
            new ConnectionInfo("localhost",null,"/hbase"),
            new ConnectionInfo("v1,v2,v3",null,null),
            new ConnectionInfo("v1,v2,v3",null,null),
            new ConnectionInfo("v1,v2,v3",null,null),
            new ConnectionInfo("v1,v2,v3",null,"/hbase"),
            new ConnectionInfo("v1,v2,v3",null,"/hbase"),
            new ConnectionInfo("v1,v2,v3",123,"/hbase"),
            new ConnectionInfo("v1,v2,v3",123,"/hbase"),
        };
        assertEquals(urls.length,infos.length);
        for (int i = 0; i < urls.length; i++) {
            try {
                ConnectionInfo info = ConnectionInfo.create(urls[i]);
                assertEquals(infos[i], info);
            } catch (AssertionError e) {
                throw new AssertionError("For \"" + urls[i] + "\": " + e.getMessage());
            }
        }
    }
    @Test
    public void testNegativeGetConnectionInfo() throws SQLException {
        String[] urls = new String[] {
            "jdbc:phoenix::",
            "jdbc:phoenix:;",
            "jdbc:phoenix:localhost:abc:/hbase",
            "jdbc:phoenix:localhost:abc:/hbase;foo=bar",
            "jdbc:phoenix:localhost:123:/hbase:blah",
            "jdbc:phoenix:localhost:123:/hbase:blah;foo=bas",
            "jdbc:phoenix:v1:1,v2:2,v3:3",
            "jdbc:phoenix:v1:1,v2:2,v3:3;test=true",
            "jdbc:phoenix:v1,v2,v3:-1:/hbase;test=true",
            "jdbc:phoenix:v1,v2,v3:-1",
            "jdbc:phoenix:v1,v2,v3:123a:/hbase;test=true",
            "jdbc:phoenix:v1,v2,v3:123::/hbase",
            "jdbc:phoenix:v1,v2,v3:123::/hbase;test=false",
        };
        for (int i = 0; i < urls.length; i++) {
            try {
                ConnectionInfo.create(urls[i]);
                throw new AssertionError("Expected exception for \"" + urls[i] + "\"");
            } catch (SQLException e) {
                try {
                    assertEquals(SQLExceptionCode.MALFORMED_CONNECTION_URL.getSQLState(), e.getSQLState());
                } catch (AssertionError ae) {
                    throw new AssertionError("For \"" + urls[i] + "\": " + ae.getMessage());
                }
            }
        }
    }
    
    @Test
    public void testNotAccept() throws Exception {
        Driver driver = new PhoenixDriver();
        assertFalse(driver.acceptsURL("jdbc:phoenix://localhost"));
        assertFalse(driver.acceptsURL("jdbc:phoenix:localhost;test=true;bar=foo"));
        assertFalse(driver.acceptsURL("jdbc:phoenix:localhost;test=true"));
        assertTrue(driver.acceptsURL("jdbc:phoenix:localhost:123"));
        assertTrue(driver.acceptsURL("jdbc:phoenix:localhost:123;untest=true"));
        assertTrue(driver.acceptsURL("jdbc:phoenix:localhost:123;untest=true;foo=bar"));
        DriverManager.deregisterDriver(driver);
    }
}
