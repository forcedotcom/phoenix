package com.salesforce.phoenix.end2end.salted;

import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.end2end.BaseHBaseManagedTimeTest;


public class SaltedTableUpsertSelectTest extends BaseHBaseManagedTimeTest {

    @Test
    public void testUpsertIntoSaltedTableFromNormalTable() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE IF NOT EXISTS source" + 
                    " (pk VARCHAR NOT NULL PRIMARY KEY, col INTEGER)";
            createTestTable(getUrl(), ddl);
            ddl = "CREATE TABLE IF NOT EXISTS target" + 
                    " (pk VARCHAR NOT NULL PRIMARY KEY, col INTEGER) SALT_BUCKETS=4";
            createTestTable(getUrl(), ddl);
            
            String query = "UPSERT INTO source(pk, col) VALUES(?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "1");
            stmt.setInt(2, 1);
            stmt.execute();
            conn.commit();
            
            query = "UPSERT INTO target(pk, col) SELECT pk, col from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            
            query = "SELECT * FROM target";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1));
            assertEquals(1, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpsertIntoNormalTableFromSaltedTable() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE IF NOT EXISTS source" + 
                    " (pk VARCHAR NOT NULL PRIMARY KEY, col INTEGER) SALT_BUCKETS=4";
            createTestTable(getUrl(), ddl);
            ddl = "CREATE TABLE IF NOT EXISTS target" + 
                    " (pk VARCHAR NOT NULL PRIMARY KEY, col INTEGER)";
            createTestTable(getUrl(), ddl);
            
            String query = "UPSERT INTO source(pk, col) VALUES(?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "1");
            stmt.setInt(2, 1);
            stmt.execute();
            conn.commit();
            
            query = "UPSERT INTO target(pk, col) SELECT pk, col from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            
            query = "SELECT * FROM target";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1));
            assertEquals(1, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpsertSaltedTableIntoSaltedTable() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE IF NOT EXISTS source" + 
                    " (pk VARCHAR NOT NULL PRIMARY KEY, col INTEGER) SALT_BUCKETS=4";
            createTestTable(getUrl(), ddl);
            ddl = "CREATE TABLE IF NOT EXISTS target" + 
                    " (pk VARCHAR NOT NULL PRIMARY KEY, col INTEGER) SALT_BUCKETS=4";
            createTestTable(getUrl(), ddl);
            
            String query = "UPSERT INTO source(pk, col) VALUES(?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "1");
            stmt.setInt(2, 1);
            stmt.execute();
            conn.commit();
            
            query = "UPSERT INTO target(pk, col) SELECT pk, col from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            
            query = "SELECT * FROM target";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1));
            assertEquals(1, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpsertSelectOnSameSaltedTable() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE IF NOT EXISTS source" + 
                    " (pk VARCHAR NOT NULL PRIMARY KEY, col1 INTEGER, col2 INTEGER) SALT_BUCKETS=4";
            createTestTable(getUrl(), ddl);
            
            String query = "UPSERT INTO source(pk, col1) VALUES(?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "1");
            stmt.setInt(2, 1);
            stmt.execute();
            conn.commit();
            
            query = "UPSERT INTO source(pk, col2) SELECT pk, col1 from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            
            query = "SELECT col2 FROM source";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpsertSelectOnSameSaltedTableWithEmptyPKColumn() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE IF NOT EXISTS source" + 
                    " (pk1 varchar NULL, pk2 varchar NULL, pk3 integer NOT NULL, col1 INTEGER" + 
                    " CONSTRAINT pk PRIMARY KEY (pk1, pk2, pk3)) SALT_BUCKETS=4";
            createTestTable(getUrl(), ddl);
            
            String query = "UPSERT INTO source(pk1, pk2, pk3, col1) VALUES(?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "1");
            stmt.setString(2, "2");
            stmt.setInt(3, 1);
            stmt.setInt(4, 1);
            stmt.execute();
            conn.commit();
            
            conn.setAutoCommit(true);
            query = "UPSERT INTO source(pk3, col1, pk1) SELECT pk3+1, col1+1, pk2 from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            
            query = "SELECT col1 FROM source";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
