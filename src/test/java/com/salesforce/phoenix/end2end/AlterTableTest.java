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

import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.util.SchemaUtil;


public class AlterTableTest extends BaseHBaseManagedTimeTest {
    public static final String SCHEMA_NAME = "";
    public static final String DATA_TABLE_NAME = "T";
    public static final String INDEX_TABLE_NAME = "I";
    public static final String DATA_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "T");
    public static final String INDEX_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "I");


    @Test
    public void testAlterTableWithVarBinaryKey() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        
        try {
            String ddl = "CREATE TABLE test_table " +
                    "  (a_string varchar not null, a_binary varbinary not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string, a_binary))\n";
            createTestTable(getUrl(), ddl);
            
            ddl = "ALTER TABLE test_table ADD b_string VARCHAR NULL PRIMARY KEY";
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            fail("Should have caught bad alter.");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1022 (42J04): Cannot add column to table when the last PK column is of type VARBINARY."));
        } finally {
            conn.close();
        }
    }


    @Test
    public void testAddVarCharColToPK() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        
        try {
            String ddl = "CREATE TABLE test_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            conn.createStatement().execute(ddl);
            
            String dml = "UPSERT INTO test_table VALUES(?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.execute();
            stmt.setString(1, "a");
            stmt.execute();
            conn.commit();
            
            String query = "SELECT * FROM test_table";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertFalse(rs.next());
            
            ddl = "ALTER TABLE test_table ADD b_string VARCHAR NULL PRIMARY KEY";
            conn.createStatement().execute(ddl);
            
            query = "SELECT * FROM test_table WHERE a_string = 'a' AND b_string IS NULL";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertFalse(rs.next());
            
            dml = "UPSERT INTO test_table VALUES(?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "c");
            stmt.execute();
            conn.commit();
           
            query = "SELECT * FROM test_table WHERE a_string = 'c' AND b_string IS NULL";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1));
            assertFalse(rs.next());
            
            dml = "UPSERT INTO test_table(a_string,col1) VALUES(?,?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 5);
            stmt.execute();
            conn.commit();
           
            query = "SELECT a_string,col1 FROM test_table WHERE a_string = 'a' AND b_string IS NULL";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(5,rs.getInt(2)); // TODO: figure out why this flaps
            assertFalse(rs.next());
            
        } finally {
            conn.close();
        }
    }
    
    private static void assertIndexExists(Connection conn, boolean exists) throws SQLException {
        ResultSet rs = conn.getMetaData().getIndexInfo(null, SCHEMA_NAME, DATA_TABLE_NAME, false, false);
        assertEquals(exists, rs.next());
    }
    
    @Test
    public void testDropIndexedColumn() throws Exception {
        String query;
        ResultSet rs;
        PreparedStatement stmt;
    
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
    
        // make sure that the tables are empty, but reachable
        conn.createStatement().execute(
          "CREATE TABLE " + DATA_TABLE_FULL_NAME
              + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
    
        conn.createStatement().execute(
          "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1, v2)");
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
    
        // load some data into the table
        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        conn.commit();
        
        assertIndexExists(conn,true);
        conn.createStatement().execute("ALTER TABLE " + DATA_TABLE_FULL_NAME + " DROP COLUMN v1");
        assertIndexExists(conn,false);
        
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("1",rs.getString(2));
        assertFalse(rs.next());
        
        // load some data into the table
        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "2");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("2",rs.getString(2));
        assertFalse(rs.next());
    }
    
    @Test
    public void testDropCoveredColumn() throws Exception {
        String query;
        ResultSet rs;
        PreparedStatement stmt;
    
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
    
        // make sure that the tables are empty, but reachable
        conn.createStatement().execute(
          "CREATE TABLE " + DATA_TABLE_FULL_NAME
              + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR, v3 VARCHAR)");
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
    
        conn.createStatement().execute(
          "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1) include (v2, v3)");
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
    
        // load some data into the table
        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.setString(4, "j");
        stmt.execute();
        conn.commit();
        
        assertIndexExists(conn,true);
        conn.createStatement().execute("ALTER TABLE " + DATA_TABLE_FULL_NAME + " DROP COLUMN v2");
        // TODO: verify meta data that we get back to confirm our column was dropped
        assertIndexExists(conn,true);
        
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("x",rs.getString(2));
        assertEquals("j",rs.getString(3));
        assertFalse(rs.next());
        
        // load some data into the table
        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "y");
        stmt.setString(3, "k");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("y",rs.getString(2));
        assertEquals("k",rs.getString(3));
        assertFalse(rs.next());
    }
    
    @Test
    public void testAddPKColumnToTableWithIndex() throws Exception {
        String query;
        ResultSet rs;
        PreparedStatement stmt;
    
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
    
        // make sure that the tables are empty, but reachable
        conn.createStatement().execute(
          "CREATE TABLE " + DATA_TABLE_FULL_NAME
              + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
    
        conn.createStatement().execute(
          "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1) include (v2)");
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
    
        // load some data into the table
        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        conn.commit();
        
        assertIndexExists(conn,true);
        conn.createStatement().execute("ALTER TABLE " + DATA_TABLE_FULL_NAME + " ADD k2 DECIMAL PRIMARY KEY");
        // TODO: verify metadata of index
        assertIndexExists(conn,true);
        
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("x",rs.getString(2));
        assertEquals("1",rs.getString(3));
        assertNull(rs.getBigDecimal(4));
        assertFalse(rs.next());
        
        // load some data into the table
        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + "(K,K2,V1,V2) VALUES(?,?,?,?)");
        stmt.setString(1, "b");
        stmt.setBigDecimal(2, BigDecimal.valueOf(2));
        stmt.setString(3, "y");
        stmt.setString(4, "2");
        stmt.execute();
        conn.commit();
        
        query = "SELECT k,k2 FROM " + DATA_TABLE_FULL_NAME + " WHERE v1='y'";
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertEquals(BigDecimal.valueOf(2),rs.getBigDecimal(2));
        assertFalse(rs.next());
    }
    
    @Test
    public void testSetSaltedTableAsImmutable() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        
        try {
            String ddl = "CREATE TABLE MESSAGES (\n" + 
            		"        SENDER_ID UNSIGNED_LONG NOT NULL,\n" + 
            		"        RECIPIENT_ID UNSIGNED_LONG NOT NULL,\n" + 
            		"        M_TIMESTAMP DATE  NOT NULL,\n" + 
            		"        ROW_ID UNSIGNED_LONG NOT NULL,\n" + 
            		"        IS_READ TINYINT,\n" + 
            		"        IS_DELETED TINYINT,\n" + 
            		"        VISIBILITY TINYINT,\n" + 
            		"        B.SENDER_IP VARCHAR,\n" + 
            		"        B.JSON VARCHAR,\n" + 
            		"        B.M_TEXT VARCHAR\n" + 
            		"        CONSTRAINT ROWKEY PRIMARY KEY\n" + 
            		"(SENDER_ID,RECIPIENT_ID,M_TIMESTAMP DESC,ROW_ID))\n" + 
            		"SALT_BUCKETS=4";
            conn.createStatement().execute(ddl);
            
            ddl = "ALTER TABLE MESSAGES SET IMMUTABLE_ROWS=true";
            conn.createStatement().execute(ddl);
            
            conn.createStatement().executeQuery("select count(*) from messages").next();
            
        } finally {
            conn.close();
        }
    }
    
    
    @Test
    public void testDropColumnFromSaltedTable() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        
        try {
            String ddl = "CREATE TABLE MESSAGES (\n" + 
                    "        SENDER_ID UNSIGNED_LONG NOT NULL,\n" + 
                    "        RECIPIENT_ID UNSIGNED_LONG NOT NULL,\n" + 
                    "        M_TIMESTAMP DATE  NOT NULL,\n" + 
                    "        ROW_ID UNSIGNED_LONG NOT NULL,\n" + 
                    "        IS_READ TINYINT,\n" + 
                    "        IS_DELETED TINYINT,\n" + 
                    "        VISIBILITY TINYINT,\n" + 
                    "        B.SENDER_IP VARCHAR,\n" + 
                    "        B.JSON VARCHAR,\n" + 
                    "        B.M_TEXT VARCHAR\n" + 
                    "        CONSTRAINT ROWKEY PRIMARY KEY\n" + 
                    "(SENDER_ID,RECIPIENT_ID,M_TIMESTAMP DESC,ROW_ID))\n" + 
                    "SALT_BUCKETS=4";
            conn.createStatement().execute(ddl);
            
            ddl = "ALTER TABLE MESSAGES DROP COLUMN B.JSON";
            conn.createStatement().execute(ddl);
            
            conn.createStatement().executeQuery("select count(*) from messages").next();
        } finally {
            conn.close();
        }
    }
}
