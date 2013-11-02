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

import static com.salesforce.phoenix.exception.SQLExceptionCode.BASE_TABLE_NOT_TOP_LEVEL;
import static com.salesforce.phoenix.exception.SQLExceptionCode.BASE_TABLE_NO_TENANT_ID_PK;
import static com.salesforce.phoenix.exception.SQLExceptionCode.CANNOT_DROP_PK;
import static com.salesforce.phoenix.exception.SQLExceptionCode.CANNOT_MUTATE_TABLE;
import static com.salesforce.phoenix.exception.SQLExceptionCode.CREATE_TENANT_TABLE_NO_PK;
import static com.salesforce.phoenix.exception.SQLExceptionCode.TABLE_UNDEFINED;
import static com.salesforce.phoenix.exception.SQLExceptionCode.TENANT_TABLE_PK;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_SCHEMA;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_TABLE;
import static com.salesforce.phoenix.schema.PTableType.SYSTEM;
import static com.salesforce.phoenix.schema.PTableType.USER;
import static com.salesforce.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.salesforce.phoenix.schema.ColumnAlreadyExistsException;
import com.salesforce.phoenix.schema.ColumnNotFoundException;
import com.salesforce.phoenix.schema.PTableType;
import com.salesforce.phoenix.schema.TableNotFoundException;
import com.salesforce.phoenix.util.SchemaUtil;

/**
 * @author elilevine
 * @since 2.0
 */
public class TenantSpecificTablesTest extends BaseClientMangedTimeTest {
    
    private static final String TENANT_ID = "ZZTop";
    private static final String PHOENIX_JDBC_TENANT_SPECIFIC_URL = getUrl() + ';' + TENANT_ID_ATTRIB + '=' + TENANT_ID;
    
    private static final String PARENT_TABLE_NAME = "PARENT_TABLE";
    private static final String PARENT_TABLE_DDL = "CREATE TABLE " + PARENT_TABLE_NAME + " ( \n" + 
            "                user VARCHAR ,\n" + 
            "                tenant_id VARCHAR(5) NOT NULL,\n" + 
            "                id INTEGER NOT NULL \n" + 
            "                CONSTRAINT pk PRIMARY KEY (tenant_id, id))";
    
    private static final String TENANT_TABLE_NAME = "TENANT_TABLE";
    private static final String TENANT_TABLE_DDL = "CREATE TABLE " + TENANT_TABLE_NAME + " ( \n" + 
            "                tenant_col VARCHAR)\n" + 
            "                BASE_TABLE='PARENT_TABLE'";
    
    @Before
    public void createTables() throws SQLException {
        createTestTable(getUrl(), PARENT_TABLE_DDL);
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, TENANT_TABLE_DDL);
    }
    
    @After
    public void dropTables() throws SQLException {
        dropTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, TENANT_TABLE_NAME);
        dropTable(getUrl(), PARENT_TABLE_NAME);
    }
    
    private void dropTable(String url, String tableName) throws SQLException {
        Connection conn = DriverManager.getConnection(url);
        try {
            conn.createStatement().execute("DROP TABLE IF EXISTS " + tableName);
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testBasicUpsertSelect() throws Exception {
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
        try {
            conn.setAutoCommit(false);
            conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, tenant_col) values (1, 'Cheap Sunglasses')");
            conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, tenant_col) values (2, 'Viva Las Vegas')");
            conn.commit();
            
            ResultSet rs = conn.createStatement().executeQuery("select tenant_col from " + TENANT_TABLE_NAME + " where id = 1");
            assertTrue("Expected 1 row in result set", rs.next());
            assertEquals("Cheap Sunglasses", rs.getString(1));
            assertFalse("Expected 1 row in result set", rs.next());
            
            // ensure we didn't create a physical HBase table for the tenant-specific table
            HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES).getAdmin();
            assertEquals(0, admin.listTables(TENANT_TABLE_NAME).length);
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testAddDropColumn() throws Exception {
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
        conn.setAutoCommit(true);
        try {
            conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, tenant_col) values (1, 'Viva Las Vegas')");
            
            conn.createStatement().execute("alter table " + TENANT_TABLE_NAME + " add tenant_col2 char(1) null");
            conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, tenant_col2) values (2, 'a')");
            
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME);
            rs.next();
            assertEquals(2, rs.getInt(1));
            
            rs = conn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME + " where tenant_col2 = 'a'");
            rs.next();
            assertEquals(1, rs.getInt(1));
            
            conn.createStatement().execute("alter table " + TENANT_TABLE_NAME + " drop column tenant_col");
            
            rs = conn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME + "");
            rs.next();
            assertEquals(2, rs.getInt(1));
            
            try {
                rs = conn.createStatement().executeQuery("select tenant_col from TENANT_TABLE");
                fail();
            }
            catch (ColumnNotFoundException expected) {}
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testMutationOfPKInTenantTablesNotAllowed() throws Exception {
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
        try {
            try {
                conn.createStatement().execute("alter table " + TENANT_TABLE_NAME + " add new_tenant_pk char(1) primary key");
                fail();
            }
            catch (SQLException expected) {
                assertEquals(TENANT_TABLE_PK.getErrorCode(), expected.getErrorCode());
            }
            
            try {
                conn.createStatement().execute("alter table " + TENANT_TABLE_NAME + " drop column id");
                fail();
            }
            catch (SQLException expected) {
                assertEquals(CANNOT_DROP_PK.getErrorCode(), expected.getErrorCode());
            }
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testColumnMutationInParentTableWithExistingTenantTable() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            // try adding a PK col
            try {
                conn.createStatement().execute("alter table " + PARENT_TABLE_NAME + " add new_pk varchar primary key");
                fail();
            }
            catch (SQLException expected) {
                assertEquals(CANNOT_MUTATE_TABLE.getErrorCode(), expected.getErrorCode());
            }
            
            // try adding a non-PK col
            try {
                conn.createStatement().execute("alter table " + PARENT_TABLE_NAME + " add new_col char(1)");
                fail();
            }
            catch (SQLException expected) {
                assertEquals(CANNOT_MUTATE_TABLE.getErrorCode(), expected.getErrorCode());
            }
            
            // try removing a PK col
            try {
                conn.createStatement().execute("alter table " + PARENT_TABLE_NAME + " drop column id");
                fail();
            }
            catch (SQLException expected) {
                assertEquals(CANNOT_DROP_PK.getErrorCode(), expected.getErrorCode());
            }
            
            // try removing a non-PK col
            try {
                conn.createStatement().execute("alter table " + PARENT_TABLE_NAME + " drop column user");
                fail();
            }
            catch (SQLException expected) {
                assertEquals(CANNOT_MUTATE_TABLE.getErrorCode(), expected.getErrorCode());
            }
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testDropParentTableWithExistingTenantTable() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().executeUpdate("drop table " + PARENT_TABLE_NAME);
            fail("Should not have been allowed to drop a parent table to which tenant-specific tables still point.");
        }
        catch (SQLException expected) {
            assertEquals(CANNOT_MUTATE_TABLE.getErrorCode(), expected.getErrorCode());
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testTableMetadataScan() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {   
            // make sure connections w/o tenant id only see non-tenant-specific tables, both SYSTEM and USER
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet rs = meta.getTables(null, null, null, null);
            assertTrue(rs.next());
            assertTableMetaData(rs, TYPE_SCHEMA, TYPE_TABLE, SYSTEM);
            assertTrue(rs.next());
            assertTableMetaData(rs, null, PARENT_TABLE_NAME, USER);
            assertFalse(rs.next());
            
            // make sure connections w/o tenant id only see non-tenant-specific columns
            rs = meta.getColumns(null, null, null, null);
            while (rs.next()) {
                assertNotEquals(TENANT_TABLE_NAME, rs.getString("TABLE_NAME"));
            }
        }
        finally {
            conn.close();
        }
        
        conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
        try {   
            // make sure tenant-specific connections only see their own tables
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet rs = meta.getTables(null, null, null, null);
            assertTrue(rs.next());
            assertTableMetaData(rs, null, TENANT_TABLE_NAME, USER);
            assertFalse(rs.next());
            
            // make sure tenants see paren table's columns and their own
            rs = meta.getColumns(null, null, null, null);
            assertTrue(rs.next());
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME, "user");
            assertTrue(rs.next());
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME, "tenant_id");
            assertTrue(rs.next());
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME, "id");
            assertTrue(rs.next());
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME, "tenant_col");
            assertFalse(rs.next()); 
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testTenantSpecificAndParentTablesMustBeInSameSchema() throws SQLException {
        try {
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE DIFFSCHEMA.TENANT_TABLE2 ( \n" + 
                    "                tenant_col VARCHAR) \n" + 
                    "                BASE_TABLE='" + PARENT_TABLE_NAME + '\'');
            fail();
        }
        catch (SQLException expected) {
            assertEquals(TABLE_UNDEFINED.getErrorCode(), expected.getErrorCode());
        }
        finally {
            dropTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "DIFFSCHEMA.TENANT_TABLE2");
        }
    }
    
    @Test
    public void testTenantSpecificTableCannotDeclarePK() throws SQLException {
        try {
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE TENANT_TABLE2 ( \n" + 
                    "                tenant_col VARCHAR PRIMARY KEY) \n" + 
                    "                BASE_TABLE='PARENT_TABLE'");
            fail();
        }
        catch (SQLException expected) {
            assertEquals(CREATE_TENANT_TABLE_NO_PK.getErrorCode(), expected.getErrorCode());
        }
        finally {
            dropTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "TENANT_TABLE2");
        }
    }
    
    @Test(expected=ColumnAlreadyExistsException.class)
    public void testTenantSpecificTableCannotOverrideParentCol() throws SQLException {
        try {
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE TENANT_TABLE2 ( \n" + 
                    "                user INTEGER) \n" + 
                    "                BASE_TABLE='PARENT_TABLE'");
        }
        finally {
            dropTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "TENANT_TABLE2");
        }
    }
    
    @Test
    public void testSelectOnlySeesTenantData() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, id, user) values ('AC/DC', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, id, user) values ('" + TENANT_ID + "', 1, 'Billy Gibbons')");
            conn.close();
            
            conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            ResultSet rs = conn.createStatement().executeQuery("select user from " + TENANT_TABLE_NAME);
            assertTrue("Expected 1 row in result set", rs.next());
            assertEquals("Billy Gibbons", rs.getString(1));
            assertFalse("Expected 1 row in result set", rs.next());
            
            rs = conn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME);
            assertTrue("Expected 1 row in result set", rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse("Expected 1 row in result set", rs.next());
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testDeleteOnlyDeletesTenantData() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, id, user) values ('AC/DC', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, id, user) values ('" + TENANT_ID + "', 1, 'Billy Gibbons')");
            conn.close();
            
            conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            conn.setAutoCommit(true);
            int count = conn.createStatement().executeUpdate("delete from " + TENANT_TABLE_NAME);
            assertEquals("Expected 1 row have been deleted", 1, count);
            
            ResultSet rs = conn.createStatement().executeQuery("select * from " + TENANT_TABLE_NAME);
            assertFalse("Expected no rows in result set", rs.next());
            conn.close();
            
            conn = DriverManager.getConnection(getUrl());
            rs = conn.createStatement().executeQuery("select count(*) from " + PARENT_TABLE_NAME);
            rs.next();
            assertEquals(1, rs.getInt(1));
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testDropTenantTableOnlyDeletesTenantData() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, id, user) values ('AC/DC', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, id, user) values ('" + TENANT_ID + "', 1, 'Billy Gibbons')");
            conn.close();
            
            conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            conn.setAutoCommit(true);
            conn.createStatement().execute("drop table " + TENANT_TABLE_NAME);
            conn.close();
            
            conn = DriverManager.getConnection(getUrl());
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + PARENT_TABLE_NAME);
            rs.next();
            assertEquals(1, rs.getInt(1));
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testUpsertSelectOnlyUpsertsTenantData() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, id, user) values ('AC/DC', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, id, user) values ('" + TENANT_ID + "', 1, 'Billy Gibbons')");
            conn.close();
            
            conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            conn.setAutoCommit(true);
            int count = conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + "(id, user) select id+100, user from " + TENANT_TABLE_NAME);
            assertEquals("Expected 1 row to have been inserted", 1, count);
            
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME);
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testUpsertValuesOnlyUpsertsTenantData() throws Exception {
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
        try {
            conn.setAutoCommit(true);
            int count = conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, user) values (1, 'Bon Scott')");
            assertEquals("Expected 1 row to have been inserted", 1, count);
            
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME);
            rs.next();
            assertEquals(1, rs.getInt(1));
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testCreateTenantTableBaseTableTopLevel() throws Exception {
        try {
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE TENANT_TABLE2 (COL VARCHAR) BASE_TABLE='TENANT_TABLE'");
            fail();
        }
        catch (SQLException expected) {
            assertEquals(BASE_TABLE_NOT_TOP_LEVEL.getErrorCode(), expected.getErrorCode());
        }
    }
    
    @Test
    public void testBaseTableCannotBeUsedInStatementsInMultitenantConnections() throws Exception {
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
        try {
            try {
                conn.createStatement().execute("select * from " + PARENT_TABLE_NAME);
                fail();
            }
            catch (TableNotFoundException expected) {};   
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testTenantTableCannotBeUsedInStatementsInNonMultitenantConnections() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            try {
                conn.createStatement().execute("select * from " + TENANT_TABLE_NAME);
                fail();
            }
            catch (TableNotFoundException expected) {};   
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testBaseTableWrongFormat() throws Exception {
        // only one PK column
        try {
            createTestTable(getUrl(), "CREATE TABLE BASE_TABLE2 (TENANT_ID VARCHAR NOT NULL, A INTEGER CONSTRAINT PK PRIMARY KEY (TENANT_ID))");
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE TENANT_TABLE2 (B VARCHAR) BASE_TABLE='BASE_TABLE2'");
            fail();
        }
        catch (SQLException expected) {
            assertEquals(BASE_TABLE_NO_TENANT_ID_PK.getErrorCode(), expected.getErrorCode());
        }
        
        // nullable tenantId column
        try {
            createTestTable(getUrl(), "CREATE TABLE BASE_TABLE2 (TENANT_ID VARCHAR NULL, ID VARCHAR, A INTEGER CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID))");
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE TENANT_TABLE2 (B VARCHAR) BASE_TABLE='BASE_TABLE2'");
            fail();
        }
        catch (SQLException expected) {
            assertEquals(BASE_TABLE_NO_TENANT_ID_PK.getErrorCode(), expected.getErrorCode());
        }
        
        // tenantId column of wrong type
        try {
            createTestTable(getUrl(), "CREATE TABLE BASE_TABLE2 (TENANT_ID INTEGER NOT NULL, ID VARCHAR, A INTEGER CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID))");
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE TENANT_TABLE2 (B VARCHAR) BASE_TABLE='BASE_TABLE2'");
            fail();
        }
        catch (SQLException expected) {
            assertEquals(BASE_TABLE_NO_TENANT_ID_PK.getErrorCode(), expected.getErrorCode());
        }
    }
    
    private void assertTableMetaData(ResultSet rs, String schema, String table, PTableType tableType) throws SQLException {
        assertEquals(schema, rs.getString("TABLE_SCHEM"));
        assertEquals(table, rs.getString("TABLE_NAME"));
        assertEquals(tableType.toString(), rs.getString("TABLE_TYPE"));
    }
    
    private void assertColumnMetaData(ResultSet rs, String schema, String table, String column) throws SQLException {
        assertEquals(schema, rs.getString("TABLE_SCHEM"));
        assertEquals(table, rs.getString("TABLE_NAME"));
        assertEquals(SchemaUtil.normalizeIdentifier(column), rs.getString("COLUMN_NAME"));
    }
}
