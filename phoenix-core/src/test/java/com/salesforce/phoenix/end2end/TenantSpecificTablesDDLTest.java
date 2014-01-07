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
import static com.salesforce.phoenix.exception.SQLExceptionCode.BASE_TABLE_NO_TENANT_ID_PK_NO_TENANT_TYPE_ID;
import static com.salesforce.phoenix.exception.SQLExceptionCode.BASE_TABLE_NO_TENANT_ID_PK_WITH_TENANT_TYPE_ID;
import static com.salesforce.phoenix.exception.SQLExceptionCode.CANNOT_DROP_PK;
import static com.salesforce.phoenix.exception.SQLExceptionCode.CANNOT_MUTATE_TABLE;
import static com.salesforce.phoenix.exception.SQLExceptionCode.CREATE_TENANT_TABLE_NO_PK;
import static com.salesforce.phoenix.exception.SQLExceptionCode.TABLE_UNDEFINED;
import static com.salesforce.phoenix.exception.SQLExceptionCode.TENANT_TABLE_PK;
import static com.salesforce.phoenix.exception.SQLExceptionCode.TYPE_ID_USED;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_SCHEMA;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_SEQUENCE;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_TABLE;
import static com.salesforce.phoenix.schema.PTableType.SYSTEM;
import static com.salesforce.phoenix.schema.PTableType.USER;
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
import java.util.Properties;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Test;

import com.salesforce.phoenix.schema.ColumnAlreadyExistsException;
import com.salesforce.phoenix.schema.ColumnNotFoundException;
import com.salesforce.phoenix.schema.PTableType;
import com.salesforce.phoenix.schema.TableAlreadyExistsException;
import com.salesforce.phoenix.schema.TableNotFoundException;
import com.salesforce.phoenix.util.PhoenixRuntime;
import com.salesforce.phoenix.util.SchemaUtil;

public class TenantSpecificTablesDDLTest extends BaseTenantSpecificTablesTest {
    
    @Test
    public void testCreateTenantSpecificTable() throws Exception {
        // ensure we didn't create a physical HBase table for the tenant-specific table
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES).getAdmin();
        assertEquals(0, admin.listTables(TENANT_TABLE_NAME).length);
    }
    
    @Test
    public void testCreateTenantTableTwice() throws Exception {
        try {
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, TENANT_TABLE_DDL, null, nextTimestamp(), false);
        	fail();
        }
        catch (TableAlreadyExistsException expected) {}
    }
    
    @Test
    public void testCreateTenantTableBaseTableTopLevel() throws Exception {
        try {
        	createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE TENANT_TABLE2 (COL VARCHAR) BASE_TABLE='" + TENANT_TABLE_NAME + "',TENANT_TYPE_ID='aaa'", null, nextTimestamp());
        }
        catch (SQLException expected) {
            assertEquals(BASE_TABLE_NOT_TOP_LEVEL.getErrorCode(), expected.getErrorCode());
        }
    }
    
    @Test
    public void testCreateTenantTableWithDifferentTypeId() throws Exception {
        try {
        	createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, TENANT_TABLE_DDL.replace(TENANT_TYPE_ID, "000"), null, nextTimestamp(), false);
            fail();
        }
        catch (TableAlreadyExistsException expected) {}
    }
    
    @Test
    public void testCreateTenantTableWithSameTypeId() throws Exception {
        createTestTable(getUrl(), PARENT_TABLE_DDL.replace(PARENT_TABLE_NAME, PARENT_TABLE_NAME + "_II"), null, nextTimestamp());
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, TENANT_TABLE_DDL, null, nextTimestamp());
        
        // Create a tenant table with tenant type id used with a different base table.  This is allowed.
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE TENANT_SPECIFIC_TABLE_II ( \n" + 
    		    " col VARCHAR\n" + 
    		    " ) BASE_TABLE='" + PARENT_TABLE_NAME + "_II" + "', TENANT_TYPE_ID='" + TENANT_TYPE_ID + "'", null, nextTimestamp());
        
        try {
        	// Create a tenant table with tenant type id already used for this base table.  This is not allowed.
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE TENANT_SPECIFIC_TABLE2 ( \n" + 
        		    " col VARCHAR\n" + 
        		    " ) BASE_TABLE='PARENT_TABLE', TENANT_TYPE_ID='" + TENANT_TYPE_ID + "'", null, nextTimestamp());
        	fail();
        }
        catch (SQLException expected) {
            assertEquals(TYPE_ID_USED.getErrorCode(), expected.getErrorCode());
        }
    }
    
    @Test(expected=TableNotFoundException.class)
    public void testDeletionOfParentTableFailsOnTenantSpecificConnection() throws Exception {
        createTestTable(getUrl(), PARENT_TABLE_DDL.replace(PARENT_TABLE_NAME, "TEMP_PARENT"), null, nextTimestamp());
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, TENANT_ID); // connection is tenant-specific
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("DROP TABLE TEMP_PARENT");
        conn.close();
    }
    
    @Test(expected=SQLException.class)
    public void testCreationOfParentTableFailsOnTenantSpecificConnection() throws Exception {
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE PARENT_TABLE ( \n" + 
                "                user VARCHAR ,\n" + 
                "                id INTEGER not null primary key desc\n" + 
                "                ) ", null, nextTimestamp());
    }
    
    @Test(expected=SQLException.class)
    public void testCreationOfTenantSpecificTableFailsOnNonTenantSpecificConnection() throws Exception {
        createTestTable(getUrl(), "CREATE TABLE TENANT_SPECIFIC_TABLE ( \n" + 
                "                tenantCol VARCHAR \n" + 
                "                ) BASE_TABLE='PARENT_TABLE', TENANT_TYPE_ID='abc'", null, nextTimestamp());
    }
    
    @Test
    public void testTenantSpecificAndParentTablesMustBeInSameSchema() throws SQLException {
        try {
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE DIFFSCHEMA.TENANT_TABLE2 ( \n" + 
                    "                tenant_col VARCHAR) \n" + 
                    "                BASE_TABLE='" + PARENT_TABLE_NAME + "',TENANT_TYPE_ID='aaa'");
            fail();
        }
        catch (SQLException expected) {
            assertEquals(TABLE_UNDEFINED.getErrorCode(), expected.getErrorCode());
        }
    }
    
    @Test
    public void testTenantSpecificTableCannotDeclarePK() throws SQLException {
        try {
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE TENANT_TABLE2 ( \n" + 
                    "                tenant_col VARCHAR PRIMARY KEY) \n" + 
                    "                BASE_TABLE='PARENT_TABLE',TENANT_TYPE_ID='aaa'");
            fail();
        }
        catch (SQLException expected) {
            assertEquals(CREATE_TENANT_TABLE_NO_PK.getErrorCode(), expected.getErrorCode());
        }
    }
    
    @Test(expected=ColumnAlreadyExistsException.class)
    public void testTenantSpecificTableCannotOverrideParentCol() throws SQLException {
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE TENANT_TABLE2 ( \n" + 
                "                user INTEGER) \n" + 
                "                BASE_TABLE='PARENT_TABLE',TENANT_TYPE_ID='aaa'");
    }
    
    @Test
    public void testBaseTableWrongFormatWithTenantTypeId() throws Exception {
        // only two PK columns
        try {
            createTestTable(getUrl(), "CREATE TABLE BASE_TABLE2 (TENANT_ID VARCHAR NOT NULL, TENANT_TYPE_ID VARCHAR(3) NOT NULL, A INTEGER CONSTRAINT PK PRIMARY KEY (TENANT_ID, TENANT_TYPE_ID))");
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE TENANT_TABLE2 (B VARCHAR) BASE_TABLE='BASE_TABLE2',TENANT_TYPE_ID='aaa'");
            fail();
        }
        catch (SQLException expected) {
            assertEquals(BASE_TABLE_NO_TENANT_ID_PK_WITH_TENANT_TYPE_ID.getErrorCode(), expected.getErrorCode());
        }
        
        // nullable tenantTypeId column
        try {
            createTestTable(getUrl(), "CREATE TABLE BASE_TABLE2 (TENANT_ID VARCHAR NOT NULL, TENANT_TYPE_ID VARCHAR(3) NULL, ID VARCHAR, A INTEGER CONSTRAINT PK PRIMARY KEY (TENANT_ID, TENANT_TYPE_ID, ID))");
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE TENANT_TABLE2 (B VARCHAR) BASE_TABLE='BASE_TABLE2',TENANT_TYPE_ID='aaa'");
            fail();
        }
        catch (SQLException expected) {
            assertEquals(BASE_TABLE_NO_TENANT_ID_PK_WITH_TENANT_TYPE_ID.getErrorCode(), expected.getErrorCode());
        }
        
        // tenantTypeId column of wrong type
        try {
            createTestTable(getUrl(), "CREATE TABLE BASE_TABLE2 (TENANT_ID VARCHAR NOT NULL, TENANT_TYPE_ID INTEGER NOT NULL, ID VARCHAR, A INTEGER CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID))");
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE TENANT_TABLE2 (B VARCHAR) BASE_TABLE='BASE_TABLE2',TENANT_TYPE_ID='aaa'");
            fail();
        }
        catch (SQLException expected) {
            assertEquals(BASE_TABLE_NO_TENANT_ID_PK_WITH_TENANT_TYPE_ID.getErrorCode(), expected.getErrorCode());
        }
    }
    
    @Test
    public void testBaseTableWrongFormatWithNoTenantTypeId() throws Exception {
        // only one PK column
        try {
            createTestTable(getUrl(), "CREATE TABLE BASE_TABLE3 (TENANT_ID VARCHAR NOT NULL, A INTEGER CONSTRAINT PK PRIMARY KEY (TENANT_ID))");
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE TENANT_TABLE3 (B VARCHAR) BASE_TABLE='BASE_TABLE3'");
            fail();
        }
        catch (SQLException expected) {
            assertEquals(BASE_TABLE_NO_TENANT_ID_PK_NO_TENANT_TYPE_ID.getErrorCode(), expected.getErrorCode());
        }
        
        // nullable tenantId column
        try {
            createTestTable(getUrl(), "CREATE TABLE BASE_TABLE3 (TENANT_ID VARCHAR NULL, ID VARCHAR, A INTEGER CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID))");
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE TENANT_TABLE3 (B VARCHAR) BASE_TABLE='BASE_TABLE3'");
            fail();
        }
        catch (SQLException expected) {
            assertEquals(BASE_TABLE_NO_TENANT_ID_PK_NO_TENANT_TYPE_ID.getErrorCode(), expected.getErrorCode());
        }
        
        // tenantId column of wrong type
        try {
            createTestTable(getUrl(), "CREATE TABLE BASE_TABLE3 (TENANT_ID INTEGER NOT NULL, ID VARCHAR, A INTEGER CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID))");
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE TENANT_TABLE3 (B VARCHAR) BASE_TABLE='BASE_TABLE3'");
            fail();
        }
        catch (SQLException expected) {
            assertEquals(BASE_TABLE_NO_TENANT_ID_PK_NO_TENANT_TYPE_ID.getErrorCode(), expected.getErrorCode());
        }
    }
    
    @Test
    public void testAddDropColumn() throws Exception {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
        conn.setAutoCommit(true);
        try {
            conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, tenant_col) values (1, 'Viva Las Vegas')");
            
            conn.createStatement().execute("alter table " + TENANT_TABLE_NAME + " add tenant_col2 char(1) null");
            
            conn.close();
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
            conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
            conn.setAutoCommit(true);
            
            conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, tenant_col2) values (2, 'a')");
            conn.close();
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
            conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
            conn.setAutoCommit(true);
            
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME);
            rs.next();
            assertEquals(2, rs.getInt(1));
            
            rs = conn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME + " where tenant_col2 = 'a'");
            rs.next();
            assertEquals(1, rs.getInt(1));
            
            conn.close();
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
            conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
            conn.setAutoCommit(true);
            
            conn.createStatement().execute("alter table " + TENANT_TABLE_NAME + " drop column tenant_col");
            
            conn.close();
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
            conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
            conn.setAutoCommit(true);
            
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
            assertTableMetaData(rs, TYPE_SCHEMA, TYPE_SEQUENCE, SYSTEM);
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
            
            // make sure tenants see parent table's columns and their own
            rs = meta.getColumns(null, null, null, null);
            assertTrue(rs.next());
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME, "user");
            assertTrue(rs.next());
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME, "tenant_id");
            assertTrue(rs.next());
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME, "tenant_type_id");
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
