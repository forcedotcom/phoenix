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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.schema.TableNotFoundException;
import com.salesforce.phoenix.util.PhoenixRuntime;

/**
 * @author elilevine
 * @since 2.0
 */
public class TenantSpecificTablesDMLTest extends BaseTenantSpecificTablesTest {
    
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
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testSelectOnlySeesTenantData() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('AC/DC', 'abc', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', '" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', 'def', 1, 'Billy Gibbons')");
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
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('AC/DC', 'abc', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', '" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', 'def', 1, 'Billy Gibbons')");
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
            assertEquals(2, rs.getInt(1));
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testDropTenantTableOnlyDeletesTenantData() throws Exception {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('AC/DC', 'abc', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', '" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', 'def', 1, 'Billy Gibbons')");
            
            conn.close();
            
            props = new Properties();
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
            conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
            conn.createStatement().execute("drop table " + TENANT_TABLE_NAME);
            conn.close();
            
            props = new Properties();
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
            conn = DriverManager.getConnection(getUrl(), props);
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + PARENT_TABLE_NAME);
            rs.next();
            assertEquals(2, rs.getInt(1));
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
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('AC/DC', 'aaa', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', '" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', 'def', 2, 'Billy Gibbons')");
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
    public void testUpsertSelectOnlyUpsertsTenantDataWithDifferentTenantTable() throws Exception {
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE ANOTHER_TENANT_TABLE ( " + 
            "tenant_col VARCHAR) BASE_TABLE='PARENT_TABLE', TENANT_TYPE_ID='def'", null, nextTimestamp(), false);
        tenantTableNames.add("ANOTHER_TENANT_TABLE");
        
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('AC/DC', 'aaa', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', '" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', 'def', 2, 'Billy Gibbons')");
            conn.close();
            
            conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            conn.setAutoCommit(true);
            int count = conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + "(id, user) select id+100, user from ANOTHER_TENANT_TABLE where id=2");
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
}
