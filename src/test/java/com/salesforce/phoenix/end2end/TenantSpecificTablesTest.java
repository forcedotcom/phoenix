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

import static com.salesforce.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Before;
import org.junit.Test;

/**
 * @author elilevine
 * @since 2.0
 */
public class TenantSpecificTablesTest extends BaseClientMangedTimeTest {
    
    private static final String TENANT_ID = "ZZTop";
    private static final String PHOENIX_JDBC_TENANT_SPECIFIC_URL = getUrl() + ';' + TENANT_ID_ATTRIB + '=' + TENANT_ID;
    
    private static final String PARENT_TABLE_DDL = "CREATE TABLE PARENT_TABLE ( \n" + 
            "                user VARCHAR ,\n" + 
            "                tenant_id VARCHAR(5) NOT NULL,\n" + 
            "                id INTEGER NOT NULL \n" + 
            "                CONSTRAINT pk PRIMARY KEY (tenant_id, id))";
    
    private static final String TENANT_TABLE_DDL = "CREATE TABLE TENANT_TABLE ( \n" + 
            "                tenant_col VARCHAR ,\n" + 
            "                tenant_id VARCHAR(5) NOT NULL,\n" + 
            "                id INTEGER NOT NULL \n" + 
            "                CONSTRAINT pk PRIMARY KEY (tenant_id, id)) \n" +
            "                BASE_TABLE='PARENT_TABLE'";
    
    private Properties props = new Properties(TEST_PROPERTIES);

    @Before
    public void initProperties() {
        props.clear();
    }
    
    @Test
    public void testBasicUpsertSelect() throws Exception {
        createTestTable(getUrl(), PARENT_TABLE_DDL);
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, TENANT_TABLE_DDL);
        
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
        try {
            conn.setAutoCommit(false);
            conn.createStatement().executeUpdate("upsert into TENANT_TABLE (tenant_id, id, tenant_col) values ('" + TENANT_ID + "', 1, 'Cheap Sunglasses')");
            conn.createStatement().executeUpdate("upsert into TENANT_TABLE (tenant_id, id, tenant_col) values ('" + TENANT_ID + "', 2, 'Viva Las Vegas')");
            conn.commit();
            
            ResultSet rs = conn.createStatement().executeQuery("select tenant_col from TENANT_TABLE where id = 1");
            assertTrue("Expected 1 row in result set", rs.next());
            assertEquals("Cheap Sunglasses", rs.getString(1));
            assertFalse("Expected 1 row in result set", rs.next());
            
            // ensure we didn't create a physical HBase table for the tenannt-specifidc table
            HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES).getAdmin();
            assertEquals(0, admin.listTables("TENANT_TABLE").length);
        }
        finally {
            conn.close();
        }
    }
}
