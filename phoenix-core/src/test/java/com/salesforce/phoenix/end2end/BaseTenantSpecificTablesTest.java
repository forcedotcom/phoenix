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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;

import com.salesforce.phoenix.util.PhoenixRuntime;

/**
 * Describe your class here.
 *
 * @author elilevine
 * @since 2.2
 */
public abstract class BaseTenantSpecificTablesTest extends BaseClientMangedTimeTest {
    protected static final String TENANT_ID = "ZZTop";
    protected static final String TENANT_TYPE_ID = "abc";
    protected static final String PHOENIX_JDBC_TENANT_SPECIFIC_URL = getUrl() + ';' + TENANT_ID_ATTRIB + '=' + TENANT_ID;
    
    protected static final String PARENT_TABLE_NAME = "PARENT_TABLE";
    protected static final String PARENT_TABLE_DDL = "CREATE TABLE " + PARENT_TABLE_NAME + " ( \n" + 
            "                user VARCHAR ,\n" + 
            "                tenant_id VARCHAR(5) NOT NULL,\n" + 
            "                tenant_type_id VARCHAR(3) NOT NULL, \n" + 
            "                id INTEGER NOT NULL\n" + 
            "                CONSTRAINT pk PRIMARY KEY (tenant_id, tenant_type_id, id))";
    
    protected static final String TENANT_TABLE_NAME = "TENANT_TABLE";
    protected static final String TENANT_TABLE_DDL = "CREATE TABLE " + TENANT_TABLE_NAME + " ( \n" + 
            "                tenant_col VARCHAR)\n" + 
            "                BASE_TABLE='PARENT_TABLE', TENANT_TYPE_ID='" + TENANT_TYPE_ID + '\'';
    
    protected List<String> tenantTableNames = new ArrayList<String>();
    
    @Before
    public void createTables() throws SQLException {
        tenantTableNames.clear();
        createTestTable(getUrl(), PARENT_TABLE_DDL, null, nextTimestamp());
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, TENANT_TABLE_DDL, null, nextTimestamp());
        tenantTableNames.add(TENANT_TABLE_NAME);
    }
    
    /**
     * {@link BaseClientMangedTimeTest} automatically drops all tables.  This method is here because
     * tenant-specific tables need to be dropped first.
     * @throws SQLException
     */
    @After
    public void dropTenantTables() throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        props.setProperty(TENANT_ID_ATTRIB, TENANT_ID);
        for (String tenantTableName : tenantTableNames) {
            Connection conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("DROP TABLE IF EXISTS " + tenantTableName);
            conn.close();
        }
    }
}
