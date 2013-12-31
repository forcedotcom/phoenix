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
import static com.salesforce.phoenix.exception.SQLExceptionCode.TYPE_ID_USED;
import static com.salesforce.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.salesforce.phoenix.jdbc.PhoenixStatement;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.schema.TableAlreadyExistsException;
import com.salesforce.phoenix.schema.TableNotFoundException;
import com.salesforce.phoenix.util.PhoenixRuntime;

public class CreateTableTest  extends BaseClientMangedTimeTest {
    
    private static final String TENANT_ID = "abc";
    private static final String TENANT_TYPE_ID = "111";
    private static final String TENANT_TABLE_NAME = "TENANT_SPECIFIC_TABLE";
    private static final String BASE_TABLE_DDL = "CREATE TABLE PARENT_TABLE ( \n" + 
            "                user VARCHAR ,\n" +
            "                tenant_id VARCHAR not null,\n" +
            "                tenant_type_id VARCHAR(3) not null,\n" +
            "                id INTEGER not null\n" + 
            "                constraint pk primary key (tenant_id, tenant_type_id, id)) ";
    private static final String TENANT_TABLE_DDL = "CREATE TABLE " + TENANT_TABLE_NAME + " ( \n" + 
            "                tenantCol VARCHAR\n" + 
            "                ) BASE_TABLE='PARENT_TABLE', TENANT_TYPE_ID='" + TENANT_TYPE_ID + "'";

    private List<String> tenantTableNames = new ArrayList<String>();
    
    @Before
    public void clearTenantTableNames() {
    	tenantTableNames.clear();
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
    
    @Test
    public void testStartKeyStopKey() throws SQLException {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE start_stop_test (pk char(2) not null primary key) SPLIT ON ('EA','EZ')");
        conn.close();
        
        String query = "select count(*) from start_stop_test where pk >= 'EA' and pk < 'EZ'";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        conn = DriverManager.getConnection(getUrl(), props);
        Statement statement = conn.createStatement();
        statement.execute(query);
        PhoenixStatement pstatement = statement.unwrap(PhoenixStatement.class);
        List<KeyRange>splits = pstatement.getQueryPlan().getSplits();
        assertTrue(splits.size() > 0);
    }
    
    @Test
    public void testCreateTable() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE m_interface_job(                data.addtime VARCHAR ,\n" + 
        		"                data.dir VARCHAR ,\n" + 
        		"                data.end_time VARCHAR ,\n" + 
        		"                data.file VARCHAR ,\n" + 
        		"                data.fk_log VARCHAR ,\n" + 
        		"                data.host VARCHAR ,\n" + 
        		"                data.row VARCHAR ,\n" + 
        		"                data.size VARCHAR ,\n" + 
        		"                data.start_time VARCHAR ,\n" + 
        		"                data.stat_date DATE ,\n" + 
        		"                data.stat_hour VARCHAR ,\n" + 
        		"                data.stat_minute VARCHAR ,\n" + 
        		"                data.state VARCHAR ,\n" + 
        		"                data.title VARCHAR ,\n" + 
        		"                data.user VARCHAR ,\n" + 
        		"                data.inrow VARCHAR ,\n" + 
        		"                data.jobid VARCHAR ,\n" + 
        		"                data.jobtype VARCHAR ,\n" + 
        		"                data.level VARCHAR ,\n" + 
        		"                data.msg VARCHAR ,\n" + 
        		"                data.outrow VARCHAR ,\n" + 
        		"                data.pass_time VARCHAR ,\n" + 
        		"                data.type VARCHAR ,\n" + 
        		"                id INTEGER not null primary key desc\n" + 
        		"                ) ");
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("DROP TABLE m_interface_job");
    }
    
    private void createTable(String ddl, String tenantId, long ts) throws Exception {
        Properties props = new Properties();
        if (tenantId != null) {
        	props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        conn.close();
    }
    
    @Test
    public void testCreateTenantSpecificTable() throws Exception {
    	tenantTableNames.add(TENANT_TABLE_NAME);
        long ts = nextTimestamp();
        createTable(BASE_TABLE_DDL, null, ts);
        createTable(TENANT_TABLE_DDL, TENANT_ID, ts+10);
        // ensure we didn't create a physical HBase table for the tenant-specific table
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES).getAdmin();
        assertEquals(0, admin.listTables("TENANT_SPECIFIC_TABLE").length);
    }
    
    @Test
    public void testCreateTenantTableTwice() throws Exception {
    	tenantTableNames.add(TENANT_TABLE_NAME);
        long ts = nextTimestamp();
        createTable(BASE_TABLE_DDL, null, ts);
        createTable(TENANT_TABLE_DDL, TENANT_ID, ts+10);
        try {
        	createTable(TENANT_TABLE_DDL, TENANT_ID, ts+20);
        	fail();
        }
        catch (TableAlreadyExistsException expected) {}
    }
    
    @Test
    public void testCreateTenantTableBaseTableTopLevel() throws Exception {
    	tenantTableNames.add(TENANT_TABLE_NAME);
        long ts = nextTimestamp();
        createTable(BASE_TABLE_DDL, null, ts);
        createTable(TENANT_TABLE_DDL, TENANT_ID, ts+10);
        try {
        	createTable("CREATE TABLE TENANT_TABLE2 (COL VARCHAR) BASE_TABLE='TENANT_SPECIFIC_TABLE',TENANT_TYPE_ID='aaa'", TENANT_ID, ts+20);
        	fail();
        }
        catch (SQLException expected) {
            assertEquals(BASE_TABLE_NOT_TOP_LEVEL.getErrorCode(), expected.getErrorCode());
        }
    }
    
    @Test
    public void testCreateTenantTableWithDifferentTypeId() throws Exception {
    	tenantTableNames.add(TENANT_TABLE_NAME);
        long ts = nextTimestamp();
        createTable(BASE_TABLE_DDL, null, ts);
        createTable(TENANT_TABLE_DDL, TENANT_ID, ts+10);
        try {
        	createTable(TENANT_TABLE_DDL.replace(TENANT_TYPE_ID, "000"), TENANT_ID, ts+20);
            fail();
        }
        catch (TableAlreadyExistsException expected) {}
    }
    
    @Test
    public void testCreateTenantTableWithSameTypeId() throws Exception {
    	tenantTableNames.add(TENANT_TABLE_NAME);
    	tenantTableNames.add("TENANT_SPECIFIC_TABLE_II");
        createTable(BASE_TABLE_DDL, null, nextTimestamp());
        createTable(BASE_TABLE_DDL.replace("PARENT_TABLE", "PARENT_TABLE_II"), null, nextTimestamp());
        createTable(TENANT_TABLE_DDL, TENANT_ID, nextTimestamp());
        
        // Create a tenant table with tenant type id used with a different base table.  This is allowed.
        createTable("CREATE TABLE TENANT_SPECIFIC_TABLE_II ( \n" + 
    		    " col VARCHAR\n" + 
    		    " ) BASE_TABLE='PARENT_TABLE_II', TENANT_TYPE_ID='" + TENANT_TYPE_ID + "'", TENANT_ID, nextTimestamp());
        
        try {
        	// Create a tenant table with tenant type id already used for this base table.  This is not allowed.
        	createTable("CREATE TABLE TENANT_SPECIFIC_TABLE2 ( \n" + 
        		    " col VARCHAR\n" + 
        		    " ) BASE_TABLE='PARENT_TABLE', TENANT_TYPE_ID='" + TENANT_TYPE_ID + "'", TENANT_ID, nextTimestamp());
        	fail();
        }
        catch (SQLException expected) {
            assertEquals(TYPE_ID_USED.getErrorCode(), expected.getErrorCode());
        }
    }
    
    @Test(expected=TableNotFoundException.class)
    public void testDeletionOfParentTableFailsOnTenantSpecificConnection() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE PARENT_TABLE ( \n" + 
                "                user VARCHAR ,\n" + 
                "                id INTEGER not null primary key desc\n" + 
                "                ) ");
        conn.close();
        
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, TENANT_ID); // connection is tenant-specific
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("DROP TABLE PARENT_TABLE");
        conn.close();
    }
    
    @Test(expected=SQLException.class)
    public void testCreationOfParentTableFailsOnTenantSpecificConnection() throws Exception {
        createTable("CREATE TABLE PARENT_TABLE ( \n" + 
                "                user VARCHAR ,\n" + 
                "                id INTEGER not null primary key desc\n" + 
                "                ) ", TENANT_ID, nextTimestamp());
    }
    
    @Test(expected=SQLException.class)
    public void testCreationOfTenantSpecificTableFailsOnNonTenantSpecificConnection() throws Exception {
        createTable("CREATE TABLE TENANT_SPECIFIC_TABLE ( \n" + 
                "                tenantCol VARCHAR \n" + 
                "                ) BASE_TABLE='PARENT_TABLE', TENANT_TYPE_ID='abc'", null, nextTimestamp());
    }
}
