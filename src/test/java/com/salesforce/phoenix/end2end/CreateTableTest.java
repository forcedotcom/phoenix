package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.sql.*;
import java.util.Properties;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Test;

import com.salesforce.phoenix.schema.TableNotFoundException;
import com.salesforce.phoenix.util.PhoenixRuntime;

public class CreateTableTest  extends BaseClientMangedTimeTest {
    
    private static final String tenantId = "abc";

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
    
    @Test
    public void testCreateTenantSpecificTable() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE PARENT_TABLE ( \n" + 
                "                user VARCHAR ,\n" + 
                "                id INTEGER not null primary key desc\n" + 
                "                ) ");
        conn.close();
        
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        conn = DriverManager.getConnection(getUrl(), props);
        
        conn.createStatement().execute("CREATE TABLE TENANT_SPECIFIC_TABLE ( \n" + 
                "                tenantCol VARCHAR ,\n" + 
                "                id INTEGER not null primary key desc\n" + 
                "                ) BASE_TABLE='PARENT_TABLE'");
        conn.close();
        
        // ensure we didn't create a physical HBase table for the tenannt-specifidc table
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES).getAdmin();
        assertEquals(0, admin.listTables("TENANT_SPECIFIC_TABLE").length);
        
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("DROP TABLE TENANT_SPECIFIC_TABLE");
        conn.close();
        
        props.remove(PhoenixRuntime.TENANT_ID_ATTRIB);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("DROP TABLE PARENT_TABLE");
        conn.close();
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
        
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId); // connection is tenant-specific
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("DROP TABLE PARENT_TABLE");
        conn.close();
    }
    
    @Test(expected=SQLException.class)
    public void testCreationOfParentTableFailsOnTenantSpecificConnection() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId); // connection is tenant-specific
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE PARENT_TABLE ( \n" + 
                "                user VARCHAR ,\n" + 
                "                id INTEGER not null primary key desc\n" + 
                "                ) ");
        conn.close();
    }
    
    @Test(expected=SQLException.class)
    public void testCreationOfTenantSpecificTableFailsOnNonTenantSpecificConnection() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE TENANT_SPECIFIC_TABLE ( \n" + 
                "                tenantCol VARCHAR ,\n" + 
                "                id INTEGER not null primary key desc\n" + 
                "                ) BASE_TABLE='PARENT_TABLE'");
        conn.close();
    }
}
