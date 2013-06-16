package com.salesforce.phoenix.end2end;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.util.PhoenixRuntime;

public class CreateTableTest  extends BaseClientMangedTimeTest {

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
}
