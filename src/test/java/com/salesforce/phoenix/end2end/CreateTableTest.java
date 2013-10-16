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
