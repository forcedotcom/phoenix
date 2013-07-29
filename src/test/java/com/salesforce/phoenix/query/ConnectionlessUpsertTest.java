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
package com.salesforce.phoenix.query;

import static org.junit.Assert.*;

import java.sql.*;
import java.sql.Date;
import java.util.*;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.*;

import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.jdbc.PhoenixDriver;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.PhoenixRuntime;


public class ConnectionlessUpsertTest {
    private static String getUrl() {
        return PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + PhoenixRuntime.CONNECTIONLESS;
    }
    
    @BeforeClass
    public static void verifyDriverRegistered() throws SQLException {
        assertTrue(DriverManager.getDriver(getUrl()) == PhoenixDriver.INSTANCE);
    }
    
    @Test
    public void testConnectionlessUpsert() throws Exception {
        String dmlStmt = "create table core.entity_history(\n" +
        "    organization_id char(15) not null, \n" + 
        "    key_prefix char(3) not null,\n" +
        "    entity_history_id char(12) not null,\n" + 
        "    created_by varchar,\n" + 
        "    created_date date\n" +
        "    CONSTRAINT pk PRIMARY KEY (organization_id, key_prefix, entity_history_id)\n" +
        ")";
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement statement = conn.prepareStatement(dmlStmt);
        statement.execute();
        
        Date now = new Date(System.currentTimeMillis());
        String upsertStmt = "upsert into core.entity_history(organization_id,key_prefix,entity_history_id, created_by, created_date)\n" +
        "values(?,?,?,?,?)";
        statement = conn.prepareStatement(upsertStmt);
        statement.setString(1, "00D300000000XHP");
        statement.setString(2, "112");
        statement.setString(3, "123456789012");
        statement.setString(4, "Simon");
        statement.setDate(5,now);
        statement.execute();
        statement.setString(1, "00D300000000XHP");
        statement.setString(2, "111");
        statement.setString(3, "123456789012");
        statement.setString(4, "Eli");
        statement.setDate(5,now);
        statement.execute();
        
        Iterator<Pair<byte[],List<KeyValue>>> dataIterator = PhoenixRuntime.getUncommittedDataIterator(conn);
        Iterator<KeyValue> iterator = dataIterator.next().getSecond().iterator();
        assertTrue(iterator.hasNext());
        assertEquals("Eli", PDataType.VARCHAR.toObject(iterator.next().getValue()));
        assertTrue(iterator.hasNext());
        assertEquals(now, PDataType.DATE.toObject(iterator.next().getValue()));
        assertTrue(iterator.hasNext());
        assertNull(PDataType.VARCHAR.toObject(iterator.next().getValue()));
        assertTrue(iterator.hasNext());
        assertEquals("Simon", PDataType.VARCHAR.toObject(iterator.next().getValue()));
        assertTrue(iterator.hasNext());
        assertEquals(now, PDataType.DATE.toObject(iterator.next().getValue()));
        assertTrue(iterator.hasNext());
        assertNull(PDataType.VARCHAR.toObject(iterator.next().getValue()));
        assertFalse(iterator.hasNext());
        assertFalse(dataIterator.hasNext());
        conn.rollback(); // to clear the list of mutations for the next
    }

    
    @Test
    public void testNoConnectionInfo() throws Exception {
        try {
            DriverManager.getConnection(PhoenixRuntime.JDBC_PROTOCOL);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.MALFORMED_CONNECTION_URL.getSQLState(),e.getSQLState());
        }
    }
    

}
