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
package phoenix.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.*;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.junit.*;

import phoenix.jdbc.PhoenixEmbeddedDriver;
import phoenix.jdbc.PhoenixProdEmbeddedDriver;
import phoenix.schema.PDataType;
import phoenix.util.PhoenixRuntime;

public class ConnectionlessUpsertTest {
    private static String getUrl() {
        return PhoenixRuntime.EMBEDDED_JDBC_PROTOCOL + PhoenixEmbeddedDriver.CONNECTIONLESS;
    }
    
    @BeforeClass
    public static void registerDriver() throws ClassNotFoundException, SQLException {
        Class.forName(PhoenixProdEmbeddedDriver.class.getName());
        DriverManager.registerDriver(PhoenixProdEmbeddedDriver.INSTANCE);
        assertTrue(DriverManager.getDriver(getUrl()) == PhoenixProdEmbeddedDriver.INSTANCE);
    }
    
    @AfterClass
    public static void deregisterDriver() throws SQLException {
        try {
            PhoenixProdEmbeddedDriver.INSTANCE.close();
        } finally {
            DriverManager.deregisterDriver(PhoenixProdEmbeddedDriver.INSTANCE);
        }
    }
    
    @Test
    public void testConnectionlessUpsert() throws Exception {
        String dmlStmt = "create table core.entity_history" +
        "   (organization_id char(15) not null, \n" + 
        "    key_prefix char(3) not null,\n" +
        "    entity_history_id char(12) not null)\n" + 
        "    a(created_by varchar,\n" + 
        "      created_date date)";
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
        
        int count = 0;
        List<Mutation>  mutations = PhoenixRuntime.getUncommittedMutations(conn);
        for (Mutation m : mutations) {
            for (List<KeyValue> kvs : m.getFamilyMap().values()) {
                if (count == 0) {
                    assertEquals("Eli", PDataType.VARCHAR.toObject(kvs.get(0).getValue()));
                    assertEquals(now, PDataType.DATE.toObject(kvs.get(1).getValue()));
                } else if (count == 1) {
                    assertEquals("Simon", PDataType.VARCHAR.toObject(kvs.get(0).getValue()));
                    assertEquals(now, PDataType.DATE.toObject(kvs.get(1).getValue()));
                }
                count++;
            }
        }
        assertEquals(2,count);
        conn.rollback(); // to clear the list of mutations for the next
    }


}
