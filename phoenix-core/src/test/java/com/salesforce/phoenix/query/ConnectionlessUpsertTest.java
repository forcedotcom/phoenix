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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.BeforeClass;
import org.junit.Test;

import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData;
import com.salesforce.phoenix.jdbc.PhoenixDriver;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.SaltingUtil;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.PhoenixRuntime;


public class ConnectionlessUpsertTest {
    private static final int saltBuckets = 200;
    private static final String orgId = "00D300000000XHP";
    private static final String keyPrefix1 = "111";
    private static final String keyPrefix2 = "112";
    private static final String entityHistoryId1 = "123456789012";
    private static final String entityHistoryId2 = "987654321098";
    private static final String name1 = "Eli";
    private static final String name2 = "Simon";
    private static final Date now = new Date(System.currentTimeMillis());
    private static final byte[] unsaltedRowKey1 = ByteUtil.concat(
            PDataType.CHAR.toBytes(orgId),PDataType.CHAR.toBytes(keyPrefix1),PDataType.CHAR.toBytes(entityHistoryId1));
    private static final byte[] unsaltedRowKey2 = ByteUtil.concat(
            PDataType.CHAR.toBytes(orgId),PDataType.CHAR.toBytes(keyPrefix2),PDataType.CHAR.toBytes(entityHistoryId2));
    private static final byte[] saltedRowKey1 = ByteUtil.concat(
            new byte[] {SaltingUtil.getSaltingByte(unsaltedRowKey1, 0, unsaltedRowKey1.length, saltBuckets)},
            unsaltedRowKey1);
    private static final byte[] saltedRowKey2 = ByteUtil.concat(
            new byte[] {SaltingUtil.getSaltingByte(unsaltedRowKey2, 0, unsaltedRowKey2.length, saltBuckets)},
            unsaltedRowKey2);

    private static String getUrl() {
        return PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + PhoenixRuntime.CONNECTIONLESS;
    }
    
    @BeforeClass
    public static void verifyDriverRegistered() throws SQLException {
        assertTrue(DriverManager.getDriver(getUrl()) == PhoenixDriver.INSTANCE);
    }
    
    @Test
    public void testConnectionlessUpsert() throws Exception {
        testConnectionlessUpsert(null);
    }
    
    @Test
    public void testSaltedConnectionlessUpsert() throws Exception {
        testConnectionlessUpsert(saltBuckets);
    }
  
    public void testConnectionlessUpsert(Integer saltBuckets) throws Exception {
        String dmlStmt = "create table core.entity_history(\n" +
        "    organization_id char(15) not null, \n" + 
        "    key_prefix char(3) not null,\n" +
        "    entity_history_id char(12) not null,\n" + 
        "    created_by varchar,\n" + 
        "    created_date date\n" +
        "    CONSTRAINT pk PRIMARY KEY (organization_id, key_prefix, entity_history_id) ) " +
        (saltBuckets == null ? "" : (PhoenixDatabaseMetaData.SALT_BUCKETS + "=" + saltBuckets));
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement statement = conn.prepareStatement(dmlStmt);
        statement.execute();
        
        String upsertStmt = "upsert into core.entity_history(organization_id,key_prefix,entity_history_id, created_by, created_date)\n" +
        "values(?,?,?,?,?)";
        statement = conn.prepareStatement(upsertStmt);
        statement.setString(1, orgId);
        statement.setString(2, keyPrefix2);
        statement.setString(3, entityHistoryId2);
        statement.setString(4, name2);
        statement.setDate(5,now);
        statement.execute();
        statement.setString(1, orgId);
        statement.setString(2, keyPrefix1);
        statement.setString(3, entityHistoryId1);
        statement.setString(4, name1);
        statement.setDate(5,now);
        statement.execute();
        
        Iterator<Pair<byte[],List<KeyValue>>> dataIterator = PhoenixRuntime.getUncommittedDataIterator(conn);
        Iterator<KeyValue> iterator = dataIterator.next().getSecond().iterator();

        byte[] expectedRowKey1 = saltBuckets == null ? unsaltedRowKey1 : saltedRowKey1;
        byte[] expectedRowKey2 = saltBuckets == null ? unsaltedRowKey2 : saltedRowKey2;
        if (Bytes.compareTo(expectedRowKey1, expectedRowKey2) < 0) {
            assertRow1(iterator, expectedRowKey1);
            assertRow2(iterator, expectedRowKey2);
        } else {
            assertRow2(iterator, expectedRowKey2);
            assertRow1(iterator, expectedRowKey1);
        }
        
        assertFalse(iterator.hasNext());
        assertFalse(dataIterator.hasNext());
        conn.rollback(); // to clear the list of mutations for the next
    }
    
    private static void assertRow1(Iterator<KeyValue> iterator, byte[] expectedRowKey1) {
        KeyValue kv;
        assertTrue(iterator.hasNext());
        kv = iterator.next();
        assertArrayEquals(expectedRowKey1, kv.getRow());        
        assertEquals(name1, PDataType.VARCHAR.toObject(kv.getValue()));
        assertTrue(iterator.hasNext());
        kv = iterator.next();
        assertArrayEquals(expectedRowKey1, kv.getRow());        
        assertEquals(now, PDataType.DATE.toObject(kv.getValue()));
        assertTrue(iterator.hasNext());
        kv = iterator.next();
        assertArrayEquals(expectedRowKey1, kv.getRow());        
        assertNull(PDataType.VARCHAR.toObject(kv.getValue()));
    }

    private static void assertRow2(Iterator<KeyValue> iterator, byte[] expectedRowKey2) {
        KeyValue kv;
        assertTrue(iterator.hasNext());
        kv = iterator.next();
        assertArrayEquals(expectedRowKey2, kv.getRow());        
        assertEquals(name2, PDataType.VARCHAR.toObject(kv.getValue()));
        assertTrue(iterator.hasNext());
        kv = iterator.next();
        assertArrayEquals(expectedRowKey2, kv.getRow());        
        assertEquals(now, PDataType.DATE.toObject(kv.getValue()));
        assertTrue(iterator.hasNext());
        kv = iterator.next();
        assertArrayEquals(expectedRowKey2, kv.getRow());        
        assertNull(PDataType.VARCHAR.toObject(kv.getValue()));
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
