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

import static com.salesforce.phoenix.util.TestUtil.*;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.*;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.util.*;


/**
 * 
 * Tests using native HBase types (i.e. Bytes.toBytes methods) for
 * integers and longs. Phoenix can support this if the numbers are
 * positive.
 *
 * @author jtaylor
 * @since 0.1
 */
public class NativeHBaseTypesTest extends BaseClientMangedTimeTest {
    private static final byte[] HBASE_NATIVE_BYTES = SchemaUtil.getTableName(Bytes.toBytes(HBASE_NATIVE));
    private static final byte[] FAMILY_NAME = Bytes.toBytes(SchemaUtil.normalizeIdentifier("1"));
    private static final byte[][] SPLITS = new byte[][] {Bytes.toBytes(20), Bytes.toBytes(30)};
    private static final long ts = nextTimestamp();
    
    @BeforeClass
    public static void doBeforeTestSetup() throws Exception {
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES).getAdmin();
        try {
            try {
                admin.disableTable(HBASE_NATIVE_BYTES);
                admin.deleteTable(HBASE_NATIVE_BYTES);
            } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
            }
            HTableDescriptor descriptor = new HTableDescriptor(HBASE_NATIVE_BYTES);
            HColumnDescriptor columnDescriptor =  new HColumnDescriptor(FAMILY_NAME);
            columnDescriptor.setKeepDeletedCells(true);
            descriptor.addFamily(columnDescriptor);
            admin.createTable(descriptor, SPLITS);
            initTableValues();
        } finally {
            admin.close();
        }
    }
    
    private static void initTableValues() throws Exception {
        ConnectionQueryServices services = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES);
        HTableInterface hTable = services.getTable(SchemaUtil.getTableName(Bytes.toBytes(HBASE_NATIVE)));
        try {
            // Insert rows using standard HBase mechanism with standard HBase "types"
            List<Row> mutations = new ArrayList<Row>();
            byte[] family = Bytes.toBytes("1");
            byte[] uintCol = Bytes.toBytes("UINT_COL");
            byte[] ulongCol = Bytes.toBytes("ULONG_COL");
            byte[] key, bKey;
            Put put;
            
            key = ByteUtil.concat(Bytes.toBytes(10), Bytes.toBytes(100L), Bytes.toBytes("a"));
            put = new Put(key);
            put.add(family, uintCol, ts-2, Bytes.toBytes(5));
            put.add(family, ulongCol, ts-2, Bytes.toBytes(50L));
            mutations.add(put);
            put = new Put(key);
            put.add(family, uintCol, ts, Bytes.toBytes(10));
            put.add(family, ulongCol, ts, Bytes.toBytes(100L));
            mutations.add(put);
            
            bKey = key = ByteUtil.concat(Bytes.toBytes(20), Bytes.toBytes(200L), Bytes.toBytes("b"));
            put = new Put(key);
            put.add(family, uintCol, ts-4, Bytes.toBytes(5000));
            put.add(family, ulongCol, ts-4, Bytes.toBytes(50000L));
            mutations.add(put);
            @SuppressWarnings("deprecation") // FIXME: Remove when unintentionally deprecated method is fixed (HBASE-7870).
            // FIXME: the version of the Delete constructor without the lock args was introduced
            // in 0.94.4, thus if we try to use it here we can no longer use the 0.94.2 version
            // of the client.
            Delete del = new Delete(key, ts-2, null);
            mutations.add(del);
            put = new Put(key);
            put.add(family, uintCol, ts, Bytes.toBytes(2000));
            put.add(family, ulongCol, ts, Bytes.toBytes(20000L));
            mutations.add(put);
            
            key = ByteUtil.concat(Bytes.toBytes(30), Bytes.toBytes(300L), Bytes.toBytes("c"));
            put = new Put(key);
            put.add(family, uintCol, ts, Bytes.toBytes(3000));
            put.add(family, ulongCol, ts, Bytes.toBytes(30000L));
            mutations.add(put);
            
            key = ByteUtil.concat(Bytes.toBytes(40), Bytes.toBytes(400L), Bytes.toBytes("d"));
            put = new Put(key);
            put.add(family, uintCol, ts, Bytes.toBytes(4000));
            put.add(family, ulongCol, ts, Bytes.toBytes(40000L));
            mutations.add(put);
            
            hTable.batch(mutations);
            
            Result r = hTable.get(new Get(bKey));
            assertFalse(r.isEmpty());
        } finally {
            hTable.close();
        }
        // Create Phoenix table after HBase table was created through the native APIs
        // The timestamp of the table creation must be later than the timestamp of the data
        ensureTableCreated(getUrl(),HBASE_NATIVE,null, ts+1);
    }
    
    @Test
    public void testRangeQuery1() throws Exception {
        String query = "SELECT uint_key, ulong_key, string_key FROM HBASE_NATIVE WHERE uint_key > 20 and ulong_key >= 400";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(40, rs.getInt(1));
            assertEquals(400L, rs.getLong(2));
            assertEquals("d", rs.getString(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testRangeQuery2() throws Exception {
        String query = "SELECT uint_key, ulong_key, string_key FROM HBASE_NATIVE WHERE uint_key > 20 and uint_key < 40";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(30, rs.getInt(1));
            assertEquals(300L, rs.getLong(2));
            assertEquals("c", rs.getString(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testRangeQuery3() throws Exception {
        String query = "SELECT uint_key, ulong_key, string_key FROM HBASE_NATIVE WHERE ulong_key > 200 and ulong_key < 400";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(30, rs.getInt(1));
            assertEquals(300L, rs.getLong(2));
            assertEquals("c", rs.getString(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNegativeAgainstUnsignedNone() throws Exception {
        String query = "SELECT uint_key, ulong_key, string_key FROM HBASE_NATIVE WHERE ulong_key < -1";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNegativeAgainstUnsignedAll() throws Exception {
        String query = "SELECT string_key FROM HBASE_NATIVE WHERE ulong_key > -100";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("d", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNegativeAddNegativeValue() throws Exception {
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO HBASE_NATIVE(uint_key,ulong_key,string_key, uint_col) VALUES(?,?,?,?)");
            stmt.setInt(1, -1);
            stmt.setLong(2, 2L);
            stmt.setString(3,"foo");
            stmt.setInt(4, 3);
            stmt.execute();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Type mismatch"));
        }
    }
    
    @Test
    public void testNegativeCompareNegativeValue() throws Exception {
        String query = "SELECT string_key FROM HBASE_NATIVE WHERE uint_key > 100000";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 7); // Run query at timestamp 7
        Properties props = new Properties(TEST_PROPERTIES);
        PhoenixConnection conn = DriverManager.getConnection(url, props).unwrap(PhoenixConnection.class);
        HTableInterface hTable = conn.getQueryServices().getTable(SchemaUtil.getTableName(Bytes.toBytes(HBASE_NATIVE)));
        
        List<Row> mutations = new ArrayList<Row>();
        byte[] family = Bytes.toBytes("1");
        byte[] uintCol = Bytes.toBytes("UINT_COL");
        byte[] ulongCol = Bytes.toBytes("ULONG_COL");
        byte[] key;
        Put put;
        
        // Need to use native APIs because the Phoenix APIs wouldn't let you insert a
        // negative number for an unsigned type
        key = ByteUtil.concat(Bytes.toBytes(-10), Bytes.toBytes(100L), Bytes.toBytes("e"));
        put = new Put(key);
        // Insert at later timestamp than other queries in this test are using, so that
        // we don't affect them
        put.add(family, uintCol, ts+6, Bytes.toBytes(10));
        put.add(family, ulongCol, ts+6, Bytes.toBytes(100L));
        put.add(family, QueryConstants.EMPTY_COLUMN_BYTES, ts+6, ByteUtil.EMPTY_BYTE_ARRAY);
        mutations.add(put);
        hTable.batch(mutations);
    
        // Demonstrates weakness of HBase Bytes serialization. Negative numbers
        // show up as bigger than positive numbers
        PreparedStatement statement = conn.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals("e", rs.getString(1));
        assertFalse(rs.next());
    }
}
    
