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

import static com.salesforce.phoenix.util.TestUtil.HBASE_DYNAMIC_COLUMNS;
import static com.salesforce.phoenix.util.TestUtil.HBASE_DYNAMIC_FAMILIES;
import static com.salesforce.phoenix.util.TestUtil.PHOENIX_JDBC_URL;
import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.util.SchemaUtil;
import com.salesforce.phoenix.schema.ColumnFamilyNotFoundException;
import com.salesforce.phoenix.exception.PhoenixParserException;

/**
 * 
 * Basic tests for Phoenix dynamic family querying "cf.*"
 *
 * @author nmaillard
 * @since 1.2
 */

public class DynamicFamilyTest  extends BaseClientMangedTimeTest {
	private static final byte[] HBASE_DYNAMIC_FAMILIES_BYTES = SchemaUtil.getTableName(Bytes.toBytes(HBASE_DYNAMIC_FAMILIES));
	 private static final byte[] FAMILY_NAME = Bytes.toBytes(SchemaUtil.normalizeIdentifier("A"));
	 private static final byte[] FAMILY_NAME2 = Bytes.toBytes(SchemaUtil.normalizeIdentifier("B"));
	 
	 @BeforeClass
	    public static void doBeforeTestSetup() throws Exception {
	        HBaseAdmin admin = new HBaseAdmin(driver.getQueryServices().getConfig());
	        try {
	            try {
	                admin.disableTable(HBASE_DYNAMIC_FAMILIES_BYTES);
	                admin.deleteTable(HBASE_DYNAMIC_FAMILIES_BYTES);
	            } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
	            }
	            ensureTableCreated(getUrl(),HBASE_DYNAMIC_FAMILIES);
	            initTableValues();
	        } finally {
	            admin.close();
	        }
	    }
	 
	 private static void initTableValues() throws Exception {
	        ConnectionQueryServices services = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES);
	        HTableInterface hTable = services.getTable(SchemaUtil.getTableName(Bytes.toBytes(HBASE_DYNAMIC_FAMILIES)));
	        try {
	            // Insert rows using standard HBase mechanism with standard HBase "types"
	            List<Row> mutations = new ArrayList<Row>();
	            byte[] dv = Bytes.toBytes("DV");
	            byte[] first = Bytes.toBytes("F");
	            byte[] f1v1 = Bytes.toBytes("F1V1");
	            byte[] f1v2 = Bytes.toBytes("F1V2");
	            byte[] f2v1 = Bytes.toBytes("F2V1");
	            byte[] f2v2 = Bytes.toBytes("F2V2");
	            byte[] key = Bytes.toBytes("entry1");
	            
	            Put put = new Put(key);
	            put.add(QueryConstants.EMPTY_COLUMN_BYTES, dv, Bytes.toBytes("default"));
	            put.add(QueryConstants.EMPTY_COLUMN_BYTES, first, Bytes.toBytes("first"));
	            put.add(FAMILY_NAME, f1v1, Bytes.toBytes("f1value1"));
	            put.add(FAMILY_NAME, f1v2,  Bytes.toBytes("f1value2"));
	            put.add(FAMILY_NAME2, f2v1, Bytes.toBytes("f2value1"));
	            put.add(FAMILY_NAME2, f2v2,  Bytes.toBytes("f2value2"));
	            mutations.add(put);
	              
	            hTable.batch(mutations);
	            
	        } finally {
	            hTable.close();
	        }
	        // Create Phoenix table after HBase table was created through the native APIs
	        // The timestamp of the table creation must be later than the timestamp of the data
	        ensureTableCreated(getUrl(),HBASE_DYNAMIC_FAMILIES);
	    }
	 
	 @Test
	    public void testDynamicFamily() throws Exception {
	        String query = "SELECT A.* FROM HBASE_DYNAMIC_FAMILIES";
	        String url = PHOENIX_JDBC_URL + ";";
	        Properties props = new Properties(TEST_PROPERTIES);
	        Connection conn = DriverManager.getConnection(url, props);
	        try {
	            PreparedStatement statement = conn.prepareStatement(query);
	            ResultSet rs = statement.executeQuery();
	            assertTrue(rs.next());
	            assertEquals("f1value1", rs.getString(1));
	            assertEquals("f1value2", rs.getString(2));
	            assertFalse(rs.next());
	        } finally {
	            conn.close();
	        }
	    }
	 
	 @Test
	    public void testDynamicFamilyDual() throws Exception {
	        String query = "SELECT A.*,F1V1,F1V2,entry,F2V1 FROM HBASE_DYNAMIC_FAMILIES";
	        String url = PHOENIX_JDBC_URL + ";";
	        Properties props = new Properties(TEST_PROPERTIES);
	        Connection conn = DriverManager.getConnection(url, props);
	        try {
	            PreparedStatement statement = conn.prepareStatement(query);
	            ResultSet rs = statement.executeQuery();
	            assertTrue(rs.next());
	            assertEquals("f1value1", rs.getString(1));
	            assertEquals("f1value2", rs.getString(2));
	            assertEquals(rs.getString(1), rs.getString(3));
	            assertEquals(rs.getString(2), rs.getString(4));
	            assertEquals("entry1", rs.getString(5));
	            assertEquals("f2value1", rs.getString(6));
	            assertFalse(rs.next());
	        } finally {
	            conn.close();
	        }
	    }
	 
	 
	 @Test(expected = ColumnFamilyNotFoundException.class)
	    public void testDynamicFamilyException() throws Exception {
	        String query = "SELECT C.* FROM HBASE_DYNAMIC_FAMILIES";
	        String url = PHOENIX_JDBC_URL + ";";
	        Properties props = new Properties(TEST_PROPERTIES);
	        Connection conn = DriverManager.getConnection(url, props);
	        try{
	            PreparedStatement statement = conn.prepareStatement(query);
	            ResultSet rs = statement.executeQuery();
	        } finally {
	            conn.close();
	        }
	    }

	@Test(expected = PhoenixParserException.class)
            public void testDynamicFamilyFunctionException() throws Exception {
                String query = "SELECT count(C.*) FROM HBASE_DYNAMIC_FAMILIES";
                String url = PHOENIX_JDBC_URL + ";";
                Properties props = new Properties(TEST_PROPERTIES);
                Connection conn = DriverManager.getConnection(url, props);
                try{
                    PreparedStatement statement = conn.prepareStatement(query);
                    ResultSet rs = statement.executeQuery();
                } finally {
                    conn.close();
                }
            } 

}

