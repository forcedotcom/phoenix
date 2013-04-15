package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.HBASE_DYNAMIC_COLUMNS;
import static com.salesforce.phoenix.util.TestUtil.PHOENIX_JDBC_URL;
import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

public class DynamicColumnTest extends BaseClientMangedTimeTest {
	 private static final byte[] HBASE_DYNAMIC_COLUMNS_BYTES = SchemaUtil.getTableName(Bytes.toBytes(HBASE_DYNAMIC_COLUMNS));
	 private static final byte[] FAMILY_NAME = Bytes.toBytes(SchemaUtil.normalizeIdentifier("A"));
	 private static final byte[] FAMILY_NAME2 = Bytes.toBytes(SchemaUtil.normalizeIdentifier("B"));
	
	 @BeforeClass
	    public static void doBeforeTestSetup() throws Exception {
	        HBaseAdmin admin = new HBaseAdmin(driver.getQueryServices().getConfig());
	        try {
	            try {
	                admin.disableTable(HBASE_DYNAMIC_COLUMNS_BYTES);
	                admin.deleteTable(HBASE_DYNAMIC_COLUMNS_BYTES);
	            } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
	            }
	            ensureTableCreated(getUrl(),HBASE_DYNAMIC_COLUMNS);
	            initTableValues();
	        } finally {
	            admin.close();
	        }
	    }
	 
	 private static void initTableValues() throws Exception {
	        ConnectionQueryServices services = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES);
	        HTableInterface hTable = services.getTable(SchemaUtil.getTableName(Bytes.toBytes(HBASE_DYNAMIC_COLUMNS)));
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
	        ensureTableCreated(getUrl(),HBASE_DYNAMIC_COLUMNS);
	    }
	
	 @Test
	    public void testDynamicColums() throws Exception {
	        String query = "SELECT * FROM HBASE_DYNAMIC_COLUMNS (DV varchar)";
	        String url = PHOENIX_JDBC_URL + ";";
	        Properties props = new Properties(TEST_PROPERTIES);
	        Connection conn = DriverManager.getConnection(url, props);
	        try {
	            PreparedStatement statement = conn.prepareStatement(query);
	            ResultSet rs = statement.executeQuery();
	            assertTrue(rs.next());
	            assertEquals("entry1", rs.getString(1));
	            assertEquals("first", rs.getString(2));
	            assertEquals("f1value1", rs.getString(3));
	            assertEquals("f1value2", rs.getString(4));
	            assertEquals("f2value1", rs.getString(5));
	            assertEquals("default", rs.getString(6));
	            assertFalse(rs.next());
	        } finally {
	            conn.close();
	        }
	    }
	 
	 @Test
	    public void testDynamicColumsFamily() throws Exception {
	        String query = "SELECT * FROM HBASE_DYNAMIC_COLUMNS (DV varchar,B.F2V2 varchar)";
	        String url = PHOENIX_JDBC_URL + ";";
	        Properties props = new Properties(TEST_PROPERTIES);
	        Connection conn = DriverManager.getConnection(url, props);
	        try {
	            PreparedStatement statement = conn.prepareStatement(query);
	            ResultSet rs = statement.executeQuery();
	            assertTrue(rs.next());
	            assertEquals("entry1", rs.getString(1));
	            assertEquals("first", rs.getString(2));
	            assertEquals("f1value1", rs.getString(3));
	            assertEquals("f1value2", rs.getString(4));
	            assertEquals("f2value1", rs.getString(5));
	            assertEquals("default", rs.getString(6));
	            assertEquals("f2value2", rs.getString(7));
	            assertFalse(rs.next());
	        } finally {
	            conn.close();
	        }
	    }
	 
	 @Test
	    public void testDynamicColumsSpecificQuery() throws Exception {
	        String query = "SELECT entry,F2V2 FROM HBASE_DYNAMIC_COLUMNS (DV varchar,B.F2V2 varchar)";
	        String url = PHOENIX_JDBC_URL + ";";
	        Properties props = new Properties(TEST_PROPERTIES);
	        Connection conn = DriverManager.getConnection(url, props);
	        try {
	            PreparedStatement statement = conn.prepareStatement(query);
	            ResultSet rs = statement.executeQuery();
	            assertTrue(rs.next());
	            assertEquals("entry1", rs.getString(1));
	            assertEquals("f2value2", rs.getString(2));
	            assertFalse(rs.next());
	        } finally {
	            conn.close();
	        }
	    }

}

