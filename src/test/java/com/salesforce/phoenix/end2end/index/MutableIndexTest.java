package com.salesforce.phoenix.end2end.index;

import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.util.QueryUtil;
import com.salesforce.phoenix.util.ReadOnlyProps;


public class MutableIndexTest extends BaseMutableIndexTest {
    @BeforeClass 
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
        // Don't split intra region so we can more easily know that the n-way parallelization is for the explain plan
        props.put(QueryServices.MAX_INTRA_REGION_PARALLELIZATION_ATTRIB, Integer.toString(1));
        // Must update config before starting server
        startServer(getUrl(), new ReadOnlyProps(props.entrySet().iterator()));
    }
        
    @Before // FIXME: this shouldn't be necessary, but the tests hang without it.
    public void destroyTables() throws Exception {
        // Physically delete HBase table so that splits occur as expected for each test
        Properties props = new Properties(TEST_PROPERTIES);
        ConnectionQueryServices services = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class).getQueryServices();
        HBaseAdmin admin = services.getAdmin();
        try {
            try {
                admin.disableTable(INDEX_TABLE_FULL_NAME);
                admin.deleteTable(INDEX_TABLE_FULL_NAME);
            } catch (TableNotFoundException e) {
            }
            try {
                admin.disableTable(DATA_TABLE_FULL_NAME);
                admin.deleteTable(DATA_TABLE_FULL_NAME);
            } catch (TableNotFoundException e) {
            }
       } finally {
                admin.close();
        }
    }
    
    @Test
    public void testIndexWithNullableFixedWithCols() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            createTestTable();
            populateTestTable();
            String ddl = "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME
                    + " (char_col1 ASC, int_col1 ASC)"
                    + " INCLUDE (long_col1, long_col2)";
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            String query = "SELECT char_col1, int_col1 from " + DATA_TABLE_FULL_NAME;
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals("chara", rs.getString("char_col1"));
            assertEquals(2, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCoveredColumnUpdates() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            createTestTable();
            populateTestTable();
            String ddl = "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME
                    + " (char_col1 ASC, int_col1 ASC)"
                    + " INCLUDE (long_col1, long_col2)";
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            String query = "SELECT char_col1, int_col1, long_col2 from " + DATA_TABLE_FULL_NAME;
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(3L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertEquals(4L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertEquals(5L, rs.getLong(3));
            assertFalse(rs.next());
            
            stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME
                    + "(varchar_pk, char_pk, int_pk, long_pk , decimal_pk, long_col2) SELECT varchar_pk, char_pk, int_pk, long_pk , decimal_pk, long_col2*2 FROM "
                    + DATA_TABLE_FULL_NAME + " WHERE long_col2=?");
            stmt.setLong(1,4L);
            assertEquals(1,stmt.executeUpdate());
            conn.commit();
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(3L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertEquals(8L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertEquals(5L, rs.getLong(3));
            assertFalse(rs.next());
            
            stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME
                    + "(varchar_pk, char_pk, int_pk, long_pk , decimal_pk, long_col2) SELECT varchar_pk, char_pk, int_pk, long_pk , decimal_pk, null FROM "
                    + DATA_TABLE_FULL_NAME + " WHERE long_col2=?");
            stmt.setLong(1,3L);
            assertEquals(1,stmt.executeUpdate());
            conn.commit();
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(0, rs.getLong(3));
            assertTrue(rs.wasNull());
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertEquals(8L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertEquals(5L, rs.getLong(3));
            assertFalse(rs.next());
            
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSelectAll() throws Exception {
        String query;
        ResultSet rs;
        
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        conn.createStatement().execute("CREATE TABLE " + DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
        
        conn.createStatement().execute("CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v2 DESC) INCLUDE (v1)");
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1,"a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        stmt.setString(1,"b");
        stmt.setString(2, "y");
        stmt.setString(3, "2");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));

        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertEquals("y",rs.getString(2));
        assertEquals("2",rs.getString(3));
        assertEquals("b",rs.getString("k"));
        assertEquals("y",rs.getString("v1"));
        assertEquals("2",rs.getString("v2"));
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("x",rs.getString(2));
        assertEquals("1",rs.getString(3));
        assertEquals("a",rs.getString("k"));
        assertEquals("x",rs.getString("v1"));
        assertEquals("1",rs.getString("v2"));
        assertFalse(rs.next());
    }
    
    @Test
    public void testSelectCF() throws Exception {
        String query;
        ResultSet rs;
        
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        conn.createStatement().execute("CREATE TABLE " + DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, a.v1 VARCHAR, a.v2 VARCHAR, b.v1 VARCHAR)  ");
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
        
        conn.createStatement().execute("CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v2 DESC) INCLUDE (a.v1)");
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?,?)");
        stmt.setString(1,"a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.setString(4, "A");
        stmt.execute();
        stmt.setString(1,"b");
        stmt.setString(2, "y");
        stmt.setString(3, "2");
        stmt.setString(4, "B");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + DATA_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));

        query = "SELECT a.* FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("y",rs.getString(1));
        assertEquals("2",rs.getString(2));
        assertEquals("y",rs.getString("v1"));
        assertEquals("2",rs.getString("v2"));
        assertTrue(rs.next());
        assertEquals("x",rs.getString(1));
        assertEquals("1",rs.getString(2));
        assertEquals("x",rs.getString("v1"));
        assertEquals("1",rs.getString("v2"));
        assertFalse(rs.next());
    }
    
    // @Test Broken, but Jesse is fixing
    public void testCompoundIndexKey() throws Exception {
        String query;
        ResultSet rs;
        
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        conn.createStatement().execute("CREATE TABLE " + DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
        
        conn.createStatement().execute("CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1, v2)");
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1,"a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("x",rs.getString(1));
        assertEquals("1",rs.getString(2));
        assertEquals("a",rs.getString(3));
        assertFalse(rs.next());

        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1,"a");
        stmt.setString(2, "y");
        stmt.setString(3, null);
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("y",rs.getString(1));
        assertNull(rs.getString(2));
        assertEquals("a",rs.getString(3));
        assertFalse(rs.next());

       query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));

        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertNull(rs.getString(2));
        assertEquals("y",rs.getString(3));
        assertFalse(rs.next());
    }
    
}
