package com.salesforce.phoenix.end2end.index;

import static com.salesforce.phoenix.util.TestUtil.*;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.salesforce.phoenix.end2end.BaseHBaseManagedTimeTest;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.util.*;


public class IndexTest extends BaseHBaseManagedTimeTest{
    private static final int TABLE_SPLITS = 3;
    private static final int INDEX_SPLITS = 4;
    private static final byte[] DATA_TABLE_FULL_NAME = SchemaUtil.getTableName(null, "T");
    private static final byte[] INDEX_TABLE_FULL_NAME = SchemaUtil.getTableName(null, "I");
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
        // Don't cache meta information for this test because the splits change between tests
        props.put(QueryServices.REGION_BOUNDARY_CACHE_TTL_MS_ATTRIB, Integer.toString(0));
        // Must update config before starting server
        startServer(getUrl(), new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    // Populate the test table with data.
    private static void populateTestTable() throws SQLException {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String upsert = "UPSERT INTO " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE
                    + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsert);
            stmt.setString(1, "varchar1");
            stmt.setString(2, "char1");
            stmt.setInt(3, 1);
            stmt.setLong(4, 1L);
            stmt.setBigDecimal(5, new BigDecimal(1.0));
            stmt.setString(6, "varchar_a");
            stmt.setString(7, "chara");
            stmt.setInt(8, 2);
            stmt.setLong(9, 2L);
            stmt.setBigDecimal(10, new BigDecimal(2.0));
            stmt.setString(11, "varchar_b");
            stmt.setString(12, "charb");
            stmt.setInt(13, 3);
            stmt.setLong(14, 3L);
            stmt.setBigDecimal(15, new BigDecimal(3.0));
            stmt.executeUpdate();
            
            stmt.setString(1, "varchar2");
            stmt.setString(2, "char2");
            stmt.setInt(3, 2);
            stmt.setLong(4, 2L);
            stmt.setBigDecimal(5, new BigDecimal(2.0));
            stmt.setString(6, "varchar_a");
            stmt.setString(7, "chara");
            stmt.setInt(8, 3);
            stmt.setLong(9, 3L);
            stmt.setBigDecimal(10, new BigDecimal(3.0));
            stmt.setString(11, "varchar_b");
            stmt.setString(12, "charb");
            stmt.setInt(13, 4);
            stmt.setLong(14, 4L);
            stmt.setBigDecimal(15, new BigDecimal(4.0));
            stmt.executeUpdate();
            
            stmt.setString(1, "varchar3");
            stmt.setString(2, "char3");
            stmt.setInt(3, 3);
            stmt.setLong(4, 3L);
            stmt.setBigDecimal(5, new BigDecimal(3.0));
            stmt.setString(6, "varchar_a");
            stmt.setString(7, "chara");
            stmt.setInt(8, 4);
            stmt.setLong(9, 4L);
            stmt.setBigDecimal(10, new BigDecimal(4.0));
            stmt.setString(11, "varchar_b");
            stmt.setString(12, "charb");
            stmt.setInt(13, 5);
            stmt.setLong(14, 5L);
            stmt.setBigDecimal(15, new BigDecimal(5.0));
            stmt.executeUpdate();
            
            conn.commit();
        } finally {
            conn.close();
        }
    }
    
    private static void destroyTables() throws Exception {
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
    public void testImmutableTableIndexMaintanenceSaltedSalted() throws Exception {
        testImmutableTableIndexMaintanence(TABLE_SPLITS, INDEX_SPLITS);
    }

    @Test
    public void testImmutableTableIndexMaintanenceSalted() throws Exception {
        testImmutableTableIndexMaintanence(null, INDEX_SPLITS);
    }

    @Test
    public void testImmutableTableIndexMaintanenceUnsalted() throws Exception {
        testImmutableTableIndexMaintanence(null, null);
    }

    private void testImmutableTableIndexMaintanence(Integer tableSaltBuckets, Integer indexSaltBuckets) throws Exception {
        try {
            String query;
            ResultSet rs;
            
            Properties props = new Properties(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            conn.setAutoCommit(false);
            conn.createStatement().execute("CREATE TABLE t (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR) IMMUTABLE_ROWS=true " +  (tableSaltBuckets == null ? "" : ", SALT_BUCKETS=" + tableSaltBuckets));
            query = "SELECT * FROM t";
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
            
            conn.createStatement().execute("CREATE INDEX i ON t (v DESC)" + (indexSaltBuckets == null ? "" : " SALT_BUCKETS=" + indexSaltBuckets));
            query = "SELECT * FROM i";
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
    
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?)");
            stmt.setString(1,"a");
            stmt.setString(2, "x");
            stmt.execute();
            stmt.setString(1,"b");
            stmt.setString(2, "y");
            stmt.execute();
            conn.commit();
            
            query = "SELECT * FROM i";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("y",rs.getString(1));
            assertEquals("b",rs.getString(2));
            assertTrue(rs.next());
            assertEquals("x",rs.getString(1));
            assertEquals("a",rs.getString(2));
            assertFalse(rs.next());
    
            query = "SELECT k,v FROM t WHERE v = 'y'";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertEquals("y",rs.getString(2));
            assertFalse(rs.next());
            
            String expectedPlan;
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            expectedPlan = indexSaltBuckets == null ? 
                 "CLIENT PARALLEL 1-WAY RANGE SCAN OVER I 'y'" : 
                ("CLIENT PARALLEL 4-WAY SKIP SCAN ON 4 KEYS OVER I 0...3,'y'\n" + 
                 "CLIENT MERGE SORT");
            assertEquals(expectedPlan,QueryUtil.getExplainPlan(rs));
    
            // Will use index, so rows returned in DESC order.
            // This is not a bug, though, because we can
            // return in any order.
            query = "SELECT k,v FROM t WHERE v >= 'x'";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertEquals("y",rs.getString(2));
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals("x",rs.getString(2));
            assertFalse(rs.next());
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            expectedPlan = indexSaltBuckets == null ? 
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER I (*-'x']" :
                ("CLIENT PARALLEL 4-WAY SKIP SCAN ON 4 RANGES OVER I 0...3,(*-'x']\n" + 
                 "CLIENT MERGE SORT");
            assertEquals(expectedPlan,QueryUtil.getExplainPlan(rs));
            
            // Will still use index, since there's no LIMIT clause
            query = "SELECT k,v FROM t WHERE v >= 'x' ORDER BY k";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals("x",rs.getString(2));
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertEquals("y",rs.getString(2));
            assertFalse(rs.next());
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            // Turns into an ORDER BY, which could be bad if lots of data is
            // being returned. Without stats we don't know. The alternative
            // would be a full table scan.
            expectedPlan = indexSaltBuckets == null ? 
                ("CLIENT PARALLEL 1-WAY RANGE SCAN OVER I (*-'x']\n" + 
                 "    SERVER TOP -1 ROWS SORTED BY [K]\n" + 
                 "CLIENT MERGE SORT") :
                ("CLIENT PARALLEL 4-WAY SKIP SCAN ON 4 RANGES OVER I 0...3,(*-'x']\n" + 
                 "    SERVER TOP -1 ROWS SORTED BY [K]\n" + 
                 "CLIENT MERGE SORT");
            assertEquals(expectedPlan,QueryUtil.getExplainPlan(rs));
            
            // Will use data table now, since there's a LIMIT clause and
            // we're able to optimize out the ORDER BY, unless the data
            // table is salted.
            query = "SELECT k,v FROM t WHERE v >= 'x' ORDER BY k LIMIT 2";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals("x",rs.getString(2));
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertEquals("y",rs.getString(2));
            assertFalse(rs.next());
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            expectedPlan = tableSaltBuckets == null ? 
                 "CLIENT PARALLEL 1-WAY FULL SCAN OVER T\n" + 
                 "    SERVER FILTER BY V >= 'x'\n" + 
                 "    SERVER 2 ROW LIMIT\n" + 
                 "CLIENT 2 ROW LIMIT" :
                 "CLIENT PARALLEL 4-WAY SKIP SCAN ON 4 RANGES OVER I 0...3,(*-'x']\n" + 
                 "    SERVER TOP 2 ROWS SORTED BY [K]\n" + 
                 "CLIENT MERGE SORT";
            assertEquals(expectedPlan,QueryUtil.getExplainPlan(rs));
        } finally {
            destroyTables();
        }
    }

    @Test
    public void testIndexWithNullableFixedWithCols() throws Exception {
    	Properties props = new Properties(TEST_PROPERTIES);
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.setAutoCommit(false);
    	try {
            ensureTableCreated(getUrl(), INDEX_DATA_TABLE);
            populateTestTable();
            String ddl = "CREATE INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE
                    + " (char_col1 ASC, int_col1 ASC)"
                    + " INCLUDE (long_col1, long_col2)";
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            String query = "SELECT char_col1, int_col1 from " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE;
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER INDEX_TEST.IDX", QueryUtil.getExplainPlan(rs));
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
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
}
