package com.salesforce.phoenix.end2end.index;

import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.salesforce.phoenix.end2end.BaseHBaseManagedTimeTest;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.util.QueryUtil;
import com.salesforce.phoenix.util.ReadOnlyProps;
import com.salesforce.phoenix.util.SchemaUtil;


public class MutableIndexTest extends BaseHBaseManagedTimeTest{
    private static final int TABLE_SPLITS = 3;
    private static final int INDEX_SPLITS = 4;
    public static final String SCHEMA_NAME = "";
    public static final String DATA_TABLE_NAME = "T";
    public static final String INDEX_TABLE_NAME = "I";
    private static final String DATA_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "T");
    private static final String INDEX_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "I");
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
        // Don't cache meta information for this test because the splits change between tests
        props.put(QueryServices.REGION_BOUNDARY_CACHE_TTL_MS_ATTRIB, Integer.toString(0));
        // Must update config before starting server
        startServer(getUrl(), new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    private static void createTestTable() throws SQLException {
        String ddl = "create table if not exists " + DATA_TABLE_FULL_NAME + "(" +
                "   varchar_pk VARCHAR NOT NULL, " +
                "   char_pk CHAR(5) NOT NULL, " +
                "   int_pk INTEGER NOT NULL, "+ 
                "   long_pk BIGINT NOT NULL, " +
                "   decimal_pk DECIMAL(31, 10) NOT NULL, " +
                "   a.varchar_col1 VARCHAR, " +
                "   a.char_col1 CHAR(5), " +
                "   a.int_col1 INTEGER, " +
                "   a.long_col1 BIGINT, " +
                "   a.decimal_col1 DECIMAL(31, 10), " +
                "   b.varchar_col2 VARCHAR, " +
                "   b.char_col2 CHAR(5), " +
                "   b.int_col2 INTEGER, " +
                "   b.long_col2 BIGINT, " +
                "   b.decimal_col2 DECIMAL(31, 10) " +
                "   CONSTRAINT pk PRIMARY KEY (varchar_pk, char_pk, int_pk, long_pk DESC, decimal_pk))";
            Properties props = new Properties(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute(ddl);
            conn.close();
    }
    
    // Populate the test table with data.
    private static void populateTestTable() throws SQLException {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String upsert = "UPSERT INTO " + DATA_TABLE_FULL_NAME
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
    
    @Before
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
    public void testMutableTableIndexMaintanenceSaltedSalted() throws Exception {
        testMutableTableIndexMaintanence(TABLE_SPLITS, INDEX_SPLITS);
    }

    @Test
    public void testMutableTableIndexMaintanenceSalted() throws Exception {
        testMutableTableIndexMaintanence(null, INDEX_SPLITS);
    }

    @Test
    public void testMutableTableIndexMaintanenceUnsalted() throws Exception {
        testMutableTableIndexMaintanence(null, null);
    }

    private void testMutableTableIndexMaintanence(Integer tableSaltBuckets, Integer indexSaltBuckets) throws Exception {
        String query;
        ResultSet rs;
        
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        conn.createStatement().execute("CREATE TABLE " + DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR)  " +  (tableSaltBuckets == null ? "" : " SALT_BUCKETS=" + tableSaltBuckets));
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
        
        conn.createStatement().execute("CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v DESC)" + (indexSaltBuckets == null ? "" : " SALT_BUCKETS=" + indexSaltBuckets));
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?)");
        stmt.setString(1,"a");
        stmt.setString(2, "x");
        stmt.execute();
        stmt.setString(1,"b");
        stmt.setString(2, "y");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("y",rs.getString(1));
        assertEquals("b",rs.getString(2));
        assertTrue(rs.next());
        assertEquals("x",rs.getString(1));
        assertEquals("a",rs.getString(2));
        assertFalse(rs.next());

        query = "SELECT k,v FROM " + DATA_TABLE_FULL_NAME + " WHERE v = 'y'";
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertEquals("y",rs.getString(2));
        assertFalse(rs.next());
        
        String expectedPlan;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        expectedPlan = indexSaltBuckets == null ? 
             "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + INDEX_TABLE_FULL_NAME + " 'y'" : 
            ("CLIENT PARALLEL 4-WAY SKIP SCAN ON 4 KEYS OVER " + INDEX_TABLE_FULL_NAME + " 0...3,'y'\n" + 
             "CLIENT MERGE SORT");
        assertEquals(expectedPlan,QueryUtil.getExplainPlan(rs));

        // Will use index, so rows returned in DESC order.
        // This is not a bug, though, because we can
        // return in any order.
        query = "SELECT k,v FROM " + DATA_TABLE_FULL_NAME + " WHERE v >= 'x'";
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
            "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + INDEX_TABLE_FULL_NAME + " (*-'x']" :
            ("CLIENT PARALLEL 4-WAY SKIP SCAN ON 4 RANGES OVER " + INDEX_TABLE_FULL_NAME + " 0...3,(*-'x']\n" + 
             "CLIENT MERGE SORT");
        assertEquals(expectedPlan,QueryUtil.getExplainPlan(rs));
        
        // Will still use index, since there's no LIMIT clause
        query = "SELECT k,v FROM " + DATA_TABLE_FULL_NAME + " WHERE v >= 'x' ORDER BY k";
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
            ("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + INDEX_TABLE_FULL_NAME + " (*-'x']\n" + 
             "    SERVER TOP -1 ROWS SORTED BY [:K]\n" + 
             "CLIENT MERGE SORT") :
            ("CLIENT PARALLEL 4-WAY SKIP SCAN ON 4 RANGES OVER " + INDEX_TABLE_FULL_NAME + " 0...3,(*-'x']\n" + 
             "    SERVER TOP -1 ROWS SORTED BY [:K]\n" + 
             "CLIENT MERGE SORT");
        assertEquals(expectedPlan,QueryUtil.getExplainPlan(rs));
        
        // Will use data table now, since there's a LIMIT clause and
        // we're able to optimize out the ORDER BY, unless the data
        // table is salted.
        query = "SELECT k,v FROM " + DATA_TABLE_FULL_NAME + " WHERE v >= 'x' ORDER BY k LIMIT 2";
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
             "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + DATA_TABLE_FULL_NAME + "\n" +
             "    SERVER FILTER BY V >= 'x'\n" + 
             "    SERVER 2 ROW LIMIT\n" + 
             "CLIENT 2 ROW LIMIT" :
             "CLIENT PARALLEL 4-WAY SKIP SCAN ON 4 RANGES OVER " + INDEX_TABLE_FULL_NAME + " 0...3,(*-'x']\n" + 
             "    SERVER TOP 2 ROWS SORTED BY [:K]\n" + 
             "CLIENT MERGE SORT";
        assertEquals(expectedPlan,QueryUtil.getExplainPlan(rs));
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
}
