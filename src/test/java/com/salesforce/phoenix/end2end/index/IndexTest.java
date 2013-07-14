package com.salesforce.phoenix.end2end.index;

import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.*;
import static com.salesforce.phoenix.util.TestUtil.*;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.end2end.BaseHBaseManagedTimeTest;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.PIndexState;
import com.salesforce.phoenix.schema.PTableType;
import com.salesforce.phoenix.util.*;


public class IndexTest extends BaseHBaseManagedTimeTest{

    private enum Order {ASC, DESC};
    
    // the normal db metadata interface is insufficient for all fields needed for an
    // index table test.
    private static final String SELECT_DATA_INDEX_ROW = "SELECT " + TABLE_CAT_NAME
            + " FROM "
            + TYPE_SCHEMA + ".\"" + TYPE_TABLE
            + "\" WHERE "
            + TABLE_SCHEM_NAME + "=? AND " + TABLE_NAME_NAME + "=? AND " + COLUMN_NAME + " IS NULL AND " + TABLE_CAT_NAME + "=?";

    private static ResultSet readDataTableIndexRow(Connection conn, String schemaName, String tableName, String indexName) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SELECT_DATA_INDEX_ROW);
        stmt.setString(1, schemaName);
        stmt.setString(2, tableName);
        stmt.setString(3, indexName);
        return stmt.executeQuery();
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

    @Test
    public void testIndexCreation() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            ensureTableCreated(getUrl(), INDEX_DATA_TABLE);
            populateTestTable();
            String ddl = "CREATE INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE
                    + " (varchar_col1 ASC, varchar_col2 ASC, int_pk DESC)"
                    + " INCLUDE (int_col1, int_col2)";
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            // Verify the metadata for index is correct.
            ResultSet rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, false, false);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 1, "A:VARCHAR_COL1", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 2, "B:VARCHAR_COL2", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 3, "INT_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 4, "VARCHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 5, "CHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 6, "LONG_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 7, "DECIMAL_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 8, "A:INT_COL1", null);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 9, "B:INT_COL2", null);
            assertFalse(rs.next());
            
            // Verify that there is a row inserted into the data table for the index table.
            rs = readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX");
            assertTrue(rs.next());
            assertEquals("IDX", rs.getString(1));
            assertFalse(rs.next());
            
            ddl = "ALTER INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE + " DISABLE";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), "IDX", new String[] {PTableType.INDEX.getSerializedValue()});
            assertTrue(rs.next());
            assertEquals("IDX", rs.getString(3));
            assertEquals(PIndexState.INACTIVE.getSerializedValue(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());
            
            ddl = "DROP INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE;
            stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            // Assert the rows for index table is completely removed.
            rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, false, false);
            assertFalse(rs.next());
            
            // Assert the row in the original data table is removed.
            // Verify that there is a row inserted into the data table for the index table.
            rs = readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX");
            assertFalse(rs.next());
            
            // Create another two indexes, and drops the table, verifies the indexes are dropped as well.
            ddl = "CREATE INDEX IDX1 ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE
                    + " (varchar_col1 ASC, varchar_col2 ASC, int_pk DESC)"
                    + " INCLUDE (int_col1, int_col2)";
            stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            ddl = "CREATE INDEX IDX2 ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE
                    + " (varchar_col1 ASC, varchar_col2 ASC, int_pk DESC)"
                    + " INCLUDE (long_pk, int_col2)";
            stmt = conn.prepareStatement(ddl);
            stmt.execute();
            rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, false, false);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX1", 1, "A:VARCHAR_COL1", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX1", 2, "B:VARCHAR_COL2", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX1", 3, "INT_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX1", 4, "VARCHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX1", 5, "CHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX1", 6, "LONG_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX1", 7, "DECIMAL_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX1", 8, "A:INT_COL1", null);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX1", 9, "B:INT_COL2", null);

            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX2", 1, "A:VARCHAR_COL1", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX2", 2, "B:VARCHAR_COL2", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX2", 3, "INT_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX2", 4, "VARCHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX2", 5, "CHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX2", 6, "LONG_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX2", 7, "DECIMAL_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX2", 8, "B:INT_COL2", null);
            assertFalse(rs.next());
            
            ddl = "DROP TABLE " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE;
            stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, false, false);
            assertFalse(rs.next());
            rs = readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX1");
            assertFalse(rs.next());
            rs = readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX2");
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    private static void assertIndexInfoMetadata(ResultSet rs, String schemaName, String dataTableName, String indexName, int colPos, String colName, Order order) throws SQLException {
        assertTrue(rs.next());
        assertEquals(null,rs.getString(1));
        assertEquals(schemaName, rs.getString(2));
        assertEquals(dataTableName, rs.getString(3));
        assertEquals(Boolean.TRUE, rs.getBoolean(4));
        assertEquals(null,rs.getString(5));
        assertEquals(indexName, rs.getString(6));
        assertEquals(DatabaseMetaData.tableIndexOther, rs.getShort(7));
        assertEquals(colPos, rs.getShort(8));
        assertEquals(colName, rs.getString(9));
        assertEquals(order == Order.ASC ? "A" : order == Order.DESC ? "D" : null, rs.getString(10));
        assertEquals(0,rs.getInt(11));
        assertTrue(rs.wasNull());
        assertEquals(0,rs.getInt(12));
        assertTrue(rs.wasNull());
        assertEquals(null,rs.getString(13));
    }
    
    @Test
    public void testImmutableTableIndexMaintanence() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        conn.createStatement().execute("CREATE TABLE t (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX i ON t (v DESC)");
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?)");
        stmt.setString(1,"a");
        stmt.setString(2, "x");
        stmt.execute();
        stmt.setString(1,"b");
        stmt.setString(2, "y");
        stmt.execute();
        conn.commit();
        
        String query;
        ResultSet rs;
        
        query = "SELECT k,v FROM t WHERE v = 'y'";
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertEquals("y",rs.getString(2));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER I 'y'",QueryUtil.getExplainPlan(rs));

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
        assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER I (*-'x']",QueryUtil.getExplainPlan(rs));
        
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
        assertEquals(
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER I (*-'x']\n" + 
        		"    SERVER TOP -1 ROWS SORTED BY [K]\n" + 
        		"CLIENT MERGE SORT",
        		QueryUtil.getExplainPlan(rs));
        
        // Will use data table now, since there's a LIMIT clause and
        // we're able to optimize out the ORDER BY
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
        assertEquals(
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER T\n" + 
                "    SERVER FILTER BY V >= 'x'\n" + 
                "    SERVER 2 ROW LIMIT\n" + 
                "CLIENT 2 ROW LIMIT",
                QueryUtil.getExplainPlan(rs));
    }
    
    @Test
    public void testIndexDefinitionWithNullableFixedWidthColInPK() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            ensureTableCreated(getUrl(), INDEX_DATA_TABLE);
            String ddl = "CREATE INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE
                    + " (char_col1 ASC, int_col2 ASC, long_col2 DESC)"
                    + " INCLUDE (int_col1)";
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            
            
        } finally {
            conn.close();
        }
    }

    @Test
    public void testIndexDefinitionWithRepeatedColumns() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            ensureTableCreated(getUrl(), TestUtil.INDEX_DATA_TABLE);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testIndexDefinitionWithSameColumnNamesInTwoFamily() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            ensureTableCreated(getUrl(), TestUtil.INDEX_DATA_TABLE);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSaltedIndexDefinition() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            ensureTableCreated(getUrl(), TestUtil.INDEX_DATA_TABLE);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSetImmutableRowsOnExistingTable() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        conn.createStatement().execute("CREATE TABLE t (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR)");
        try {
            conn.createStatement().execute("CREATE INDEX i ON t (v DESC)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INDEX_ONLY_ON_IMMUTABLE_TABLE.getErrorCode(), e.getErrorCode());
        }
        conn.createStatement().execute("ALTER TABLE t SET IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX i ON t (v DESC)");
    }
}
