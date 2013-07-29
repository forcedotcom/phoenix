package com.salesforce.phoenix.end2end.index;

import static com.salesforce.phoenix.util.TestUtil.INDEX_DATA_SCHEMA;
import static com.salesforce.phoenix.util.TestUtil.INDEX_DATA_TABLE;
import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.end2end.BaseHBaseManagedTimeTest;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.PIndexState;
import com.salesforce.phoenix.schema.PTableType;
import com.salesforce.phoenix.util.StringUtil;
import com.salesforce.phoenix.util.TestUtil;


public class IndexMetadataTest extends BaseHBaseManagedTimeTest{

	private enum Order {ASC, DESC};
	
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

    private static void assertIndexInfoMetadata(ResultSet rs, String schemaName, String dataTableName, String indexName, int colPos, String colName, Order order, int type) throws SQLException {
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
        assertEquals(type,rs.getInt(14));
    }
	
    @Test
    public void testIndexCreation() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            ensureTableCreated(getUrl(), INDEX_DATA_TABLE);
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
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX");
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
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX");
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
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX1");
            assertFalse(rs.next());
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX2");
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testIndexDefinitionWithNullableFixedWidthColInPK() throws Exception {
    	// If we have nullable fixed width column in the PK, we convert those types into a compatible variable type
    	// column. The definition is defined in IndexUtil.getIndexColumnDataType.
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
            
            // Verify the CHAR, INT and LONG are converted to right type.
            ResultSet rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, false, false);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 1, "A:CHAR_COL1", Order.ASC, Types.VARCHAR);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 2, "B:INT_COL2", Order.ASC, Types.DECIMAL);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 3, "B:LONG_COL2", Order.DESC, Types.DECIMAL);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 4, "VARCHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 5, "CHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 6, "INT_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 7, "LONG_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 8, "DECIMAL_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 9, "A:INT_COL1", null);
            assertFalse(rs.next());
            
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX");
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
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX");
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testIndexDefinitionWithRepeatedColumns() throws Exception {
    	// Test index creation when the columns is included in both the PRIMARY and INCLUDE section. Test de-duplication.
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            ensureTableCreated(getUrl(), TestUtil.INDEX_DATA_TABLE);
            String ddl = "CREATE INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE
            		+ " (a.int_col1, a.long_col1, b.int_col2, b.long_col2)"
            		+ " INCLUDE(int_col1, int_col2)";
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            fail("Should have caught exception.");
        } catch (SQLException e) {
        	assertTrue(e.getMessage(), e.getMessage().contains("ERROR 503 (42711): A duplicate column name was detected in the object definition or ALTER TABLE statement."));
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
            String ddl = "create table test_table (char_pk varchar not null,"
            		+ " a.int_col integer, a.long_col integer,"
            		+ " b.int_col integer, b.long_col integer"
            		+ " constraint pk primary key (char_pk))";
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            ddl = "CREATE INDEX IDX ON test_table (int_col ASC, long_cal ASC)";
            stmt = conn.prepareStatement(ddl);
            stmt.execute();
            fail("Should have caught exception.");
        } catch (SQLException e) {
        	assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1023 (42Y82): Index may only be created on a table with immutable rows."));
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
