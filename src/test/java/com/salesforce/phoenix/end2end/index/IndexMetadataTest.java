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
package com.salesforce.phoenix.end2end.index;

import static com.salesforce.phoenix.util.TestUtil.INDEX_DATA_SCHEMA;
import static com.salesforce.phoenix.util.TestUtil.INDEX_DATA_TABLE;
import static com.salesforce.phoenix.util.TestUtil.MUTABLE_INDEX_DATA_TABLE;
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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Test;

import com.salesforce.phoenix.end2end.BaseHBaseManagedTimeTest;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.AmbiguousColumnException;
import com.salesforce.phoenix.schema.PIndexState;
import com.salesforce.phoenix.schema.PTableType;
import com.salesforce.phoenix.util.SchemaUtil;
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
	
    private static void assertActiveIndex(Connection conn, String schemaName, String tableName) throws SQLException {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        conn.createStatement().executeQuery("SELECT count(*) FROM " + fullTableName).next(); // client side cache will update
        conn.unwrap(PhoenixConnection.class).getPMetaData().getTable(fullTableName).getIndexMaintainers(ptr);
        assertTrue(ptr.getLength() > 0);
    }
    
    private static void assertNoActiveIndex(Connection conn, String schemaName, String tableName) throws SQLException {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        conn.createStatement().executeQuery("SELECT count(*) FROM " + fullTableName).next(); // client side cache will update
        conn.unwrap(PhoenixConnection.class).getPMetaData().getTable(fullTableName).getIndexMaintainers(ptr);
        assertTrue(ptr.getLength() == 0);
    }
    
    @Test
    public void testIndexCreation() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            ensureTableCreated(getUrl(), MUTABLE_INDEX_DATA_TABLE);
            String ddl = "CREATE INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE
                    + " (varchar_col1 ASC, varchar_col2 ASC, int_pk DESC)"
                    + " INCLUDE (int_col1, int_col2)";
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            // Verify the metadata for index is correct.
            ResultSet rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, false, false);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 1, "A:VARCHAR_COL1", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 2, "B:VARCHAR_COL2", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 3, ":INT_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 4, ":VARCHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 5, ":CHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 6, ":LONG_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 7, ":DECIMAL_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 8, "A:INT_COL1", null);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 9, "B:INT_COL2", null);
            assertFalse(rs.next());
            
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), StringUtil.escapeLike("IDX"), new String[] {PTableType.INDEX.getValue().getString() });
            assertTrue(rs.next());
            assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));

            // Verify that there is a row inserted into the data table for the index table.
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX");
            assertTrue(rs.next());
            assertEquals("IDX", rs.getString(1));
            assertFalse(rs.next());
            
            assertActiveIndex(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE);
            
            ddl = "ALTER INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE + " UNUSABLE";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), "IDX", new String[] {PTableType.INDEX.toString()});
            assertTrue(rs.next());
            assertEquals("IDX", rs.getString(3));
            assertEquals(PIndexState.INACTIVE.toString(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());
            
            assertActiveIndex(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE);

            ddl = "ALTER INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE + " USABLE";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), "IDX", new String[] {PTableType.INDEX.toString()});
            assertTrue(rs.next());
            assertEquals("IDX", rs.getString(3));
            assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());
            
            assertActiveIndex(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE);

            ddl = "ALTER INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE + " DISABLE";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), "IDX", new String[] {PTableType.INDEX.toString()});
            assertTrue(rs.next());
            assertEquals("IDX", rs.getString(3));
            assertEquals(PIndexState.DISABLE.toString(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());
            
            assertNoActiveIndex(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE);

            try {
                ddl = "ALTER INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE + " USABLE";
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.INVALID_INDEX_STATE_TRANSITION.getErrorCode(), e.getErrorCode());
            }
            try {
                ddl = "ALTER INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE + " UNUSABLE";
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.INVALID_INDEX_STATE_TRANSITION.getErrorCode(), e.getErrorCode());
            }
            
            ddl = "ALTER INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE + " REBUILD";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), "IDX", new String[] {PTableType.INDEX.toString()});
            assertTrue(rs.next());
            assertEquals("IDX", rs.getString(3));
            assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());
            
            assertActiveIndex(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE);

            ddl = "DROP INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE;
            stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            assertNoActiveIndex(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE);

           // Assert the rows for index table is completely removed.
            rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, false, false);
            assertFalse(rs.next());
            
            // Assert the row in the original data table is removed.
            // Verify that there is a row inserted into the data table for the index table.
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX");
            assertFalse(rs.next());
            
            // Create another two indexes, and drops the table, verifies the indexes are dropped as well.
            ddl = "CREATE INDEX IDX1 ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE
                    + " (varchar_col1 ASC, varchar_col2 ASC, int_pk DESC)"
                    + " INCLUDE (int_col1, int_col2)";
            stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            ddl = "CREATE INDEX IDX2 ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE
                    + " (varchar_col1 ASC, varchar_col2 ASC, int_pk DESC)"
                    + " INCLUDE (long_pk, int_col2)";
            stmt = conn.prepareStatement(ddl);
            stmt.execute();
            rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, false, false);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 1, "A:VARCHAR_COL1", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 2, "B:VARCHAR_COL2", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 3, ":INT_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 4, ":VARCHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 5, ":CHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 6, ":LONG_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 7, ":DECIMAL_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 8, "A:INT_COL1", null);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 9, "B:INT_COL2", null);

            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2", 1, "A:VARCHAR_COL1", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2", 2, "B:VARCHAR_COL2", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2", 3, ":INT_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2", 4, ":VARCHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2", 5, ":CHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2", 6, ":LONG_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2", 7, ":DECIMAL_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2", 8, "B:INT_COL2", null);
            assertFalse(rs.next());
            
            ddl = "DROP TABLE " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE;
            stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, false, false);
            assertFalse(rs.next());
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1");
            assertFalse(rs.next());
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2");
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
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 4, ":VARCHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 5, ":CHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 6, ":INT_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 7, ":LONG_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 8, ":DECIMAL_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 9, "A:INT_COL1", null);
            assertFalse(rs.next());
            
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX");
            assertTrue(rs.next());
            assertEquals("IDX", rs.getString(1));
            assertFalse(rs.next());
            
            ddl = "ALTER INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE + " UNUSABLE";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), "IDX", new String[] {PTableType.INDEX.toString()});
            assertTrue(rs.next());
            assertEquals("IDX", rs.getString(3));
            assertEquals(PIndexState.INACTIVE.toString(), rs.getString("INDEX_STATE"));
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
        String ddl = "create table test_table (char_pk varchar not null,"
        		+ " a.int_col integer, a.long_col integer,"
        		+ " b.int_col integer, b.long_col integer"
        		+ " constraint pk primary key (char_pk))";
        PreparedStatement stmt = conn.prepareStatement(ddl);
        stmt.execute();
        
        ddl = "CREATE INDEX IDX1 ON test_table (a.int_col, b.int_col)";
        stmt = conn.prepareStatement(ddl);
        stmt.execute();
        try {
            ddl = "CREATE INDEX IDX2 ON test_table (int_col)";
            stmt = conn.prepareStatement(ddl);
            stmt.execute();
            fail("Should have caught exception");
        } catch (AmbiguousColumnException e) {
            assertEquals(SQLExceptionCode.AMBIGUOUS_COLUMN.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }
}
