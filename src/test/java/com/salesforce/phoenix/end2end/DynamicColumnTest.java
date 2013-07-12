/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc. All rights reserved. Redistribution and use in source and binary forms, with
 * or without modification, are permitted provided that the following conditions are met: Redistributions of source code
 * must retain the above copyright notice, this list of conditions and the following disclaimer. Redistributions in
 * binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution. Neither the name of Salesforce.com nor the names
 * of its contributors may be used to endorse or promote products derived from this software without specific prior
 * written permission. THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/

package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.*;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.*;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.ColumnAlreadyExistsException;
import com.salesforce.phoenix.schema.ColumnFamilyNotFoundException;
import com.salesforce.phoenix.util.SchemaUtil;

/**
 * Basic tests for Phoenix dynamic upserting
 * 
 * @author nmaillard
 * @since 1.3
 */

public class DynamicColumnTest extends BaseClientMangedTimeTest {
    private static final byte[] HBASE_DYNAMIC_COLUMNS_BYTES = SchemaUtil.getTableName(Bytes
            .toBytes(HBASE_DYNAMIC_COLUMNS));
    private static final byte[] FAMILY_NAME = Bytes.toBytes(SchemaUtil.normalizeIdentifier("A"));
    private static final byte[] FAMILY_NAME2 = Bytes.toBytes(SchemaUtil.normalizeIdentifier("B"));

    @BeforeClass
    public static void doBeforeTestSetup() throws Exception {
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES).getAdmin();
        try {
            try {
                admin.disableTable(HBASE_DYNAMIC_COLUMNS_BYTES);
                admin.deleteTable(HBASE_DYNAMIC_COLUMNS_BYTES);
            } catch (org.apache.hadoop.hbase.TableNotFoundException e) {}
            ensureTableCreated(getUrl(), HBASE_DYNAMIC_COLUMNS);
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
            put.add(FAMILY_NAME, f1v2, Bytes.toBytes("f1value2"));
            put.add(FAMILY_NAME2, f2v1, Bytes.toBytes("f2value1"));
            put.add(FAMILY_NAME2, f2v2, Bytes.toBytes("f2value2"));
            mutations.add(put);

            hTable.batch(mutations);

        } finally {
            hTable.close();
        }
        // Create Phoenix table after HBase table was created through the native APIs
        // The timestamp of the table creation must be later than the timestamp of the data
        ensureTableCreated(getUrl(), HBASE_DYNAMIC_COLUMNS);
    }

    /**
     * Test a simple select with a dynamic Column
     */
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

    /**
     * Test a select with a colum family.column dynamic Column
     */
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

    /**
     * Test a select with a colum family.column dynamic Column and check the value
     */

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

    /**
     * Test an upsert of prexisting schema defined columns and dynamic ones with different datatypes
     */
    @Test(expected = ColumnAlreadyExistsException.class)
    public void testAmbiguousStaticSelect() throws Exception {
        String upsertquery = "Select * FROM HBASE_DYNAMIC_COLUMNS(A.F1V1 INTEGER)";
        String url = PHOENIX_JDBC_URL + ";";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            statement.executeQuery();
        } finally {
            conn.close();
        }
    }

    /**
     * Test a select of an undefined ColumnFamily dynamic columns
     */
    @Test(expected = ColumnFamilyNotFoundException.class)
    public void testFakeCFDynamicUpsert() throws Exception {
        String upsertquery = "Select * FROM HBASE_DYNAMIC_COLUMNS(fakecf.DynCol VARCHAR)";
        String url = PHOENIX_JDBC_URL + ";";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            statement.executeQuery();
        } finally {
            conn.close();
        }
    }

}
