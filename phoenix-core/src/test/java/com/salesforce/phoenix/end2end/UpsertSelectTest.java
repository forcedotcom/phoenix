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

import static com.salesforce.phoenix.util.PhoenixRuntime.UPSERT_BATCH_SIZE_ATTRIB;
import static com.salesforce.phoenix.util.TestUtil.A_VALUE;
import static com.salesforce.phoenix.util.TestUtil.B_VALUE;
import static com.salesforce.phoenix.util.TestUtil.CUSTOM_ENTITY_DATA_FULL_NAME;
import static com.salesforce.phoenix.util.TestUtil.C_VALUE;
import static com.salesforce.phoenix.util.TestUtil.PHOENIX_JDBC_URL;
import static com.salesforce.phoenix.util.TestUtil.PTSDB_NAME;
import static com.salesforce.phoenix.util.TestUtil.ROW6;
import static com.salesforce.phoenix.util.TestUtil.ROW7;
import static com.salesforce.phoenix.util.TestUtil.ROW8;
import static com.salesforce.phoenix.util.TestUtil.ROW9;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.PhoenixRuntime;
import com.salesforce.phoenix.util.QueryUtil;
import com.salesforce.phoenix.util.TestUtil;


public class UpsertSelectTest extends BaseClientMangedTimeTest {
    
    @Test
    public void testUpsertSelectWithNoIndex() throws Exception {
        testUpsertSelect(false);
    }
    
    @Test
    public void testUpsertSelecWithIndex() throws Exception {
        testUpsertSelect(true);
    }
    
    private void testUpsertSelect(boolean createIndex) throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts-1);
        ensureTableCreated(PHOENIX_JDBC_URL, CUSTOM_ENTITY_DATA_FULL_NAME, ts-1);
        String indexName = "IDX1";
        if (createIndex) {
            Properties props = new Properties();
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts)); // Execute at timestamp 1
            Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS " + indexName + " ON " + TestUtil.ATABLE_NAME + "(a_string)" );
            conn.close();
        }
        PreparedStatement upsertStmt;
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        props.setProperty(UPSERT_BATCH_SIZE_ATTRIB, Integer.toString(3)); // Trigger multiple batches
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.setAutoCommit(true);
        String upsert = "UPSERT INTO " + CUSTOM_ENTITY_DATA_FULL_NAME + "(custom_entity_data_id, key_prefix, organization_id, created_by) " +
            "SELECT substr(entity_id, 4), substr(entity_id, 1, 3), organization_id, a_string  FROM ATABLE WHERE a_string = ?";
        if (createIndex) { // Confirm index is used
            upsertStmt = conn.prepareStatement("EXPLAIN " + upsert);
            upsertStmt.setString(1, tenantId);
            ResultSet ers = upsertStmt.executeQuery();
            assertTrue(ers.next());
            String explainPlan = QueryUtil.getExplainPlan(ers);
            assertTrue(explainPlan.contains(" SCAN OVER " + indexName));
        }
        
        upsertStmt = conn.prepareStatement(upsert);
        upsertStmt.setString(1, A_VALUE);
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(4, rowsInserted);
        conn.commit();
        conn.close();
        
        String query = "SELECT key_prefix, substr(custom_entity_data_id, 1, 1), created_by FROM " + CUSTOM_ENTITY_DATA_FULL_NAME + " WHERE organization_id = ? ";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 3)); // Execute at timestamp 3
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        ResultSet rs = statement.executeQuery();
        
        assertTrue (rs.next());
        assertEquals("00A", rs.getString(1));
        assertEquals("1", rs.getString(2));
        assertEquals(A_VALUE, rs.getString(3));
        
        assertTrue (rs.next());
        assertEquals("00A", rs.getString(1));
        assertEquals("2", rs.getString(2));
        assertEquals(A_VALUE, rs.getString(3));
        
        assertTrue (rs.next());
        assertEquals("00A", rs.getString(1));
        assertEquals("3", rs.getString(2));
        assertEquals(A_VALUE, rs.getString(3));
        
        assertTrue (rs.next());
        assertEquals("00A", rs.getString(1));
        assertEquals("4", rs.getString(2));
        assertEquals(A_VALUE, rs.getString(3));

        assertFalse(rs.next());
        conn.close();

        // Test UPSERT through coprocessor
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.setAutoCommit(true);
        upsert = "UPSERT INTO " + CUSTOM_ENTITY_DATA_FULL_NAME + "(custom_entity_data_id, key_prefix, organization_id, last_update_by, division) " +
            "SELECT custom_entity_data_id, key_prefix, organization_id, created_by, 1.0  FROM " + CUSTOM_ENTITY_DATA_FULL_NAME + " WHERE organization_id = ? and created_by >= 'a'";
        
        upsertStmt = conn.prepareStatement(upsert);
        upsertStmt.setString(1, tenantId);
        assertEquals(4, upsertStmt.executeUpdate());
        conn.commit();

        query = "SELECT key_prefix, substr(custom_entity_data_id, 1, 1), created_by, last_update_by, division FROM " + CUSTOM_ENTITY_DATA_FULL_NAME + " WHERE organization_id = ?";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 5)); 
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        rs = statement.executeQuery();
       
        assertTrue (rs.next());
        assertEquals("00A", rs.getString(1));
        assertEquals("1", rs.getString(2));
        assertEquals(A_VALUE, rs.getString(3));
        assertEquals(A_VALUE, rs.getString(4));
        assertTrue(BigDecimal.valueOf(1.0).compareTo(rs.getBigDecimal(5)) == 0);
        
        assertTrue (rs.next());
        assertEquals("00A", rs.getString(1));
        assertEquals("2", rs.getString(2));
        assertEquals(A_VALUE, rs.getString(3));
        assertEquals(A_VALUE, rs.getString(4));
        assertTrue(BigDecimal.valueOf(1.0).compareTo(rs.getBigDecimal(5)) == 0);
        
        assertTrue (rs.next());
        assertEquals("00A", rs.getString(1));
        assertEquals("3", rs.getString(2));
        assertEquals(A_VALUE, rs.getString(3));
        assertEquals(A_VALUE, rs.getString(4));
        assertTrue(BigDecimal.valueOf(1.0).compareTo(rs.getBigDecimal(5)) == 0);
        
        assertTrue (rs.next());
        assertEquals("00A", rs.getString(1));
        assertEquals("4", rs.getString(2));
        assertEquals(A_VALUE, rs.getString(3));
        assertEquals(A_VALUE, rs.getString(4));
        assertTrue(BigDecimal.valueOf(1.0).compareTo(rs.getBigDecimal(5)) == 0);

        assertFalse(rs.next());
        conn.close();
    }

    // TODO: more tests - nullable fixed length last PK column
    @Test
    public void testUpsertSelectEmptyPKColumn() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts-1);
        ensureTableCreated(PHOENIX_JDBC_URL, PTSDB_NAME, ts-1);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1)); // Execute at timestamp 1
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.setAutoCommit(false);
        String upsert = "UPSERT INTO " + PTSDB_NAME + "(date, val, host) " +
            "SELECT current_date(), x_integer+2, entity_id FROM ATABLE WHERE a_integer >= ?";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        upsertStmt.setInt(1, 6);
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(4, rowsInserted);
        conn.commit();
        conn.close();
        
        String query = "SELECT inst,host,date,val FROM " + PTSDB_NAME;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        PreparedStatement statement = conn.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        
        Date now = new Date(System.currentTimeMillis());
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW6, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertEquals(null, rs.getBigDecimal(4));
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW7, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(7).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW8, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(6).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW9, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(5).compareTo(rs.getBigDecimal(4)) == 0);

        assertFalse(rs.next());
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 3));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.setAutoCommit(true);
        upsert = "UPSERT INTO " + PTSDB_NAME + "(date, val, inst) " +
            "SELECT date+1, val*10, host FROM " + PTSDB_NAME;
        upsertStmt = conn.prepareStatement(upsert);
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(4, rowsInserted);
        conn.commit();
        conn.close();
        
        Date then = new Date(now.getTime() + QueryConstants.MILLIS_IN_DAY);
        query = "SELECT host,inst, date,val FROM " + PTSDB_NAME + " where inst is not null";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4)); // Execute at timestamp 2
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        statement = conn.prepareStatement(query);
        
        rs = statement.executeQuery();
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW6, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertEquals(null, rs.getBigDecimal(4));
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW7, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertTrue(BigDecimal.valueOf(70).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW8, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertTrue(BigDecimal.valueOf(60).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW9, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertTrue(BigDecimal.valueOf(50).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertFalse(rs.next());
        conn.close();
        
        // Should just update all values with the same value, essentially just updating the timestamp
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.setAutoCommit(true);
        upsert = "UPSERT INTO " + PTSDB_NAME + " SELECT * FROM " + PTSDB_NAME;
        upsertStmt = conn.prepareStatement(upsert);
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(8, rowsInserted);
        conn.commit();
        conn.close();
        
        query = "SELECT * FROM " + PTSDB_NAME ;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4)); // Execute at timestamp 2
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        statement = conn.prepareStatement(query);
        
        rs = statement.executeQuery();
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW6, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertEquals(null, rs.getBigDecimal(4));
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW7, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(7).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW8, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(6).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW9, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(5).compareTo(rs.getBigDecimal(4)) == 0);

        assertTrue (rs.next());
        assertEquals(ROW6, rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertEquals(null, rs.getBigDecimal(4));
        
        assertTrue (rs.next());
        assertEquals(ROW7, rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertTrue(BigDecimal.valueOf(70).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(ROW8, rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertTrue(BigDecimal.valueOf(60).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(ROW9, rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertTrue(BigDecimal.valueOf(50).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testUpsertSelectForAggAutoCommit() throws Exception {
        testUpsertSelectForAgg(true);
    }
    
    @Test
    public void testUpsertSelectForAgg() throws Exception {
        testUpsertSelectForAgg(false);
    }
    
    private void testUpsertSelectForAgg(boolean autoCommit) throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts-1);
        ensureTableCreated(PHOENIX_JDBC_URL, PTSDB_NAME, ts-1);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1)); // Execute at timestamp 1
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.setAutoCommit(autoCommit);
        String upsert = "UPSERT INTO " + PTSDB_NAME + "(date, val, host) " +
            "SELECT current_date(), sum(a_integer), a_string FROM ATABLE GROUP BY a_string";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(3, rowsInserted);
        if (!autoCommit) {
            conn.commit();
        }
        conn.close();
        
        String query = "SELECT inst,host,date,val FROM " + PTSDB_NAME;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        PreparedStatement statement = conn.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        Date now = new Date(System.currentTimeMillis());
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(A_VALUE, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(10).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(B_VALUE, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(26).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(C_VALUE, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(9).compareTo(rs.getBigDecimal(4)) == 0);
        assertFalse(rs.next());
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 3));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.setAutoCommit(true);
        upsert = "UPSERT INTO " + PTSDB_NAME + "(date, val, host, inst) " +
            "SELECT current_date(), max(val), max(host), 'x' FROM " + PTSDB_NAME;
        upsertStmt = conn.prepareStatement(upsert);
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        if (!autoCommit) {
            conn.commit();
        }
        conn.close();
        
        query = "SELECT inst,host,date,val FROM " + PTSDB_NAME + " WHERE inst='x'";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        statement = conn.prepareStatement(query);
        rs = statement.executeQuery();
        now = new Date(System.currentTimeMillis());
        
        assertTrue (rs.next());
        assertEquals("x", rs.getString(1));
        assertEquals(C_VALUE, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(26).compareTo(rs.getBigDecimal(4)) == 0);
        assertFalse(rs.next());
        
    }

    @Test
    public void testUpsertSelectLongToInt() throws Exception {
        byte[][] splits = new byte[][] {PDataType.INTEGER.toBytes(1), PDataType.INTEGER.toBytes(2),
                PDataType.INTEGER.toBytes(3), PDataType.INTEGER.toBytes(4)};
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),"IntKeyTest",splits, ts-2);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        String upsert = "UPSERT INTO IntKeyTest VALUES(1)";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 5));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        upsert = "UPSERT INTO IntKeyTest select i+1 from IntKeyTest";
        upsertStmt = conn.prepareStatement(upsert);
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        String select = "SELECT i FROM IntKeyTest";
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2,rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testUpsertSelectRunOnServer() throws Exception {
        byte[][] splits = new byte[][] {PDataType.INTEGER.toBytes(1), PDataType.INTEGER.toBytes(2),
                PDataType.INTEGER.toBytes(3), PDataType.INTEGER.toBytes(4)};
        long ts = nextTimestamp();
        createTestTable(getUrl(), "create table IntKeyTest (i integer not null primary key desc, j integer)" ,splits, ts-2);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        String upsert = "UPSERT INTO IntKeyTest VALUES(1, 1)";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 3));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        String select = "SELECT i,j+1 FROM IntKeyTest";
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertEquals(2,rs.getInt(2));
        assertFalse(rs.next());
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 5));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.setAutoCommit(true); // Force to run on server side.
        upsert = "UPSERT INTO IntKeyTest(i,j) select i, j+1 from IntKeyTest";
        upsertStmt = conn.prepareStatement(upsert);
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        select = "SELECT j FROM IntKeyTest";
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(2,rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 15));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.setAutoCommit(true); // Force to run on server side.
        upsert = "UPSERT INTO IntKeyTest(i,j) select i, i from IntKeyTest";
        upsertStmt = conn.prepareStatement(upsert);
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        select = "SELECT j FROM IntKeyTest";
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testUpsertSelectOnDescToAsc() throws Exception {
        byte[][] splits = new byte[][] {PDataType.INTEGER.toBytes(1), PDataType.INTEGER.toBytes(2),
                PDataType.INTEGER.toBytes(3), PDataType.INTEGER.toBytes(4)};
        long ts = nextTimestamp();
        createTestTable(getUrl(), "create table IntKeyTest (i integer not null primary key desc, j integer)" ,splits, ts-2);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        String upsert = "UPSERT INTO IntKeyTest VALUES(1, 1)";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 5));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.setAutoCommit(true); // Force to run on server side.
        upsert = "UPSERT INTO IntKeyTest(i,j) select i+1, j+1 from IntKeyTest";
        upsertStmt = conn.prepareStatement(upsert);
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        String select = "SELECT i,j FROM IntKeyTest";
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(2,rs.getInt(1));
        assertEquals(2,rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertEquals(1,rs.getInt(2));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testUpsertSelectRowKeyMutationOnSplitedTable() throws Exception {
        byte[][] splits = new byte[][] {PDataType.INTEGER.toBytes(1), PDataType.INTEGER.toBytes(2),
                PDataType.INTEGER.toBytes(3), PDataType.INTEGER.toBytes(4)};
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),"IntKeyTest",splits,ts-2);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        String upsert = "UPSERT INTO IntKeyTest VALUES(?)";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        upsertStmt.setInt(1, 1);
        upsertStmt.executeUpdate();
        upsertStmt.setInt(1, 3);
        upsertStmt.executeUpdate();
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 5));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        // Normally this would force a server side update. But since this changes the PK column, it would
        // for to run on the client side.
        conn.setAutoCommit(true);
        upsert = "UPSERT INTO IntKeyTest(i) SELECT i+1 from IntKeyTest";
        upsertStmt = conn.prepareStatement(upsert);
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(2, rowsInserted);
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        String select = "SELECT i FROM IntKeyTest";
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertEquals(4,rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
    }
    
    @Test
    public void testUpsertSelectWithLimit() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.createStatement().execute("create table phoenix_test (id varchar(10) not null primary key, val varchar(10), ts timestamp)");
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.createStatement().execute("upsert into phoenix_test values ('aaa', 'abc', current_date())");
        conn.createStatement().execute("upsert into phoenix_test values ('bbb', 'bcd', current_date())");
        conn.createStatement().execute("upsert into phoenix_test values ('ccc', 'cde', current_date())");
        conn.commit();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        ResultSet rs = conn.createStatement().executeQuery("select * from phoenix_test");
        
        assertTrue(rs.next());
        assertEquals("aaa",rs.getString(1));
        assertEquals("abc",rs.getString(2));
        assertNotNull(rs.getDate(3));
        
        assertTrue(rs.next());
        assertEquals("bbb",rs.getString(1));
        assertEquals("bcd",rs.getString(2));
        assertNotNull(rs.getDate(3));
        
        assertTrue(rs.next());
        assertEquals("ccc",rs.getString(1));
        assertEquals("cde",rs.getString(2));
        assertNotNull(rs.getDate(3));
        
        assertFalse(rs.next());
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.createStatement().execute("upsert into phoenix_test (id, ts) select id, null from phoenix_test where id <= 'bbb' limit 1");
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        rs = conn.createStatement().executeQuery("select * from phoenix_test");
        
        assertTrue(rs.next());
        assertEquals("aaa",rs.getString(1));
        assertEquals("abc",rs.getString(2));
        assertNull(rs.getDate(3));
        
        assertTrue(rs.next());
        assertEquals("bbb",rs.getString(1));
        assertEquals("bcd",rs.getString(2));
        assertNotNull(rs.getDate(3));
        
        assertTrue(rs.next());
        assertEquals("ccc",rs.getString(1));
        assertEquals("cde",rs.getString(2));
        assertNotNull(rs.getDate(3));
        
        assertFalse(rs.next());
        conn.close();

    }
}
