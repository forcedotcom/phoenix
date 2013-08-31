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

import static com.salesforce.phoenix.util.TestUtil.*;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.salesforce.phoenix.jdbc.PhoenixStatement;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.schema.ConstraintViolationException;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.PhoenixRuntime;



/**
 * 
 * Basic tests for Phoenix JDBC implementation
 *
 * @author jtaylor
 * @since 0.1
 */
public class QueryExecTest extends BaseClientMangedTimeTest {

    @Test
    public void testScan() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_string, /* comment ok? */ b_string FROM aTable WHERE ?=organization_id and 5=a_integer";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertEquals(rs.getString("B_string"), C_VALUE);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanByByteValue() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_string, /* comment ok? */ b_string, a_byte FROM aTable WHERE ?=organization_id and 1=a_byte";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertEquals(rs.getByte(3), 1);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanByShortValue() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_string, /* comment ok? */ b_string, a_short FROM aTable WHERE ?=organization_id and 128=a_short";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertEquals(rs.getShort("a_short"), 128);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanByFloatValue() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_string, /* comment ok? */ b_string, a_float FROM aTable WHERE ?=organization_id and ?=a_float";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setFloat(2, 0.01f);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertTrue(Floats.compare(rs.getFloat(3), 0.01f) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanByUnsignedFloatValue() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_string, /* comment ok? */ b_string, a_unsigned_float FROM aTable WHERE ?=organization_id and ?=a_unsigned_float";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setFloat(2, 0.01f);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertTrue(Floats.compare(rs.getFloat(3), 0.01f) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanByDoubleValue() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_string, /* comment ok? */ b_string, a_double FROM aTable WHERE ?=organization_id and ?=a_double";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setDouble(2, 0.0001);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertTrue(Doubles.compare(rs.getDouble(3), 0.0001) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanByUnsigned_DoubleValue() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_string, /* comment ok? */ b_string, a_unsigned_double FROM aTable WHERE ?=organization_id and ?=a_unsigned_double";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setDouble(2, 0.0001);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertTrue(Doubles.compare(rs.getDouble(3), 0.0001) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testAllScan() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT ALL a_string, /* comment ok? */ b_string FROM aTable WHERE ?=organization_id and 5=a_integer";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertEquals(rs.getString("B_string"), C_VALUE);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDistinctScan() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT DISTINCT a_string FROM aTable WHERE organization_id=?";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), C_VALUE);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testInListSkipScan() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id, b_string FROM aTable WHERE organization_id=? and entity_id IN (?,?)";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, ROW2);
            statement.setString(3, ROW4);
            ResultSet rs = statement.executeQuery();
            Set<String> expectedvals = new HashSet<String>();
            expectedvals.add(ROW2+"_"+C_VALUE);
            expectedvals.add(ROW4+"_"+B_VALUE);
            Set<String> vals = new HashSet<String>();
            assertTrue (rs.next());
            vals.add(rs.getString(1) + "_" + rs.getString(2));
            assertTrue (rs.next());
            vals.add(rs.getString(1) + "_" + rs.getString(2));
            assertFalse(rs.next());
            assertEquals(expectedvals, vals);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testNotInList() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable WHERE organization_id=? and entity_id NOT IN (?,?,?,?,?,?)";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, ROW2);
            statement.setString(3, ROW4);
            statement.setString(4, ROW1);
            statement.setString(5, ROW5);
            statement.setString(6, ROW7);
            statement.setString(7, ROW8);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW3, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW6, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSumDouble() throws Exception {
        long ts = nextTimestamp();
        initSumDoubleValues(null, ts);
        String query = "SELECT SUM(d) FROM SumDoubleTest";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.015)==0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSumUnsignedDouble() throws Exception {
        long ts = nextTimestamp();
        initSumDoubleValues(null, ts);
        String query = "SELECT SUM(ud) FROM SumDoubleTest";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.015)==0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSumFloat() throws Exception {
        long ts = nextTimestamp();
        initSumDoubleValues(null, ts);
        String query = "SELECT SUM(f) FROM SumDoubleTest";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Floats.compare(rs.getFloat(1), 0.15f)==0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSumUnsignedFloat() throws Exception {
        long ts = nextTimestamp();
        initSumDoubleValues(null, ts);
        String query = "SELECT SUM(uf) FROM SumDoubleTest";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Floats.compare(rs.getFloat(1), 0.15f)==0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNotInListOfFloat() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_float FROM aTable WHERE organization_id=? and a_float NOT IN (?,?,?,?,?,?)";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setFloat(2, 0.01f);
            statement.setFloat(3, 0.02f);
            statement.setFloat(4, 0.03f);
            statement.setFloat(5, 0.04f);
            statement.setFloat(6, 0.05f);
            statement.setFloat(7, 0.06f);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.07f)==0);
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.08f)==0);
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.09f)==0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNotInListOfDouble() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_double FROM aTable WHERE organization_id=? and a_double NOT IN (?,?,?,?,?,?)";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setDouble(2, 0.0001);
            statement.setDouble(3, 0.0002);
            statement.setDouble(4, 0.0003);
            statement.setDouble(5, 0.0004);
            statement.setDouble(6, 0.0005);
            statement.setDouble(7, 0.0006);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.0007)==0);
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.0008)==0);
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.0009)==0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testGroupByPlusOne() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_integer+1 FROM aTable WHERE organization_id=? and a_integer = 5 GROUP BY a_integer+1";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(6, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNoWhereScan() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT y_integer FROM aTable";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            for (int i =0; i < 8; i++) {
                assertTrue (rs.next());
                assertEquals(0, rs.getInt(1));
                assertTrue(rs.wasNull());
            }
            assertTrue (rs.next());
            assertEquals(300, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testToDateOnString() throws Exception { // TODO: test more conversion combinations
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_string FROM aTable WHERE organization_id=? and a_integer = 5";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            rs.getDate(1);
            fail();
        } catch (ConstraintViolationException e) { // Expected
        } finally {
            conn.close();
        }
    }

    @Test
    public void testNotEquals() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id -- and here comment\n" + 
        "FROM aTable WHERE organization_id=? and a_integer != 1 and a_integer <= 2";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNotEqualsByTinyInt() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_byte -- and here comment\n" + 
        "FROM aTable WHERE organization_id=? and a_byte != 1 and a_byte <= 2";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getByte(1), 2);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNotEqualsBySmallInt() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_short -- and here comment\n" + 
        "FROM aTable WHERE organization_id=? and a_short != 128 and a_short !=0 and a_short <= 129";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getShort(1), 129);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNotEqualsByFloat() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_float -- and here comment\n" + 
        "FROM aTable WHERE organization_id=? and a_float != 0.01d and a_float <= 0.02d";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Floats.compare(rs.getFloat(1), 0.02f) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNotEqualsByUnsignedFloat() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_unsigned_float -- and here comment\n" + 
        "FROM aTable WHERE organization_id=? and a_unsigned_float != 0.01d and a_unsigned_float <= 0.02d";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Floats.compare(rs.getFloat(1), 0.02f) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNotEqualsByDouble() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_double -- and here comment\n" + 
        "FROM aTable WHERE organization_id=? and a_double != 0.0001d and a_double <= 0.0002d";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.0002) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNotEqualsByUnsignedDouble() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_unsigned_double -- and here comment\n" + 
        "FROM aTable WHERE organization_id=? and a_unsigned_double != 0.0001d and a_unsigned_double <= 0.0002d";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.0002) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testNotEquals2() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM // one more comment  \n" +
        "aTable WHERE organization_id=? and not a_integer = 1 and a_integer <= 2";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testColumnOnBothSides() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable WHERE organization_id=? and a_string = b_string";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW7);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    public void testNoStringValue(String value) throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1);
        Properties props = new Properties(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        PreparedStatement stmt = upsertConn.prepareStatement(
                "upsert into ATABLE VALUES (?, ?, ?)"); // without specifying columns
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW5);
        stmt.setString(3, value);
        stmt.execute(); // should commit too
        upsertConn.close();
        
        String query = "SELECT a_string, b_string FROM aTable WHERE organization_id=? and a_integer = 5";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(null, rs.getString(1));
            assertTrue(rs.wasNull());
            assertEquals(rs.getString("B_string"), C_VALUE);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testNullStringValue() throws Exception {
        testNoStringValue(null);
    }
    
    @Test
    public void testEmptyStringValue() throws Exception {
        testNoStringValue("");
    }

    @Test
    public void testPointInTimeScan() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        // Override value that was set at creation time
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        String upsertStmt =
            "upsert into " +
            "ATABLE(" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_INTEGER) " +
            "VALUES (?, ?, ?)";
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        PreparedStatement stmt = upsertConn.prepareStatement(upsertStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW4);
        stmt.setInt(3, 5);
        stmt.execute(); // should commit too
        upsertConn.close();

        // Override value again, but should be ignored since it's past the SCN
        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 3); // Run query at timestamp 5
        upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        stmt = upsertConn.prepareStatement(upsertStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW4);
        stmt.setInt(3, 9);
        stmt.execute(); // should commit too
        upsertConn.close();
        
        String query = "SELECT organization_id, a_string AS a FROM atable WHERE organization_id=? and a_integer = 5";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(tenantId, rs.getString(1));
        assertEquals(A_VALUE, rs.getString("a"));
        assertTrue(rs.next());
        assertEquals(tenantId, rs.getString(1));
        assertEquals(B_VALUE, rs.getString(2));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testPointInTimeLimitedScan() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        // Override value that was set at creation time
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        String upsertStmt =
            "upsert into " +
            "ATABLE(" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_INTEGER) " +
            "VALUES (?, ?, ?)";
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        PreparedStatement stmt = upsertConn.prepareStatement(upsertStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW1);
        stmt.setInt(3, 6);
        stmt.execute(); // should commit too
        upsertConn.close();

        // Override value again, but should be ignored since it's past the SCN
        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 3);
        upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        stmt = upsertConn.prepareStatement(upsertStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW1);
        stmt.setInt(3, 0);
        stmt.execute(); // should commit too
        upsertConn.close();
        
        String query = "SELECT a_integer,b_string FROM atable WHERE organization_id=? and a_integer <= 5 limit 2";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(C_VALUE, rs.getString(2));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals(E_VALUE, rs.getString(2));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testUpperLowerBoundRangeScan() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable WHERE organization_id=? and substr(entity_id,1,3) > '00A' and substr(entity_id,1,3) < '00C'";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW5);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW6);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW7);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpperBoundRangeScan() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable WHERE organization_id=? and substr(entity_id,1,3) >= '00B' ";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW5);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW6);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW7);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW9);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testLowerBoundRangeScan() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable WHERE organization_id=? and substr(entity_id,1,3) < '00B' ";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW1);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW3);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW4);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUnboundRangeScan1() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable WHERE organization_id <= ?";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW1);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW3);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW4);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW5);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW6);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW7);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW9);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testUnboundRangeScan2() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable WHERE organization_id >= ?";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW1);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW3);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW4);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW5);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW6);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW7);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW9);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    // FIXME: this is flapping with an phoenix.memory.InsufficientMemoryException
    // in the GroupedAggregateRegionObserver. We can work around it by increasing
    // the amount of available memory in QueryServicesTestImpl, but we shouldn't
    // have to. I think something may no be being closed to reclaim the memory.
    @Test
    public void testGroupedAggregation() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_string, count(1), 'foo' FROM atable WHERE organization_id=? GROUP BY a_string";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getLong(2), 4L);
            assertEquals(rs.getString(3), "foo");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertEquals(rs.getLong(2), 4L);
            assertEquals(rs.getString(3), "foo");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), C_VALUE);
            assertEquals(rs.getLong(2), 1L);
            assertEquals(rs.getString(3), "foo");
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDistinctGroupedAggregation() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT DISTINCT a_string, count(1), 'foo' FROM atable WHERE organization_id=? GROUP BY a_string, b_string ORDER BY a_string, count(1)";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            
            assertTrue(rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getLong(2), 1L);
            assertEquals(rs.getString(3), "foo");
            
            assertTrue(rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getLong(2), 2L);
            assertEquals(rs.getString(3), "foo");
            
            assertTrue(rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertEquals(rs.getLong(2), 1L);
            assertEquals(rs.getString(3), "foo");
            
            assertTrue(rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertEquals(rs.getLong(2), 2L);
            assertEquals(rs.getString(3), "foo");
            
            assertTrue(rs.next());
            assertEquals(rs.getString(1), C_VALUE);
            assertEquals(rs.getLong(2), 1L);
            assertEquals(rs.getString(3), "foo");
            
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDistinctLimitedGroupedAggregation() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT DISTINCT a_string, count(1), 'foo' FROM atable WHERE organization_id=? GROUP BY a_string, b_string ORDER BY count(1) desc,a_string LIMIT 2";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            
            assertTrue(rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getLong(2), 2L);
            assertEquals(rs.getString(3), "foo");
            
            assertTrue(rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertEquals(rs.getLong(2), 2L);
            assertEquals(rs.getString(3), "foo");
            
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDistinctUngroupedAggregation() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT DISTINCT count(1), 'foo' FROM atable WHERE organization_id=?";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            
            assertTrue(rs.next());
            assertEquals(9L, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testGroupedLimitedAggregation() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_string, count(1) FROM atable WHERE organization_id=? GROUP BY a_string LIMIT 2";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(4L, rs.getLong(2));
            assertTrue(rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertEquals(4L, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPointInTimeGroupedAggregation() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String updateStmt = 
            "upsert into " +
            "ATABLE VALUES ('" + tenantId + "','" + ROW5 + "','" + C_VALUE +"')";
        // Override value that was set at creation time
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        Statement stmt = upsertConn.createStatement();
        stmt.execute(updateStmt); // should commit too
        upsertConn.close();

        // Override value again, but should be ignored since it's past the SCN
        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 3); // Run query at timestamp 5
        upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        updateStmt = 
            "upsert into " +
            "ATABLE VALUES (?, ?, ?)";
        // Insert all rows at ts
        PreparedStatement pstmt = upsertConn.prepareStatement(updateStmt);
        pstmt.setString(1, tenantId);
        pstmt.setString(2, ROW5);
        pstmt.setString(3, E_VALUE);
        pstmt.execute(); // should commit too
        upsertConn.close();
        
        String query = "SELECT a_string, count(1) FROM atable WHERE organization_id='" + tenantId + "' GROUP BY a_string";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(A_VALUE, rs.getString(1));
        assertEquals(4, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(B_VALUE, rs.getString(1));
        assertEquals(3, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals(C_VALUE, rs.getString(1));
        assertEquals(2, rs.getInt(2));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testUngroupedAggregation() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT count(1) FROM atable WHERE organization_id=? and a_string = ?";
        String url = PHOENIX_JDBC_URL;
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 5)); // Execute query at ts + 5
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, B_VALUE);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(4, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
        // Run again to catch unintentianal deletion of rows during an ungrouped aggregation (W-1455633)
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 6)); // Execute at ts + 6
        conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, B_VALUE);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(4, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUngroupedAggregationNoWhere() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT count(*) FROM atable";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(9, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPointInTimeUngroupedAggregation() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        // Override value that was set at creation time
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        String updateStmt = 
            "upsert into " +
            "ATABLE(" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_STRING) " +
            "VALUES (?, ?, ?)";
        // Insert all rows at ts
        PreparedStatement stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW5);
        stmt.setString(3, null);
        stmt.execute();
        stmt.setString(3, C_VALUE);
        stmt.execute();
        stmt.setString(2, ROW7);
        stmt.setString(3, E_VALUE);
        stmt.execute();
        upsertConn.commit();
        upsertConn.close();

        // Override value again, but should be ignored since it's past the SCN
        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 3); // Run query at timestamp 5
        upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW6);
        stmt.setString(3, E_VALUE);
        stmt.execute();
        upsertConn.close();
        
        String query = "SELECT count(1) FROM atable WHERE organization_id=? and a_string = ?";
        // Specify CurrentSCN on URL with extra stuff afterwards (which should be ignored)
        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 2) + ";foo=bar"; // Run query at timestamp 2 
        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        statement.setString(2, B_VALUE);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getLong(1));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testPointInTimeUngroupedLimitedAggregation() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String updateStmt = 
            "upsert into " +
            "ATABLE(" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_STRING) " +
            "VALUES (?, ?, ?)";
        // Override value that was set at creation time
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        PreparedStatement stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW6);
        stmt.setString(3, C_VALUE);
        stmt.execute();
        stmt.setString(3, E_VALUE);
        stmt.execute();
        stmt.setString(3, B_VALUE);
        stmt.execute();
        stmt.setString(3, B_VALUE);
        stmt.execute();
        upsertConn.close();

        // Override value again, but should be ignored since it's past the SCN
        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 3); // Run query at timestamp 5
        upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW6);
        stmt.setString(3, E_VALUE);
        stmt.execute();
        upsertConn.close();

        String query = "SELECT count(1) FROM atable WHERE organization_id=? and a_string = ? LIMIT 3";
        // Specify CurrentSCN on URL with extra stuff afterwards (which should be ignored)
        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 2) + ";foo=bar"; // Run query at timestamp 2 
        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        statement.setString(2, B_VALUE);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(4, rs.getLong(1)); // LIMIT applied at end, so all rows would be counted
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testPointInTimeDeleteUngroupedAggregation() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String updateStmt = 
            "upsert into " +
            "ATABLE(" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_STRING) " +
            "VALUES (?, ?, ?)";
        
        // Override value that was set at creation time
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);

        // Remove column value at ts + 1 (i.e. equivalent to setting the value to null)
        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement stmt = conn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW7);
        stmt.setString(3, null);
        stmt.execute();
        
        // Delete row 
        stmt = conn.prepareStatement("delete from atable where organization_id=? and entity_id=?");
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW5);
        stmt.execute();
        conn.commit();
        conn.close();
        
        // Delete row at timestamp 3. This should not be seen by the query executing
        // Remove column value at ts + 1 (i.e. equivalent to setting the value to null)
        Connection futureConn = DriverManager.getConnection(PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 3), props);
        stmt = futureConn.prepareStatement("delete from atable where organization_id=? and entity_id=?");
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW6);
        stmt.execute();
        futureConn.commit();
        futureConn.close();

        String query = "SELECT count(1) FROM atable WHERE organization_id=? and a_string = ?";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        statement.setString(2, B_VALUE);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getLong(1));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testIntFilter() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String updateStmt = 
            "upsert into " +
            "ATABLE(" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_INTEGER) " +
            "VALUES (?, ?, ?)";
        // Override value that was set at creation time
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1);
        Properties props = new Properties(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        PreparedStatement stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW4);
        stmt.setInt(3, -10);
        stmt.execute();
        upsertConn.close();

        String query = "SELECT entity_id FROM aTable WHERE organization_id=? and a_integer >= ?";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        statement.setInt(2, 7);
        ResultSet rs = statement.executeQuery();
        assertTrue (rs.next());
        assertEquals(rs.getString(1), ROW7);
        assertTrue (rs.next());
        assertEquals(rs.getString(1), ROW8);
        assertTrue (rs.next());
        assertEquals(rs.getString(1), ROW9);
        assertFalse(rs.next());

        query = "SELECT entity_id FROM aTable WHERE organization_id=? and a_integer < 2";
        statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        rs = statement.executeQuery();
        assertTrue (rs.next());
        assertEquals(rs.getString(1), ROW1);
        assertTrue (rs.next());
        assertEquals(rs.getString(1), ROW4);
        assertFalse(rs.next());

        query = "SELECT entity_id FROM aTable WHERE organization_id=? and a_integer <= 2";
        statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        rs = statement.executeQuery();
        assertTrue (rs.next());
        assertEquals(rs.getString(1), ROW1);
        assertTrue (rs.next());
        assertEquals(rs.getString(1), ROW2);
        assertTrue (rs.next());
        assertEquals(rs.getString(1), ROW4);
        assertFalse(rs.next());

        query = "SELECT entity_id FROM aTable WHERE organization_id=? and a_integer >=9";
        statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        rs = statement.executeQuery();
        assertTrue (rs.next());
        assertEquals(rs.getString(1), ROW9);
        assertFalse(rs.next());
        conn.close();
    }
    

    private static boolean compare(CompareOp op, ImmutableBytesWritable lhsOutPtr, ImmutableBytesWritable rhsOutPtr) {
        int compareResult = Bytes.compareTo(lhsOutPtr.get(), lhsOutPtr.getOffset(), lhsOutPtr.getLength(), rhsOutPtr.get(), rhsOutPtr.getOffset(), rhsOutPtr.getLength());
        return ByteUtil.compare(op, compareResult);
    }
    
    @Test
    public void testDateAdd() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), new Date(System.currentTimeMillis()), ts);
        String query = "SELECT entity_id, b_string FROM ATABLE WHERE a_date + 0.5d < ?";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setDate(1, new Date(System.currentTimeMillis() + MILLIS_IN_DAY));
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW1, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW4, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW7, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDateSubtract() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), new Date(System.currentTimeMillis()), ts);
        String query = "SELECT entity_id, b_string FROM ATABLE WHERE a_date - 0.5d > ?";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setDate(1, new Date(System.currentTimeMillis() + MILLIS_IN_DAY));
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW3, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW6, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTimestamp() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String updateStmt = 
            "upsert into " +
            "ATABLE(" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_TIMESTAMP) " +
            "VALUES (?, ?, ?)";
        // Override value that was set at creation time
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1);
        Properties props = new Properties(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        PreparedStatement stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW4);
        Timestamp tsValue1 = new Timestamp(5000);
        byte[] ts1 = PDataType.TIMESTAMP.toBytes(tsValue1);
        stmt.setTimestamp(3, tsValue1);
        stmt.execute();

        updateStmt = 
            "upsert into " +
            "ATABLE(" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_TIMESTAMP," +
            "    A_TIME) " +
            "VALUES (?, ?, ?, ?)";
        stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW5);
        Timestamp tsValue2 = new Timestamp(5000);
        tsValue2.setNanos(200);
        byte[] ts2 = PDataType.TIMESTAMP.toBytes(tsValue2);
        stmt.setTimestamp(3, tsValue2);
        stmt.setTime(4, new Time(tsValue2.getTime()));
        stmt.execute();
        upsertConn.close();
        
        assertTrue(compare(CompareOp.GREATER, new ImmutableBytesWritable(ts2), new ImmutableBytesWritable(ts1)));
        assertFalse(compare(CompareOp.GREATER, new ImmutableBytesWritable(ts1), new ImmutableBytesWritable(ts1)));

        String query = "SELECT entity_id, a_timestamp, a_time FROM aTable WHERE organization_id=? and a_timestamp > ?";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 3)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setTimestamp(2, new Timestamp(5000));
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW5);
            assertEquals(rs.getTimestamp("A_TIMESTAMP"), tsValue2);
            assertEquals(rs.getTime("A_TIME"), new Time(tsValue2.getTime()));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCoerceTinyIntToSmallInt() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT entity_id FROM ATABLE WHERE organization_id=? AND a_byte >= a_short";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCoerceIntegerToLong() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT entity_id FROM ATABLE WHERE organization_id=? AND x_long >= x_integer";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW7, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCoerceLongToDecimal1() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT entity_id FROM ATABLE WHERE organization_id=? AND x_decimal > x_integer";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCoerceLongToDecimal2() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT entity_id FROM ATABLE WHERE organization_id=? AND x_integer <= x_decimal";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSimpleCaseStatement() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT CASE a_integer WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN 'c' ELSE 'd' END AS a FROM ATABLE WHERE organization_id=? AND a_integer < 6";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("d", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("d", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testMultiCondCaseStatement() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT CASE WHEN a_integer <= 2 THEN 1.5 WHEN a_integer = 3 THEN 2 WHEN a_integer <= 6 THEN 4.5 ELSE 5 END AS a FROM ATABLE WHERE organization_id=?";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(1.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(1.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(2), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(4.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(4.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(4.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(5), rs.getBigDecimal(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testPartialEvalCaseStatement() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT entity_id FROM ATABLE WHERE organization_id=? and CASE WHEN 1234 = a_integer THEN 1 WHEN x_integer = 5 THEN 2 ELSE 3 END = 2";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW7, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testFoundIndexOnPartialEvalCaseStatement() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT entity_id FROM ATABLE WHERE organization_id=? and CASE WHEN a_integer = 1234 THEN 1 WHEN x_integer = 3 THEN y_integer ELSE 3 END = 300";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    // TODO: we need some tests that have multiple versions of key values
    @Test
    public void testUnfoundMultiColumnCaseStatement() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT entity_id, b_string FROM ATABLE WHERE organization_id=? and CASE WHEN a_integer = 1234 THEN 1 WHEN a_date < ? THEN y_integer WHEN x_integer = 4 THEN 4 ELSE 3 END = 4";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setDate(2, new Date(System.currentTimeMillis()));
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testUnfoundSingleColumnCaseStatement() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT entity_id, b_string FROM ATABLE WHERE organization_id=? and CASE WHEN a_integer = 0 or a_integer != 0 THEN 1 ELSE 0 END = 0";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        // Set ROW5.A_INTEGER to null so that we have one row
        // where the else clause of the CASE statement will
        // fire.
        url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1); // Run query at timestamp 5
        Connection upsertConn = DriverManager.getConnection(url, props);
        String upsertStmt =
            "upsert into " +
            "ATABLE(" +
            "    ENTITY_ID, " +
            "    ORGANIZATION_ID, " +
            "    A_INTEGER) " +
            "VALUES ('" + ROW5 + "','" + tenantId + "', null)";
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        PreparedStatement stmt = upsertConn.prepareStatement(upsertStmt);
        stmt.execute(); // should commit too
        upsertConn.close();
        
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(ROW5, rs.getString(1));
        assertFalse(rs.next());
        conn.close();
    }
    
    @Test
    public void testNonNullMultiCondCaseStatement() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT CASE WHEN entity_id = '000000000000000' THEN 1 WHEN entity_id = '000000000000001' THEN 2 ELSE 3 END FROM ATABLE WHERE organization_id=?";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            ResultSetMetaData rsm = rs.getMetaData();
            assertEquals(ResultSetMetaData.columnNoNulls,rsm.isNullable(1));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNullMultiCondCaseStatement() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT CASE WHEN entity_id = '000000000000000' THEN 1 WHEN entity_id = '000000000000001' THEN 2 END FROM ATABLE WHERE organization_id=?";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            ResultSetMetaData rsm = rs.getMetaData();
            assertEquals(ResultSetMetaData.columnNullable,rsm.isNullable(1));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNullabilityMultiCondCaseStatement() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT CASE WHEN a_integer <= 2 THEN ? WHEN a_integer = 3 THEN ? WHEN a_integer <= ? THEN ? ELSE 5 END AS a FROM ATABLE WHERE organization_id=?";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setBigDecimal(1,BigDecimal.valueOf(1.5));
            statement.setInt(2,2);
            statement.setInt(3,6);
            statement.setBigDecimal(4,BigDecimal.valueOf(4.5));
            statement.setString(5, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(1.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(1.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(2), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(4.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(4.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(4.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(5), rs.getBigDecimal(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSimpleInListStatement() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT entity_id FROM ATABLE WHERE organization_id=? AND a_integer IN (2,4)";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW2, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW4, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    /**
     * Test to repro Null Pointer Exception
     * @throws Exception
     */
    @Test
    public void testInFilterOnKey() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT count(entity_id) FROM ATABLE WHERE organization_id IN (?,?)";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(9, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testOneInListStatement() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT entity_id FROM ATABLE WHERE organization_id=? AND b_string IN (?)";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, E_VALUE);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW3, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW6, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    
    @Test
    public void testMixedTypeInListStatement() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT entity_id FROM ATABLE WHERE organization_id=? AND x_long IN (5, ?)";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            long l = Integer.MAX_VALUE + 1L;
            statement.setLong(2, l);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW7, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testIsNull() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable WHERE X_DECIMAL is null";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW1);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW3);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW4);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW5);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW6);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCountIsNull() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT count(1) FROM aTable WHERE X_DECIMAL is null";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(6, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCountIsNotNull() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT count(1) FROM aTable WHERE X_DECIMAL is not null";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(3, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testIsNotNull() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable WHERE X_DECIMAL is not null";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW7);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW9);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testIntSubtractionExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where A_INTEGER - 4  <= 0";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW1);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW3);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW4);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    @Test
    public void testDecimalSubtraction1Expression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where A_INTEGER - 3.5  <= 0";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW1, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW2, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW3, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    @Test
    public void testDecimalSubtraction2Expression() throws Exception {// check if decimal part makes a difference
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where X_DECIMAL - 3.5  > 0";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    @Test
    public void testLongSubtractionExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where X_LONG - 1  < 0";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    @Test
    public void testDoubleSubtractionExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where a_double - 0.0002d  < 0";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW1);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    @Test
    public void testSmallIntSubtractionExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where a_short - 129  = 0";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testTernarySubtractionExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where  X_INTEGER - X_LONG - 10  < 0";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW7);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW9);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    @Test
    public void testSelectWithSubtractionExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id, x_integer - 4 FROM aTable where  x_integer - 4 = 0";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertEquals(rs.getInt(2), 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    @Test
    public void testConstantSubtractionExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where A_INTEGER = 5 - 1 - 2";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testIntDivideExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where A_INTEGER / 3 > 2";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDoubleDivideExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where a_double / 3.0d = 0.0003";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSmallIntDivideExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where a_short / 135 = 1";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testIntToDecimalDivideExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where A_INTEGER / 3.0 > 2";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW7, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testConstantDivideExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where A_INTEGER = 9 / 3 / 3";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW1);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    @Test
    public void testSelectWithDivideExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id, a_integer/3 FROM aTable where  a_integer = 9";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNegateExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where A_INTEGER - 4 = -1";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW3, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testIntMultiplyExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where A_INTEGER * 2 = 16";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDoubleMultiplyExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where A_DOUBLE * 2.0d = 0.0002";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW1, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testLongMultiplyExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where X_LONG * 2 * 2 = 20";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW7, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testIntToDecimalMultiplyExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where A_INTEGER * 1.5 > 9";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW7, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    

    @Test
    public void testDecimalMultiplyExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where X_DECIMAL * A_INTEGER > 29.5";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testIntAddExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where A_INTEGER + 2 = 4";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW2, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDecimalAddExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where A_INTEGER + X_DECIMAL > 11";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDoubleAddExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where a_double + a_float > 0.08";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testUnsignedDoubleAddExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable where a_unsigned_double + a_unsigned_float > 0.08";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="RV_RETURN_VALUE_IGNORED",
            justification="Test code.")
    @Test
    public void testValidArithmetic() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String[] queries = new String[] { 
                "SELECT entity_id,organization_id FROM atable where (A_DATE - A_DATE) * 5 < 0",
                "SELECT entity_id,organization_id FROM atable where 1 + A_DATE  < A_DATE",
                "SELECT entity_id,organization_id FROM atable where A_DATE - 1 < A_DATE",
                "SELECT entity_id,organization_id FROM atable where A_INTEGER - 45 < 0",
                "SELECT entity_id,organization_id FROM atable where X_DECIMAL / 45 < 0", };

        for (String query : queries) {
            Properties props = new Properties();
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
            Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.executeQuery();
            }
            finally {
                conn.close();
            }
        }
    }
    
    @Test
    public void testValidStringConcatExpression() throws Exception {//test fails with stack overflow wee
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        int counter=0;
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String[] answers = new String[]{"00D300000000XHP5bar","a5bar","5bar","15bar","5bar","5bar"};
        String[] queries = new String[] { 
        		"SELECT  organization_id || 5 || 'bar' FROM atable limit 1",
        		"SELECT a_string || 5 || 'bar' FROM atable limit 1",
        		"SELECT a_date||5||'bar' FROM atable limit 1",
        		"SELECT a_integer||5||'bar' FROM atable limit 1",
        		"SELECT x_decimal||5||'bar' FROM atable limit 1",
        		"SELECT x_long||5||'bar' FROM atable limit 1"
        };

        for (String query : queries) {
        	Properties props = new Properties();
        	props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        	Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        	try {
        		PreparedStatement statement = conn.prepareStatement(query);
        		ResultSet rs=statement.executeQuery();
        		assertTrue(rs.next());
        		assertEquals(answers[counter++],rs.getString(1));
           		assertFalse(rs.next());
        	}
        	finally {
        		conn.close();
        	}
        }
    }
    
    @Test
    public void testStartKeyStopKey() throws SQLException {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE start_stop_test (pk char(2) not null primary key) SPLIT ON ('EA','EZ')");
        conn.close();
        
        String query = "select count(*) from start_stop_test where pk >= 'EA' and pk < 'EZ'";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        conn = DriverManager.getConnection(getUrl(), props);
        Statement statement = conn.createStatement();
        statement.execute(query);
        PhoenixStatement pstatement = statement.unwrap(PhoenixStatement.class);
        List<KeyRange>splits = pstatement.getQueryPlan().getSplits();
        assertTrue(splits.size() > 0);
    }
    
    @Test
    public void testRowKeySingleIn() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable WHERE organization_id=? and entity_id IN (?,?,?)";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, ROW2);
            statement.setString(3, ROW6);
            statement.setString(4, ROW8);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW6);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    
    @Test
    public void testRowKeyMultiIn() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT entity_id FROM aTable WHERE organization_id=? and entity_id IN (?,?,?) and a_string IN (?,?)";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, ROW2);
            statement.setString(3, ROW6);
            statement.setString(4, ROW9);
            statement.setString(5, B_VALUE);
            statement.setString(6, C_VALUE);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW6);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW9);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
