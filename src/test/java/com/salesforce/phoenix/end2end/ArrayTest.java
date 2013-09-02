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

import java.sql.*;
import java.util.Properties;

import org.junit.Test;

import com.google.common.primitives.Floats;
import com.salesforce.phoenix.query.BaseTest;
import com.salesforce.phoenix.schema.PhoenixArray;
import com.salesforce.phoenix.util.PhoenixRuntime;

public class ArrayTest extends BaseClientMangedTimeTest {
    
    @Test
    public void testScanByArrayValue() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(BaseConnectedQueryTest.getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts);
        String query = "SELECT a_double_array, /* comment ok? */ b_string, a_float FROM table_with_array WHERE ?=organization_id and ?=a_float";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setFloat(2, 0.01f);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            // Need to support primitive
            Double[] doubleArr =  new Double[2];
            doubleArr[0] = 25.343;
            doubleArr[1] = 36.763;
            PhoenixArray array = (PhoenixArray)conn.createArrayOf("DOUBLE", doubleArr);
            PhoenixArray resultArray = (PhoenixArray)rs.getArray(1);
            assertEquals(resultArray, array);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertTrue(Floats.compare(rs.getFloat(3), 0.01f) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanWithArrayInWhereClause() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(BaseConnectedQueryTest.getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts);
        String query = "SELECT a_double_array, /* comment ok? */ b_string, a_float FROM table_with_array WHERE ?=organization_id and ?=a_byte_array";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            // Need to support primitive
            Byte[] byteArr =  new Byte[2];
            byteArr[0] = 25;
            byteArr[1] = 36;
            Array array = conn.createArrayOf("TINYINT", byteArr);
            statement.setArray(2, array);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            // Need to support primitive
            Double[] doubleArr =  new Double[2];
            doubleArr[0] = 25.343;
            doubleArr[1] = 36.763;
            array = conn.createArrayOf("DOUBLE", doubleArr);
            PhoenixArray resultArray = (PhoenixArray)rs.getArray(1);
            assertEquals(resultArray, array);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertTrue(Floats.compare(rs.getFloat(3), 0.01f) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanWithNonFixedWidthArrayInWhereClause() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(BaseConnectedQueryTest.getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts);
        String query = "SELECT a_double_array, /* comment ok? */ b_string, a_float FROM table_with_array WHERE ?=organization_id and ?=a_string_array";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            // Need to support primitive
            String[] strArr =  new String[2];
            strArr[0] = "ABC";
            strArr[1] = "CEDF";
            Array array = conn.createArrayOf("VARCHAR", strArr);
            statement.setArray(2, array);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            // Need to support primitive
            Double[] doubleArr =  new Double[2];
            doubleArr[0] = 25.343;
            doubleArr[1] = 36.763;
            array = conn.createArrayOf("DOUBLE", doubleArr);
            PhoenixArray resultArray = (PhoenixArray)rs.getArray(1);
            assertEquals(resultArray, array);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertTrue(Floats.compare(rs.getFloat(3), 0.01f) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    static void createTableWithArray(String url, byte[][] bs, Object object, long ts) throws SQLException {
        String ddlStmt =  "create table " + TABLE_WITH_ARRAY +
                "   (organization_id char(15) not null, \n" +
                "    entity_id char(15) not null,\n" +
                "    a_string_array varchar array[],\n" +
                "    b_string varchar(100),\n" +
                "    a_integer integer,\n" +
                "    a_date date,\n" +
                "    a_time time,\n" +
                "    a_timestamp timestamp,\n" +
                "    x_decimal decimal(31,10),\n" +
                "    x_long_array bigint array[],\n" +
                "    x_integer integer,\n" +
                "    a_byte_array tinyint array[],\n" +
                "    a_short smallint,\n" +
                "    a_float float,\n" +
                "    a_double_array double array[],\n" +
                "    a_unsigned_float unsigned_float,\n" +
                "    a_unsigned_double unsigned_double \n" +
                "    CONSTRAINT pk PRIMARY KEY (organization_id, entity_id)\n" +
                ")";
        BaseTest.createTestTable(url, ddlStmt, bs, ts);
    }
}
