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

import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;

import com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData;
import com.salesforce.phoenix.query.BaseTest;
import com.salesforce.phoenix.schema.PTableType;
import com.salesforce.phoenix.util.*;


/**
 * 
 * Base class for tests that need to be connected to an HBase server
 *
 * @author jtaylor
 * @since 0.1
 */
public abstract class BaseConnectedQueryTest extends BaseTest {
    protected static byte[][] getDefaultSplits(String tenantId) {
        return new byte[][] { 
            Bytes.toBytes(tenantId + "00A"),
            Bytes.toBytes(tenantId + "00B"),
            Bytes.toBytes(tenantId + "00C"),
            };
    }
    
    protected static String getUrl() {
        return TestUtil.PHOENIX_JDBC_URL;
    }

    @BeforeClass
    public static void doSetup() throws Exception {
        startServer(getUrl());
    }
    
    protected static void deletePriorTables(long ts) throws Exception {
        Properties props = new Properties();
        if (ts != HConstants.LATEST_TIMESTAMP) {
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        }
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, null, new String[] {PTableType.USER.getSerializedValue(), PTableType.VIEW.getSerializedValue()});
            while (rs.next()) {
                String fullTableName = SchemaUtil.getTableDisplayName(
                        rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM_NAME),
                        rs.getString(PhoenixDatabaseMetaData.TABLE_NAME_NAME));
                conn.createStatement().executeUpdate("DROP " + (PTableType.fromSerializedValue(rs.getString(PhoenixDatabaseMetaData.TABLE_TYPE_NAME)) == PTableType.VIEW ? "VIEW " : "TABLE ") + fullTableName);
            }
        } finally {
            conn.close();
        }
    }
    
    protected static void initSumDoubleValues(byte[][] splits, Long ts) throws Exception {
        if (ts == null) {
            ensureTableCreated(getUrl(), "SumDoubleTest", splits);
        } else {
            ensureTableCreated(getUrl(), "SumDoubleTest", splits, ts-2);
        }
        Properties props = new Properties();
        if (ts != null) {
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, ts.toString());
        }
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " +
                    "SumDoubleTest(" +
                    "    id, " +
                    "    d, " +
                    "    f, " +
                    "    ud, " +
                    "    uf) " +
                    "VALUES (?, ?, ?, ?, ?)");
            stmt.setString(1, "1");
            stmt.setDouble(2, 0.001);
            stmt.setFloat(3, 0.01f);
            stmt.setDouble(4, 0.001);
            stmt.setFloat(5, 0.01f);
            stmt.execute();
                
            stmt.setString(1, "2");
            stmt.setDouble(2, 0.002);
            stmt.setFloat(3, 0.02f);
            stmt.setDouble(4, 0.002);
            stmt.setFloat(5, 0.02f);
            stmt.execute();
                
            stmt.setString(1, "3");
            stmt.setDouble(2, 0.003);
            stmt.setFloat(3, 0.03f);
            stmt.setDouble(4, 0.003);
            stmt.setFloat(5, 0.03f);
            stmt.execute();
                
            stmt.setString(1, "4");
            stmt.setDouble(2, 0.004);
            stmt.setFloat(3, 0.04f);
            stmt.setDouble(4, 0.004);
            stmt.setFloat(5, 0.04f);
            stmt.execute();
                
            stmt.setString(1, "5");
            stmt.setDouble(2, 0.005);
            stmt.setFloat(3, 0.05f);
            stmt.setDouble(4, 0.005);
            stmt.setFloat(5, 0.05f);
            stmt.execute();
                
            conn.commit();
        } finally {
            conn.close();
        }
    }
    
    protected static void initATableValues(String tenantId, byte[][] splits) throws Exception {
        initATableValues(tenantId, splits, null);
    }
    
    protected static void initATableValues(String tenantId, byte[][] splits, Date date) throws Exception {
        initATableValues(tenantId, splits, date, null);
    }
    
    protected static void initATableValues(String tenantId, byte[][] splits, Date date, Long ts) throws Exception {
        if (ts == null) {
            ensureTableCreated(getUrl(), ATABLE_NAME, splits);
        } else {
            ensureTableCreated(getUrl(), ATABLE_NAME, splits, ts-2);
        }
        
        Properties props = new Properties();
        if (ts != null) {
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, ts.toString());
        }
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " +
                    "ATABLE(" +
                    "    ORGANIZATION_ID, " +
                    "    ENTITY_ID, " +
                    "    A_STRING, " +
                    "    B_STRING, " +
                    "    A_INTEGER, " +
                    "    A_DATE, " +
                    "    X_DECIMAL, " +
                    "    X_LONG, " +
                    "    X_INTEGER," +
                    "    Y_INTEGER," +
                    "    A_BYTE," +
                    "    A_SHORT," +
                    "    A_FLOAT," +
                    "    A_DOUBLE," +
                    "    A_UNSIGNED_FLOAT," +
                    "    A_UNSIGNED_DOUBLE)" +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW1);
            stmt.setString(3, A_VALUE);
            stmt.setString(4, B_VALUE);
            stmt.setInt(5, 1);
            stmt.setDate(6, date);
            stmt.setBigDecimal(7, null);
            stmt.setNull(8, Types.BIGINT);
            stmt.setNull(9, Types.INTEGER);
            stmt.setNull(10, Types.INTEGER);
            stmt.setByte(11, (byte)1);
            stmt.setShort(12, (short) 128);
            stmt.setFloat(13, 0.01f);
            stmt.setDouble(14, 0.0001);
            stmt.setFloat(15, 0.01f);
            stmt.setDouble(16, 0.0001);
            stmt.execute();
                
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW2);
            stmt.setString(3, A_VALUE);
            stmt.setString(4, C_VALUE);
            stmt.setInt(5, 2);
            stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 1));
            stmt.setBigDecimal(7, null);
            stmt.setNull(8, Types.BIGINT);
            stmt.setNull(9, Types.INTEGER);
            stmt.setNull(10, Types.INTEGER);
            stmt.setByte(11, (byte)2);
            stmt.setShort(12, (short) 129);
            stmt.setFloat(13, 0.02f);
            stmt.setDouble(14, 0.0002);
            stmt.setFloat(15, 0.02f);
            stmt.setDouble(16, 0.0002);
            stmt.execute();
                
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW3);
            stmt.setString(3, A_VALUE);
            stmt.setString(4, E_VALUE);
            stmt.setInt(5, 3);
            stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 2));
            stmt.setBigDecimal(7, null);
            stmt.setNull(8, Types.BIGINT);
            stmt.setNull(9, Types.INTEGER);
            stmt.setNull(10, Types.INTEGER);
            stmt.setByte(11, (byte)3);
            stmt.setShort(12, (short) 130);
            stmt.setFloat(13, 0.03f);
            stmt.setDouble(14, 0.0003);
            stmt.setFloat(15, 0.03f);
            stmt.setDouble(16, 0.0003);
            stmt.execute();
                
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW4);
            stmt.setString(3, A_VALUE);
            stmt.setString(4, B_VALUE);
            stmt.setInt(5, 4);
            stmt.setDate(6, date == null ? null : date);
            stmt.setBigDecimal(7, null);
            stmt.setNull(8, Types.BIGINT);
            stmt.setNull(9, Types.INTEGER);
            stmt.setNull(10, Types.INTEGER);
            stmt.setByte(11, (byte)4);
            stmt.setShort(12, (short) 131);
            stmt.setFloat(13, 0.04f);
            stmt.setDouble(14, 0.0004);
            stmt.setFloat(15, 0.04f);
            stmt.setDouble(16, 0.0004);
            stmt.execute();
                
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW5);
            stmt.setString(3, B_VALUE);
            stmt.setString(4, C_VALUE);
            stmt.setInt(5, 5);
            stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 1));
            stmt.setBigDecimal(7, null);
            stmt.setNull(8, Types.BIGINT);
            stmt.setNull(9, Types.INTEGER);
            stmt.setNull(10, Types.INTEGER);
            stmt.setByte(11, (byte)5);
            stmt.setShort(12, (short) 132);
            stmt.setFloat(13, 0.05f);
            stmt.setDouble(14, 0.0005);
            stmt.setFloat(15, 0.05f);
            stmt.setDouble(16, 0.0005);
            stmt.execute();
                
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW6);
            stmt.setString(3, B_VALUE);
            stmt.setString(4, E_VALUE);
            stmt.setInt(5, 6);
            stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 2));
            stmt.setBigDecimal(7, null);
            stmt.setNull(8, Types.BIGINT);
            stmt.setNull(9, Types.INTEGER);
            stmt.setNull(10, Types.INTEGER);
            stmt.setByte(11, (byte)6);
            stmt.setShort(12, (short) 133);
            stmt.setFloat(13, 0.06f);
            stmt.setDouble(14, 0.0006);
            stmt.setFloat(15, 0.06f);
            stmt.setDouble(16, 0.0006);
            stmt.execute();
                
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW7);
            stmt.setString(3, B_VALUE);
            stmt.setString(4, B_VALUE);
            stmt.setInt(5, 7);
            stmt.setDate(6, date == null ? null : date);
            stmt.setBigDecimal(7, BigDecimal.valueOf(0.1));
            stmt.setLong(8, 5L);
            stmt.setInt(9, 5);
            stmt.setNull(10, Types.INTEGER);
            stmt.setByte(11, (byte)7);
            stmt.setShort(12, (short) 134);
            stmt.setFloat(13, 0.07f);
            stmt.setDouble(14, 0.0007);
            stmt.setFloat(15, 0.07f);
            stmt.setDouble(16, 0.0007);
            stmt.execute();
                
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW8);
            stmt.setString(3, B_VALUE);
            stmt.setString(4, C_VALUE);
            stmt.setInt(5, 8);
            stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 1));
            stmt.setBigDecimal(7, BigDecimal.valueOf(3.9));
            long l = Integer.MIN_VALUE - 1L;
            assert(l < Integer.MIN_VALUE);
            stmt.setLong(8, l);
            stmt.setInt(9, 4);
            stmt.setNull(10, Types.INTEGER);
            stmt.setByte(11, (byte)8);
            stmt.setShort(12, (short) 135);
            stmt.setFloat(13, 0.08f);
            stmt.setDouble(14, 0.0008);
            stmt.setFloat(15, 0.08f);
            stmt.setDouble(16, 0.0008);
            stmt.execute();
                
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW9);
            stmt.setString(3, C_VALUE);
            stmt.setString(4, E_VALUE);
            stmt.setInt(5, 9);
            stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 2));
            stmt.setBigDecimal(7, BigDecimal.valueOf(3.3));
            l = Integer.MAX_VALUE + 1L;
            assert(l > Integer.MAX_VALUE);
            stmt.setLong(8, l);
            stmt.setInt(9, 3);
            stmt.setInt(10, 300);
            stmt.setByte(11, (byte)9);
            stmt.setShort(12, (short) 0);
            stmt.setFloat(13, 0.09f);
            stmt.setDouble(14, 0.0009);
            stmt.setFloat(15, 0.09f);
            stmt.setDouble(16, 0.0009);
            stmt.execute();
                
            conn.commit();
        } finally {
            conn.close();
        }
    }
}
