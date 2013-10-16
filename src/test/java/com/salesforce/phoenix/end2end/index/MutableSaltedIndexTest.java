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

import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Before;
import org.junit.Test;

import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.util.QueryUtil;


public class MutableSaltedIndexTest extends BaseMutableIndexTest{
    private static final int TABLE_SPLITS = 3;
    private static final int INDEX_SPLITS = 4;
    
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
             "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + INDEX_TABLE_FULL_NAME + " [~'y']" : 
            ("CLIENT PARALLEL 4-WAY SKIP SCAN ON 4 KEYS OVER " + INDEX_TABLE_FULL_NAME + " [0,~'y'] - [3,~'y']\n" + 
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
            "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + INDEX_TABLE_FULL_NAME + " [*] - [~'x']" :
            ("CLIENT PARALLEL 4-WAY SKIP SCAN ON 4 RANGES OVER " + INDEX_TABLE_FULL_NAME + " [0,*] - [3,~'x']\n" + 
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
            ("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + INDEX_TABLE_FULL_NAME + " [*] - [~'x']\n" + 
             "    SERVER TOP -1 ROWS SORTED BY [:K]\n" + 
             "CLIENT MERGE SORT") :
            ("CLIENT PARALLEL 4-WAY SKIP SCAN ON 4 RANGES OVER " + INDEX_TABLE_FULL_NAME + " [0,*] - [3,~'x']\n" + 
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
                 "CLIENT PARALLEL 3-WAY FULL SCAN OVER " + DATA_TABLE_FULL_NAME + "\n" +
                 "    SERVER FILTER BY V >= 'x'\n" + 
                 "    SERVER 2 ROW LIMIT\n" + 
                 "CLIENT MERGE SORT\n" + 
                 "CLIENT 2 ROW LIMIT";
        assertEquals(expectedPlan,QueryUtil.getExplainPlan(rs));
    }
}
