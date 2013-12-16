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

import static com.salesforce.phoenix.util.TestUtil.JOIN_CUSTOMER_TABLE;
import static com.salesforce.phoenix.util.TestUtil.JOIN_ITEM_TABLE;
import static com.salesforce.phoenix.util.TestUtil.JOIN_SUPPLIER_TABLE;
import static com.salesforce.phoenix.util.TestUtil.PHOENIX_JDBC_URL;
import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.schema.TableAlreadyExistsException;
import com.salesforce.phoenix.util.QueryUtil;

public class HashJoinTestWithIndex extends HashJoinTest {
    
    @Override
    protected void createIndices() throws Exception {
        createIndex("CREATE INDEX INDEX_" + JOIN_CUSTOMER_TABLE + " ON " + JOIN_CUSTOMER_TABLE + " (name)");
        createIndex("CREATE INDEX INDEX_" + JOIN_ITEM_TABLE + " ON " + JOIN_ITEM_TABLE + " (name)");
        createIndex("CREATE INDEX INDEX_" + JOIN_SUPPLIER_TABLE + " ON " + JOIN_SUPPLIER_TABLE + " (name)");
    }
    
    private void createIndex(String ddl) throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl(), new Properties());
        try {
            conn.createStatement().execute(ddl);
        } catch (TableAlreadyExistsException e) {
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJoinPlanWithIndex() throws Exception {
        initTableValues();
        String query1 = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_ITEM_TABLE + " item LEFT JOIN " + JOIN_SUPPLIER_TABLE + " supp ON substr(item.name, 2, 1) = substr(supp.name, 2, 1) AND (supp.name BETWEEN 'S1' AND 'S5') WHERE item.name BETWEEN 'T1' AND 'T5'";
        String query2 = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_ITEM_TABLE + " item INNER JOIN " + JOIN_SUPPLIER_TABLE + " supp ON item.supplier_id = supp.supplier_id AND (supp.name = 'S1' OR supp.name = 'S5') WHERE item.name = 'T1' OR item.name = 'T5'";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(QueryServices.USE_INDEXES_ATTRIB, Boolean.toString(false));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query1);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "0000000003");
            assertEquals(rs.getString(4), "S3");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000004");
            assertEquals(rs.getString(4), "S4");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query1);
            assertEquals(
                    "CLIENT PARALLEL 1-WAY RANGE SCAN OVER INDEX_JOIN_ITEM_TABLE ['T1'] - ['T5']\n" +
                    "    PARALLEL EQUI-JOIN 1 HASH TABLES:\n" +
                    "    BUILD HASH TABLE 0\n" +
                    "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER INDEX_JOIN_SUPPLIER_TABLE ['S1'] - ['S5']", 
                    QueryUtil.getExplainPlan(rs));
            
            
            statement = conn.prepareStatement(query2);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query2);
            assertEquals(
                    "CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_ITEM_TABLE\n" +
                    "    SERVER FILTER BY (NAME = 'T1' OR NAME = 'T5')\n" +
                    "    PARALLEL EQUI-JOIN 1 HASH TABLES:\n" +
                    "    BUILD HASH TABLE 0\n" +
                    "        CLIENT PARALLEL 1-WAY SKIP SCAN ON 2 KEYS OVER INDEX_JOIN_SUPPLIER_TABLE ['S1'] - ['S5']", 
                    QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }
}
