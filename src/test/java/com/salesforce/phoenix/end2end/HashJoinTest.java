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
import static com.salesforce.phoenix.util.TestUtil.JOIN_ORDER_TABLE;
import static com.salesforce.phoenix.util.TestUtil.JOIN_SUPPLIER_TABLE;
import static com.salesforce.phoenix.util.TestUtil.PHOENIX_JDBC_URL;
import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.junit.Test;

public class HashJoinTest extends BaseClientMangedTimeTest {
    
    private void initAllTableValues() throws Exception {
    	initMetaInfoTableValues();
        ensureTableCreated(getUrl(), JOIN_ORDER_TABLE);
        
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // Insert into order table
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + JOIN_ORDER_TABLE +
                    "   (ORDER_ID, " +
                    "    CUSTOMER_ID, " +
                    "    ITEM_ID, " +
                    "    QUANTITY," +
                    "    DATE) " +
                    "values (?, ?, ?, ?, ?)");
            stmt.setString(1, "000000000000001");
            stmt.setString(2, "0000000004");
            stmt.setString(3, "0000000001");
            stmt.setInt(4, 1000);
            stmt.setDate(5, new Date(System.currentTimeMillis()));
            stmt.execute();

            stmt.setString(1, "000000000000002");
            stmt.setString(2, "0000000003");
            stmt.setString(3, "0000000006");
            stmt.setInt(4, 2000);
            stmt.setDate(5, new Date(System.currentTimeMillis()));
            stmt.execute();

            stmt.setString(1, "000000000000003");
            stmt.setString(2, "0000000002");
            stmt.setString(3, "0000000002");
            stmt.setInt(4, 3000);
            stmt.setDate(5, new Date(System.currentTimeMillis()));
            stmt.execute();

            stmt.setString(1, "000000000000004");
            stmt.setString(2, "0000000004");
            stmt.setString(3, "0000000006");
            stmt.setInt(4, 4000);
            stmt.setDate(5, new Date(System.currentTimeMillis()));
            stmt.execute();

            stmt.setString(1, "000000000000005");
            stmt.setString(2, "0000000005");
            stmt.setString(3, "0000000003");
            stmt.setInt(4, 5000);
            stmt.setDate(5, new Date(System.currentTimeMillis()));
            stmt.execute();

            conn.commit();
        } finally {
            conn.close();
        }
    }
    
    private void initMetaInfoTableValues() throws Exception {
        ensureTableCreated(getUrl(), JOIN_CUSTOMER_TABLE);
        ensureTableCreated(getUrl(), JOIN_ITEM_TABLE);
        ensureTableCreated(getUrl(), JOIN_SUPPLIER_TABLE);
        
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // Insert into customer table
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + JOIN_CUSTOMER_TABLE +
                    "   (CUSTOMER_ID, " +
                    "    NAME, " +
                    "    PHONE, " +
                    "    ADDRESS, " +
                    "    LOC_ID) " +
                    "values (?, ?, ?, ?, ?)");
            stmt.setString(1, "0000000001");
            stmt.setString(2, "C1");
            stmt.setString(3, "999-999-1111");
            stmt.setString(4, "101 XXX Street");
            stmt.setString(5, "10001");
            stmt.execute();
                
            stmt.setString(1, "0000000002");
            stmt.setString(2, "C2");
            stmt.setString(3, "999-999-2222");
            stmt.setString(4, "202 XXX Street");
            stmt.setString(5, null);
            stmt.execute();

            stmt.setString(1, "0000000003");
            stmt.setString(2, "C3");
            stmt.setString(3, "999-999-3333");
            stmt.setString(4, "303 XXX Street");
            stmt.setString(5, null);
            stmt.execute();

            stmt.setString(1, "0000000004");
            stmt.setString(2, "C4");
            stmt.setString(3, "999-999-4444");
            stmt.setString(4, "404 XXX Street");
            stmt.setString(5, "10004");
            stmt.execute();

            stmt.setString(1, "0000000005");
            stmt.setString(2, "C5");
            stmt.setString(3, "999-999-5555");
            stmt.setString(4, "505 XXX Street");
            stmt.setString(5, "10005");
            stmt.execute();

            stmt.setString(1, "0000000006");
            stmt.setString(2, "C6");
            stmt.setString(3, "999-999-6666");
            stmt.setString(4, "606 XXX Street");
            stmt.setString(5, "10001");
            stmt.execute();
            
            // Insert into item table
            stmt = conn.prepareStatement(
                    "upsert into " + JOIN_ITEM_TABLE +
                    "   (ITEM_ID, " +
                    "    NAME, " +
                    "    PRICE, " +
                    "    SUPPLIER_ID, " +
                    "    DESCRIPTION) " +
                    "values (?, ?, ?, ?, ?)");
            stmt.setString(1, "0000000001");
            stmt.setString(2, "T1");
            stmt.setInt(3, 100);
            stmt.setString(4, "0000000001");
            stmt.setString(5, "Item T1");
            stmt.execute();

            stmt.setString(1, "0000000002");
            stmt.setString(2, "T2");
            stmt.setInt(3, 200);
            stmt.setString(4, "0000000001");
            stmt.setString(5, "Item T2");
            stmt.execute();

            stmt.setString(1, "0000000003");
            stmt.setString(2, "T3");
            stmt.setInt(3, 300);
            stmt.setString(4, "0000000002");
            stmt.setString(5, "Item T3");
            stmt.execute();

            stmt.setString(1, "0000000004");
            stmt.setString(2, "T4");
            stmt.setInt(3, 400);
            stmt.setString(4, "0000000002");
            stmt.setString(5, "Item T4");
            stmt.execute();

            stmt.setString(1, "0000000005");
            stmt.setString(2, "T5");
            stmt.setInt(3, 500);
            stmt.setString(4, "0000000005");
            stmt.setString(5, "Item T5");
            stmt.execute();

            stmt.setString(1, "0000000006");
            stmt.setString(2, "T6");
            stmt.setInt(3, 600);
            stmt.setString(4, "0000000006");
            stmt.setString(5, "Item T6");
            stmt.execute();
            
            stmt.setString(1, "invalid001");
            stmt.setString(2, "INVALID-1");
            stmt.setInt(3, 0);
            stmt.setString(4, "0000000000");
            stmt.setString(5, "Invalid item for join test");
            stmt.execute();

            // Insert into supplier table
            stmt = conn.prepareStatement(
                    "upsert into " + JOIN_SUPPLIER_TABLE +
                    "   (SUPPLIER_ID, " +
                    "    NAME, " +
                    "    PHONE, " +
                    "    ADDRESS, " +
                    "    LOC_ID) " +
                    "values (?, ?, ?, ?, ?)");
            stmt.setString(1, "0000000001");
            stmt.setString(2, "S1");
            stmt.setString(3, "888-888-1111");
            stmt.setString(4, "101 YYY Street");
            stmt.setString(5, "10001");
            stmt.execute();
                
            stmt.setString(1, "0000000002");
            stmt.setString(2, "S2");
            stmt.setString(3, "888-888-2222");
            stmt.setString(4, "202 YYY Street");
            stmt.setString(5, "10002");
            stmt.execute();

            stmt.setString(1, "0000000003");
            stmt.setString(2, "S3");
            stmt.setString(3, "888-888-3333");
            stmt.setString(4, "303 YYY Street");
            stmt.setString(5, null);
            stmt.execute();

            stmt.setString(1, "0000000004");
            stmt.setString(2, "S4");
            stmt.setString(3, "888-888-4444");
            stmt.setString(4, "404 YYY Street");
            stmt.setString(5, null);
            stmt.execute();

            stmt.setString(1, "0000000005");
            stmt.setString(2, "S5");
            stmt.setString(3, "888-888-5555");
            stmt.setString(4, "505 YYY Street");
            stmt.setString(5, "10005");
            stmt.execute();

            stmt.setString(1, "0000000006");
            stmt.setString(2, "S6");
            stmt.setString(3, "888-888-6666");
            stmt.setString(4, "606 YYY Street");
            stmt.setString(5, "10006");
            stmt.execute();

            conn.commit();
        } finally {
            conn.close();
        }
    }

    @Test
    public void testInnerJoin() throws Exception {
        initMetaInfoTableValues();
        String query = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_ITEM_TABLE + " item INNER JOIN " + JOIN_SUPPLIER_TABLE + " supp ON item.supplier_id = supp.supplier_id";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "0000000006");
            assertEquals(rs.getString(4), "S6");

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
            
    @Test
    public void testLeftJoin() throws Exception {
        initMetaInfoTableValues();
        String query = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_ITEM_TABLE + " item LEFT JOIN " + JOIN_SUPPLIER_TABLE + " supp ON item.supplier_id = supp.supplier_id";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "0000000006");
            assertEquals(rs.getString(4), "S6");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "invalid001");
            assertEquals(rs.getString(2), "INVALID-1");
            assertNull(rs.getString(3));
            assertNull(rs.getString(4));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testRightJoin() throws Exception {
        initMetaInfoTableValues();
        String query = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_SUPPLIER_TABLE + " supp RIGHT JOIN " + JOIN_ITEM_TABLE + " item ON item.supplier_id = supp.supplier_id";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "0000000006");
            assertEquals(rs.getString(4), "S6");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "invalid001");
            assertEquals(rs.getString(2), "INVALID-1");
            assertNull(rs.getString(3));
            assertNull(rs.getString(4));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testInnerJoinWithPreFilters() throws Exception {
        initMetaInfoTableValues();
        String query1 = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_ITEM_TABLE + " item INNER JOIN " + JOIN_SUPPLIER_TABLE + " supp ON item.supplier_id = supp.supplier_id AND supp.supplier_id BETWEEN '0000000001' AND '0000000005'";
        String query2 = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_ITEM_TABLE + " item INNER JOIN " + JOIN_SUPPLIER_TABLE + " supp ON item.supplier_id = supp.supplier_id AND (supp.supplier_id = '0000000001' OR supp.supplier_id = '0000000005')";
        Properties props = new Properties(TEST_PROPERTIES);
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
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");

            assertFalse(rs.next());
            
            
            statement = conn.prepareStatement(query2);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testLeftJoinWithPreFilters() throws Exception {
        initMetaInfoTableValues();
        String query = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_ITEM_TABLE + " item LEFT JOIN " + JOIN_SUPPLIER_TABLE + " supp ON item.supplier_id = supp.supplier_id AND (supp.supplier_id = '0000000001' OR supp.supplier_id = '0000000005')";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T3");
            assertNull(rs.getString(3));
            assertNull(rs.getString(4));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertNull(rs.getString(3));
            assertNull(rs.getString(4));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");
            assertNull(rs.getString(3));
            assertNull(rs.getString(4));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "invalid001");
            assertEquals(rs.getString(2), "INVALID-1");
            assertNull(rs.getString(3));
            assertNull(rs.getString(4));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJoinWithPostFilters() throws Exception {
        initMetaInfoTableValues();
        String query1 = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_SUPPLIER_TABLE + " supp RIGHT JOIN " + JOIN_ITEM_TABLE + " item ON item.supplier_id = supp.supplier_id WHERE supp.supplier_id BETWEEN '0000000001' AND '0000000005'";
        String query2 = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_ITEM_TABLE + " item LEFT JOIN " + JOIN_SUPPLIER_TABLE + " supp ON item.supplier_id = supp.supplier_id WHERE supp.supplier_id = '0000000001' OR supp.supplier_id = '0000000005'";
        Properties props = new Properties(TEST_PROPERTIES);
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
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");

            assertFalse(rs.next());
            
            
            statement = conn.prepareStatement(query2);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testStarJoin() throws Exception {
        initAllTableValues();
        String query = "SELECT order_id, c.name, i.name, quantity, date FROM " + JOIN_ORDER_TABLE + " o LEFT JOIN " 
        	+ JOIN_CUSTOMER_TABLE + " c ON o.customer_id = c.customer_id LEFT JOIN " 
        	+ JOIN_ITEM_TABLE + " i ON o.item_id = i.item_id";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertEquals(rs.getString(2), "C4");
            assertEquals(rs.getString(3), "T1");
            assertEquals(rs.getInt(4), 1000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000002");
            assertEquals(rs.getString(2), "C3");
            assertEquals(rs.getString(3), "T6");
            assertEquals(rs.getInt(4), 2000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertEquals(rs.getString(2), "C2");
            assertEquals(rs.getString(3), "T2");
            assertEquals(rs.getInt(4), 3000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000004");
            assertEquals(rs.getString(2), "C4");
            assertEquals(rs.getString(3), "T6");
            assertEquals(rs.getInt(4), 4000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertEquals(rs.getString(2), "C5");
            assertEquals(rs.getString(3), "T3");
            assertEquals(rs.getInt(4), 5000);
            assertNotNull(rs.getDate(5));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testLeftJoinWithAggregation() throws Exception {
        initAllTableValues();
        String query = "SELECT i.name, sum(quantity) FROM " + JOIN_ORDER_TABLE + " o LEFT JOIN " 
        	+ JOIN_ITEM_TABLE + " i ON o.item_id = i.item_id GROUP BY i.name ORDER BY i.name";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T1");
            assertEquals(rs.getInt(2), 1000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T2");
            assertEquals(rs.getInt(2), 3000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T3");
            assertEquals(rs.getInt(2), 5000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T6");
            assertEquals(rs.getInt(2), 6000);

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testRightJoinWithAggregation() throws Exception {
        initAllTableValues();
        String query = "SELECT i.name, sum(quantity) FROM " + JOIN_ORDER_TABLE + " o RIGHT JOIN " 
        	+ JOIN_ITEM_TABLE + " i ON o.item_id = i.item_id GROUP BY i.name ORDER BY i.name";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "INVALID-1");
            assertEquals(rs.getInt(2), 0);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T1");
            assertEquals(rs.getInt(2), 1000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T2");
            assertEquals(rs.getInt(2), 3000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T3");
            assertEquals(rs.getInt(2), 5000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T4");
            assertEquals(rs.getInt(2), 0);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T5");
            assertEquals(rs.getInt(2), 0);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T6");
            assertEquals(rs.getInt(2), 6000);

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testLeftRightJoin() throws Exception {
        initAllTableValues();
        String query = "SELECT order_id, i.name, s.name, quantity, date FROM " + JOIN_ORDER_TABLE + " o LEFT JOIN " 
			+ JOIN_ITEM_TABLE + " i ON o.item_id = i.item_id RIGHT JOIN "
			+ JOIN_SUPPLIER_TABLE + " s ON i.supplier_id = s.supplier_id ORDER BY order_id, s.supplier_id DESC";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertNull(rs.getString(1));
            assertNull(rs.getString(2));
            assertEquals(rs.getString(3), "S5");
            assertEquals(rs.getInt(4), 0);
            assertNull(rs.getDate(5));
            assertTrue (rs.next());
            assertNull(rs.getString(1));
            assertNull(rs.getString(2));
            assertEquals(rs.getString(3), "S4");
            assertEquals(rs.getInt(4), 0);
            assertNull(rs.getDate(5));
            assertTrue (rs.next());
            assertNull(rs.getString(1));
            assertNull(rs.getString(2));
            assertEquals(rs.getString(3), "S3");
            assertEquals(rs.getInt(4), 0);
            assertNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getInt(4), 1000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000002");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "S6");
            assertEquals(rs.getInt(4), 2000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getInt(4), 3000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000004");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "S6");
            assertEquals(rs.getInt(4), 4000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "S2");
            assertEquals(rs.getInt(4), 5000);
            assertNotNull(rs.getDate(5));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testMultiLeftJoin() throws Exception {
        initAllTableValues();
        String query = "SELECT order_id, i.name, s.name, quantity, date FROM " + JOIN_ORDER_TABLE + " o LEFT JOIN " 
			+ JOIN_ITEM_TABLE + " i ON o.item_id = i.item_id LEFT JOIN "
			+ JOIN_SUPPLIER_TABLE + " s ON i.supplier_id = s.supplier_id";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getInt(4), 1000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000002");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "S6");
            assertEquals(rs.getInt(4), 2000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getInt(4), 3000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000004");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "S6");
            assertEquals(rs.getInt(4), 4000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "S2");
            assertEquals(rs.getInt(4), 5000);
            assertNotNull(rs.getDate(5));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testMultiRightJoin() throws Exception {
        initAllTableValues();
        String query = "SELECT order_id, i.name, s.name, quantity, date FROM " + JOIN_ORDER_TABLE + " o RIGHT JOIN " 
			+ JOIN_ITEM_TABLE + " i ON o.item_id = i.item_id RIGHT JOIN "
			+ JOIN_SUPPLIER_TABLE + " s ON i.supplier_id = s.supplier_id ORDER BY order_id, s.supplier_id DESC";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertNull(rs.getString(1));
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "S5");
            assertEquals(rs.getInt(4), 0);
            assertNull(rs.getDate(5));
            assertTrue (rs.next());
            assertNull(rs.getString(1));
            assertNull(rs.getString(2));
            assertEquals(rs.getString(3), "S4");
            assertEquals(rs.getInt(4), 0);
            assertNull(rs.getDate(5));
            assertTrue (rs.next());
            assertNull(rs.getString(1));
            assertNull(rs.getString(2));
            assertEquals(rs.getString(3), "S3");
            assertEquals(rs.getInt(4), 0);
            assertNull(rs.getDate(5));
            assertTrue (rs.next());
            assertNull(rs.getString(1));
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "S2");
            assertEquals(rs.getInt(4), 0);
            assertNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getInt(4), 1000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000002");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "S6");
            assertEquals(rs.getInt(4), 2000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getInt(4), 3000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000004");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "S6");
            assertEquals(rs.getInt(4), 4000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "S2");
            assertEquals(rs.getInt(4), 5000);
            assertNotNull(rs.getDate(5));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJoinWithWildcard() throws Exception {
        initMetaInfoTableValues();
        String query = "SELECT * FROM " + JOIN_ITEM_TABLE + " item LEFT JOIN " + JOIN_SUPPLIER_TABLE + " supp ON item.supplier_id = supp.supplier_id";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getInt(3), 100);
            assertEquals(rs.getString(4), "0000000001");
            assertEquals(rs.getString(5), "Item T1");
            assertEquals(rs.getString(6), "0000000001");
            assertEquals(rs.getString(7), "S1");
            assertEquals(rs.getString(8), "888-888-1111");
            assertEquals(rs.getString(9), "101 YYY Street");
            assertEquals(rs.getString(10), "10001");            
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getInt(3), 200);
            assertEquals(rs.getString(4), "0000000001");
            assertEquals(rs.getString(5), "Item T2");
            assertEquals(rs.getString(6), "0000000001");
            assertEquals(rs.getString(7), "S1");
            assertEquals(rs.getString(8), "888-888-1111");
            assertEquals(rs.getString(9), "101 YYY Street");
            assertEquals(rs.getString(10), "10001");            
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getInt(3), 300);
            assertEquals(rs.getString(4), "0000000002");
            assertEquals(rs.getString(5), "Item T3");
            assertEquals(rs.getString(6), "0000000002");
            assertEquals(rs.getString(7), "S2");
            assertEquals(rs.getString(8), "888-888-2222");
            assertEquals(rs.getString(9), "202 YYY Street");
            assertEquals(rs.getString(10), "10002");            
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getInt(3), 400);
            assertEquals(rs.getString(4), "0000000002");
            assertEquals(rs.getString(5), "Item T4");
            assertEquals(rs.getString(6), "0000000002");
            assertEquals(rs.getString(7), "S2");
            assertEquals(rs.getString(8), "888-888-2222");
            assertEquals(rs.getString(9), "202 YYY Street");
            assertEquals(rs.getString(10), "10002");            
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getInt(3), 500);
            assertEquals(rs.getString(4), "0000000005");
            assertEquals(rs.getString(5), "Item T5");
            assertEquals(rs.getString(6), "0000000005");
            assertEquals(rs.getString(7), "S5");
            assertEquals(rs.getString(8), "888-888-5555");
            assertEquals(rs.getString(9), "505 YYY Street");
            assertEquals(rs.getString(10), "10005");            
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getInt(3), 600);
            assertEquals(rs.getString(4), "0000000006");
            assertEquals(rs.getString(5), "Item T6");
            assertEquals(rs.getString(6), "0000000006");
            assertEquals(rs.getString(7), "S6");
            assertEquals(rs.getString(8), "888-888-6666");
            assertEquals(rs.getString(9), "606 YYY Street");
            assertEquals(rs.getString(10), "10006");            
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "invalid001");
            assertEquals(rs.getString(2), "INVALID-1");
            assertEquals(rs.getInt(3), 0);
            assertEquals(rs.getString(4), "0000000000");
            assertEquals(rs.getString(5), "Invalid item for join test");
            assertNull(rs.getString(6));
            assertNull(rs.getString(7));
            assertNull(rs.getString(8));
            assertNull(rs.getString(9));
            assertNull(rs.getString(10));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJoinMultiJoinKeys() throws Exception {
        initMetaInfoTableValues();
        String query = "SELECT c.name, s.name FROM " + JOIN_CUSTOMER_TABLE + " c LEFT JOIN " + JOIN_SUPPLIER_TABLE + " s ON customer_id = supplier_id AND c.loc_id = s.loc_id AND substr(s.name, 2, 1) = substr(c.name, 2, 1)";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "C1");
            assertEquals(rs.getString(2), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "C2");
            assertNull(rs.getString(2));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "C3");
            assertEquals(rs.getString(2), "S3");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "C4");
            assertNull(rs.getString(2));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "C5");
            assertEquals(rs.getString(2), "S5");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "C6");
            assertNull(rs.getString(2));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

}
