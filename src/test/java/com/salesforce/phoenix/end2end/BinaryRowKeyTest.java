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

import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

import org.junit.Test;

public class BinaryRowKeyTest extends BaseHBaseManagedTimeTest {

    private static void initTableValues() throws SQLException {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        
        try {
            String ddl = "CREATE TABLE test_table" +
                    "   (a_binary binary(10) not null, \n" +
                    "    a_string varchar not null, \n" +
                    "    b_binary varbinary \n" +
                    "    CONSTRAINT pk PRIMARY KEY (a_binary, a_string))\n";
            createTestTable(getUrl(), ddl);
            
            String query;
            PreparedStatement stmt;
            
            query = "UPSERT INTO test_table"
                    + "(a_binary, a_string) "
                    + "VALUES(?,?)";
            stmt = conn.prepareStatement(query);
            
            stmt.setBytes(1, new byte[] {0,0,0,0,0,0,0,0,0,1});
            stmt.setString(2, "a");
            stmt.execute();
            
            stmt.setBytes(1, new byte[] {0,0,0,0,0,0,0,0,0,2});
            stmt.setString(2, "b");
            stmt.execute();
            conn.commit();
            
        } finally {
            conn.close();
        }
    }

    @Test
    public void testInsertPaddedBinaryValue() throws SQLException {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues();
            conn.setAutoCommit(true);
            conn.createStatement().execute("DELETE FROM test_table");
           
            String query = "UPSERT INTO test_table"
                    + "(a_binary, a_string) "
                    + "VALUES(?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setBytes(1, new byte[] {0,0,0,0,0,0,0,0,1});
            stmt.setString(2, "a");
            stmt.execute();
            
            ResultSet rs = conn.createStatement().executeQuery("SELECT a_string FROM test_table");
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectValues() throws SQLException {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        
        try {
            initTableValues();
            
            String query = "SELECT * FROM test_table";
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            
            assertTrue(rs.next());
            assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,1}, rs.getBytes(1));
            assertEquals("a", rs.getString(2));
            
            assertTrue(rs.next());
            assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,2}, rs.getBytes(1));
            assertEquals("b", rs.getString(2));
            
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpsertSelectValues() throws SQLException {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        
        try {
            initTableValues();
            
            String query = "UPSERT INTO test_table (a_binary, a_string, b_binary) "
                    + " SELECT a_binary, a_string, a_binary FROM test_table";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            
            query = "SELECT a_binary, b_binary FROM test_table";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            
            assertTrue(rs.next());
            assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,1}, rs.getBytes(1));
            assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,1}, rs.getBytes(2));
            
            assertTrue(rs.next());
            assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,2}, rs.getBytes(1));
            assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,2}, rs.getBytes(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
