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


public class AlterTableTest extends BaseHBaseManagedTimeTest {

    @Test
    public void testAlterTableWithVarBinaryKey() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        
        try {
            String ddl = "CREATE TABLE test_table " +
                    "  (a_string varchar not null, a_binary varbinary not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string, a_binary))\n";
            createTestTable(getUrl(), ddl);
            
            ddl = "ALTER TABLE test_table ADD b_string VARCHAR NULL PRIMARY KEY";
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            fail("Should have caught bad alter.");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1022 (42J04): Cannot add column to table when the last PK column is of type VARBINARY."));
        } finally {
            conn.close();
        }
    }


    @Test
    public void testAddVarCharColToPK() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        
        try {
            String ddl = "CREATE TABLE test_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            conn.createStatement().execute(ddl);
            
            String dml = "UPSERT INTO test_table VALUES(?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.execute();
            stmt.setString(1, "a");
            stmt.execute();
            conn.commit();
            
            String query = "SELECT * FROM test_table";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertFalse(rs.next());
            
            ddl = "ALTER TABLE test_table ADD b_string VARCHAR NULL PRIMARY KEY";
            conn.createStatement().execute(ddl);
            
            query = "SELECT * FROM test_table WHERE a_string = 'a' AND b_string IS NULL";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertFalse(rs.next());
            
            dml = "UPSERT INTO test_table VALUES(?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "c");
            stmt.execute();
            conn.commit();
           
            query = "SELECT * FROM test_table WHERE a_string = 'c' AND b_string IS NULL";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1));
            assertFalse(rs.next());
            
            dml = "UPSERT INTO test_table(a_string,col1) VALUES(?,?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 5);
            stmt.execute();
            conn.commit();
           
            query = "SELECT a_string,col1 FROM test_table WHERE a_string = 'a' AND b_string IS NULL";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(5,rs.getInt(2)); // TODO: figure out why this flaps
            assertFalse(rs.next());
            
        } finally {
            conn.close();
        }
    }
}
