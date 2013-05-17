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


public class UpsertIncreaseTest extends BaseHBaseManagedTimeTest {

    @Test
    public void testIncrease() throws Exception {
        
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        
        String ddl = "CREATE TABLE test_table " +
                "  (row varchar not null, col_int integer, col_long bigint, col_long2 bigint" +
                "  CONSTRAINT pk PRIMARY KEY (row))\n";
        createTestTable(getUrl(), ddl);

        // increase on existing row.
        String query = "UPSERT INTO test_table(row, col_int, col_long, col_long2) VALUES('row1', 1, 1, 1)";
        PreparedStatement statement = conn.prepareStatement(query);
        statement.executeUpdate();
        query = "UPSERT INTO test_table(row, col_long, col_long2) INCREASE VALUES('row1', 100, 1000)";
        statement = conn.prepareStatement(query);
        statement.executeUpdate();
        
        query = "SELECT * FROM test_table WHERE row = 'row1'";
        statement = conn.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals("row1", rs.getString(1));
        assertEquals(1, rs.getInt(2));
        assertEquals(101, rs.getLong(3));
        assertEquals(1001, rs.getLong(4));

        // increase on non existing row.
        query = "UPSERT INTO test_table(row, col_long, col_long2) INCREASE VALUES('row2', 100, 1000)";
        statement = conn.prepareStatement(query);
        statement.executeUpdate();
        
        query = "SELECT * FROM test_table WHERE row = 'row2'";
        statement = conn.prepareStatement(query);
        rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals("row2", rs.getString(1));
        
        // FIXME: Why this is zero?, should be NULL?
        assertEquals(0, rs.getInt(2));

        assertEquals(100, rs.getLong(3));
        assertEquals(1000, rs.getLong(4));

        
        // double increase with auto commit off to check mutation did merge.
        conn.setAutoCommit(false);
        query = "UPSERT INTO test_table(row, col_int, col_long, col_long2) VALUES('row3', 1, 1, 1)";
        statement = conn.prepareStatement(query);
        statement.executeUpdate();
        conn.commit();

        query = "UPSERT INTO test_table(row, col_long, col_long2) INCREASE VALUES('row3', 100, -1000)";
        statement = conn.prepareStatement(query);
        statement.executeUpdate();
        
        query = "UPSERT INTO test_table(row, col_long, col_long2) INCREASE VALUES('row3', -50, 500)";
        statement = conn.prepareStatement(query);
        statement.executeUpdate();
        conn.commit();

        query = "SELECT * FROM test_table WHERE row = 'row3'";
        statement = conn.prepareStatement(query);
        rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals("row3", rs.getString(1));
        assertEquals(1, rs.getInt(2));
        assertEquals(51, rs.getLong(3));
        assertEquals(-499, rs.getLong(4));

        conn.close();
    }

}
