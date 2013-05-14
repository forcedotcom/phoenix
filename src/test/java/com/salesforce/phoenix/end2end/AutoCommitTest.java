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


public class AutoCommitTest extends BaseHBaseManagedTimeTest {

    @Test
    public void testMutationJoin() throws Exception {
        
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        
        String ddl = "CREATE TABLE test_table " +
                "  (row varchar not null, col1 integer" +
                "  CONSTRAINT pk PRIMARY KEY (row))\n";
        createTestTable(getUrl(), ddl);
        
        String query = "UPSERT INTO test_table(row, col1) VALUES('row1', 1)";
        PreparedStatement statement = conn.prepareStatement(query);
        statement.executeUpdate();
        conn.commit();
        
        conn.setAutoCommit(false);
        query = "UPSERT INTO test_table(row, col1) VALUES('row1', 2)";
        statement = conn.prepareStatement(query);
        statement.executeUpdate();
        
        query = "DELETE FROM test_table WHERE row='row1'";
        statement = conn.prepareStatement(query);
        statement.executeUpdate();
        conn.commit();
        
        query = "SELECT * FROM test_table";
        statement = conn.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        assertFalse(rs.next());

        query = "DELETE FROM test_table WHERE row='row1'";
        statement = conn.prepareStatement(query);
        statement.executeUpdate();

        query = "UPSERT INTO test_table(row, col1) VALUES('row1', 3)";
        statement = conn.prepareStatement(query);
        statement.executeUpdate();
        conn.commit();
        
        query = "SELECT * FROM test_table";
        statement = conn.prepareStatement(query);
        rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals("row1", rs.getString(1));
        assertEquals(3, rs.getInt(2));

        conn.close();
    }
}
