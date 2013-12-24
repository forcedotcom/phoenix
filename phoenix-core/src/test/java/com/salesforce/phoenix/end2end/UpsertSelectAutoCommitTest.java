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

import static com.salesforce.phoenix.util.TestUtil.A_VALUE;
import static com.salesforce.phoenix.util.TestUtil.ROW1;
import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.junit.Test;

public class UpsertSelectAutoCommitTest extends BaseHBaseManagedTimeTest {

    public UpsertSelectAutoCommitTest() {
    }

    @Test
    public void testAutoCommitUpsertSelect() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        conn.createStatement().execute("CREATE TABLE atable (ORGANIZATION_ID CHAR(15) NOT NULL, ENTITY_ID CHAR(15) NOT NULL, A_STRING VARCHAR\n" +
        "CONSTRAINT pk PRIMARY KEY (organization_id, entity_id))");
        
        String tenantId = getOrganizationId();
       // Insert all rows at ts
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " +
                "ATABLE(" +
                "    ORGANIZATION_ID, " +
                "    ENTITY_ID, " +
                "    A_STRING " +
                "    )" +
                "VALUES (?, ?, ?)");
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW1);
        stmt.setString(3, A_VALUE);
        stmt.execute();
        
        String query = "SELECT entity_id, a_string FROM ATABLE";
        PreparedStatement statement = conn.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        
        assertTrue(rs.next());
        assertEquals(ROW1, rs.getString(1));
        assertEquals(A_VALUE, rs.getString(2));
        assertFalse(rs.next());
        
        conn.createStatement().execute("CREATE TABLE atable2 (ORGANIZATION_ID CHAR(15) NOT NULL, ENTITY_ID CHAR(15) NOT NULL, A_STRING VARCHAR\n" +
        "CONSTRAINT pk PRIMARY KEY (organization_id, entity_id))");
        
        conn.createStatement().execute("UPSERT INTO atable2 SELECT * FROM ATABLE");
        query = "SELECT entity_id, a_string FROM ATABLE2";
        statement = conn.prepareStatement(query);
        rs = statement.executeQuery();
        
        assertTrue(rs.next());
        assertEquals(ROW1, rs.getString(1));
        assertEquals(A_VALUE, rs.getString(2));
        assertFalse(rs.next());
        
    }
}
