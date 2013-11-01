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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.Test;

public class ReverseFunctionTest extends BaseHBaseManagedTimeTest {
    private void initTable(Connection conn, String sortOrder, String s) throws Exception {
        String ddl = "CREATE TABLE REVERSE_TEST (pk VARCHAR NOT NULL PRIMARY KEY " + sortOrder + ", kv VARCHAR)";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO REVERSE_TEST VALUES(?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setString(1, s);
        stmt.execute();
        conn.commit();        
    }
    
    private void testReverse(Connection conn, String s) throws Exception {
        StringBuilder buf = new StringBuilder(s);
        String reverse = buf.reverse().toString();
        
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT reverse(pk) FROM REVERSE_TEST");
        assertTrue(rs.next());
        assertEquals(reverse, rs.getString(1));
        assertFalse(rs.next());
        
        PreparedStatement stmt = conn.prepareStatement("SELECT pk FROM REVERSE_TEST WHERE pk=reverse(?)");
        stmt.setString(1, reverse);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(s, rs.getString(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testSingleByteReverseAscending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String s = "abc";
        initTable(conn, "ASC", s);
        testReverse(conn, s);
    }                                                           

    @Test
    public void testMultiByteReverseAscending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String s = "ɚɦɰɸ";
        initTable(conn, "DESC", s);
        testReverse(conn, s);
    }                                                           

    
    @Test
    public void testSingleByteReverseDecending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String s = "abc";
        initTable(conn, "DESC", s);
        testReverse(conn, s);
    }                                                           

    @Test
    public void testMultiByteReverseDecending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String s = "ɚɦɰɸ";
        initTable(conn, "ASC", s);
        testReverse(conn, s);
    }
    
    @Test
    public void testNullReverse() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String s = "abc";
        initTable(conn, "ASC", s);
        
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT reverse(kv) FROM REVERSE_TEST");
        assertTrue(rs.next());
        assertNull(rs.getString(1));
        assertFalse(rs.next());
    }                                                           

}
