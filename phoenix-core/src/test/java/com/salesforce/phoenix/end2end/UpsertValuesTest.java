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

import static com.salesforce.phoenix.util.TestUtil.PHOENIX_JDBC_URL;
import static com.salesforce.phoenix.util.TestUtil.closeStatement;
import static com.salesforce.phoenix.util.TestUtil.closeStmtAndConn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.util.DateUtil;
import com.salesforce.phoenix.util.PhoenixRuntime;
import com.salesforce.phoenix.util.TestUtil;


public class UpsertValuesTest extends BaseClientMangedTimeTest {
    @Test
    public void testUpsertDateValues() throws Exception {
        long ts = nextTimestamp();
        Date now = new Date(System.currentTimeMillis());
        ensureTableCreated(getUrl(),TestUtil.PTSDB_NAME,null, ts-2);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1)); // Execute at timestamp 1
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        String dateString = "1999-01-01 02:00:00";
        PreparedStatement upsertStmt = conn.prepareStatement("upsert into ptsdb(inst,host,date) values('aaa','bbb',to_date('" + dateString + "'))");
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        upsertStmt = conn.prepareStatement("upsert into ptsdb(inst,host,date) values('ccc','ddd',current_date())");
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.commit();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 1
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        String select = "SELECT date,current_date() FROM ptsdb";
        ResultSet rs = conn.createStatement().executeQuery(select);
        Date then = new Date(System.currentTimeMillis());
        assertTrue(rs.next());
        Date date = DateUtil.parseDate(dateString);
        assertEquals(date,rs.getDate(1));
        assertTrue(rs.next());
        assertTrue(rs.getDate(1).after(now) && rs.getDate(1).before(then));
        assertFalse(rs.next());
    }
    
    @Test
    public void testUpsertValuesWithExpression() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),"IntKeyTest",null, ts-2);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1)); // Execute at timestamp 1
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        String upsert = "UPSERT INTO IntKeyTest VALUES(-1)";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        upsert = "UPSERT INTO IntKeyTest VALUES(1+2)";
        upsertStmt = conn.prepareStatement(upsert);
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 1
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        String select = "SELECT i FROM IntKeyTest";
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(-1,rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testUpsertValuesWithDate() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("create table UpsertDateTest (k VARCHAR not null primary key,date DATE)");
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+5));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into UpsertDateTest values ('a',to_date('2013-06-08 00:00:00'))");
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+10));
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select k,to_char(date) from UpsertDateTest");
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("2013-06-08 00:00:00", rs.getString(2));
    }

    @Test
    public void testUpsertVarCharWithMaxLength() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("create table phoenix_uuid_mac (mac_md5 VARCHAR not null primary key,raw_mac VARCHAR)");
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+5));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into phoenix_uuid_mac values ('00000000591','a')");
        conn.createStatement().execute("upsert into phoenix_uuid_mac values ('000000005919','b')");
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+10));
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select max(mac_md5) from phoenix_uuid_mac");
        assertTrue(rs.next());
        assertEquals("000000005919", rs.getString(1));
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+15));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into phoenix_uuid_mac values ('000000005919adfasfasfsafdasdfasfdasdfdasfdsafaxxf1','b')");
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+20));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("select max(mac_md5) from phoenix_uuid_mac");
        assertTrue(rs.next());
        assertEquals("000000005919adfasfasfsafdasdfasfdasdfdasfdsafaxxf1", rs.getString(1));
        conn.close();
    }
    
    @Test
    public void testUpsertValuesWithDescExpression() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("create table UpsertWithDesc (k VARCHAR not null primary key desc)");
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+5));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into UpsertWithDesc values (to_char(100))");
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+10));
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select to_number(k) from UpsertWithDesc");
        assertTrue(rs.next());
        assertEquals(100, rs.getInt(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testUpsertValuesWithMoreValuesThanNumColsInTable() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.createStatement();
            stmt.execute("create table UpsertWithDesc (k VARCHAR not null primary key desc)");
        } finally {
            closeStmtAndConn(stmt, conn);
        }

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+5));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.createStatement();
            stmt.execute("upsert into UpsertWithDesc values (to_char(100), to_char(100), to_char(100))");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.UPSERT_COLUMN_NUMBERS_MISMATCH.getErrorCode(),e.getErrorCode());
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }
    
    @Test
    public void testTimestampSerializedAndDeserializedCorrectly() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("create table UpsertTimestamp (a integer NOT NULL, t timestamp NOT NULL CONSTRAINT pk PRIMARY KEY (a, t))");
            stmt.execute();
        } finally {
            closeStmtAndConn(stmt, conn);
        }
        
        Timestamp ts1 = new Timestamp(120055);
        ts1.setNanos(ts1.getNanos() + 60);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into UpsertTimestamp values (1, ?)");
            stmt.setTimestamp(1, ts1);
            stmt.executeUpdate();
            conn.commit();
         } finally {
             closeStmtAndConn(stmt, conn);
        }
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select t from UpsertTimestamp where t = ?");
            stmt.setTimestamp(1, ts1);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(ts1, rs.getTimestamp(1));
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }
    
    @Test
    public void testTimestampAddSubtractArithmetic() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("create table UpsertTimestamp (a integer NOT NULL, t timestamp NOT NULL CONSTRAINT pk PRIMARY KEY (a, t))");
            stmt.execute();
        } finally {
            closeStmtAndConn(stmt, conn);
        }
        
        Timestamp ts1 = new Timestamp(120550);
        int extraNanos = 60;
        ts1.setNanos(ts1.getNanos() + extraNanos);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into UpsertTimestamp values (1, ?)");
            stmt.setTimestamp(1, ts1);
            stmt.executeUpdate();
            conn.commit();
        } finally {
             closeStmtAndConn(stmt, conn);
        }
        
        Timestamp ts2 = new Timestamp(ts1.getTime() + 500);
        ts2.setNanos(ts2.getNanos() + extraNanos + 10); //setting the extra nanos as well as what spilled over from timestamp millis.
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select (t + (500.0/(1*24*60*60*1000) + 10.0/(1*24*60*60*1000*1000000)))  from UpsertTimestamp LIMIT 1");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(ts2, rs.getTimestamp(1));
        } finally {
            closeStatement(stmt);
        }
        
        ts2 = new Timestamp(ts1.getTime() - 250);
        ts2.setNanos(ts2.getNanos() + extraNanos - 30); //setting the extra nanos as well as what spilled over from timestamp millis.
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        try {
            stmt = conn.prepareStatement("select (t - (250.0/(1*24*60*60*1000) + 30.0/(1*24*60*60*1000*1000000)))  from UpsertTimestamp LIMIT 1");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(ts2, rs.getTimestamp(1));
        } finally {
            closeStatement(stmt);
        }
        
        ts2 = new Timestamp(ts1.getTime() + 250);
        ts2.setNanos(ts2.getNanos() + extraNanos);
        try {
            stmt = conn.prepareStatement("select t from UpsertTimestamp where t = ? - 250.0/(1*24*60*60*1000) LIMIT 1");
            stmt.setTimestamp(1, ts2);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(ts1, rs.getTimestamp(1));
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }
    
    @Test
    public void testUpsertIntoFloat() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("create table UpsertFloat (k varchar primary key, v float)");
            stmt.execute();
        } finally {
            closeStmtAndConn(stmt, conn);
        }
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into UpsertFloat values ('a', 0.0)");
            stmt.executeUpdate();
            conn.commit();
        } finally {
             closeStmtAndConn(stmt, conn);
        }
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select * from UpsertFloat");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertTrue(Float.valueOf(0.0f).equals(rs.getFloat(2)));
            assertFalse(rs.next());
        } finally {
             closeStmtAndConn(stmt, conn);
        }
    }
        
}
