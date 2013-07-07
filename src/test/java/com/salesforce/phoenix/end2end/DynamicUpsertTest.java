/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc. All rights reserved. Redistribution and use in source and binary forms, with
 * or without modification, are permitted provided that the following conditions are met: Redistributions of source code
 * must retain the above copyright notice, this list of conditions and the following disclaimer. Redistributions in
 * binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution. Neither the name of Salesforce.com nor the names
 * of its contributors may be used to endorse or promote products derived from this software without specific prior
 * written permission. THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.PHOENIX_JDBC_URL;
import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.schema.ColumnAlreadyExistsException;
import com.salesforce.phoenix.schema.ColumnFamilyNotFoundException;

/**
 * Basic tests for Phoenix dynamic upserting
 * 
 * @author nmaillard
 * @since 1.3
 */

public class DynamicUpsertTest extends BaseClientMangedTimeTest {

    private static final String TABLE = "DynamicUpserts";
    //private static final byte[] TABLE_BYTES = Bytes.toBytes(TABLE);

    @BeforeClass
    public static void doBeforeTestSetup() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "create table if not exists  " + TABLE + "   (entry varchar not null primary key,"
                + "    a.dummy varchar," + "    b.dummy varchar)";
        conn.createStatement().execute(ddl);
        conn.close();
    }

    /**
     * Test a simple upsert with a dynamic Column
     */
    @Test
    public void testUpsert() throws Exception {
        String upsertquery = "UPSERT INTO " + TABLE
                + " (entry, a.DynCol VARCHAR,a.dummy) VALUES ('dynEntry','DynValue','DynColValue')";
        String selectquery = "SELECT DynCol FROM " + TABLE + " (a.DynCol VARCHAR) where entry='dynEntry'";
        // String selectquery = "SELECT * FROM "+TABLE;

        String url = PHOENIX_JDBC_URL + ";";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            int rowsInserted = statement.executeUpdate();
            assertEquals(1, rowsInserted);

            // since the upsert does not alter the schema check with a dynamicolumn
            PreparedStatement selectStatement = conn.prepareStatement(selectquery);
            ResultSet rs = selectStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals("DynValue", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * Test an upsert of multiple dynamic Columns
     */
    @Test
    public void testMultiUpsert() throws Exception {
        String upsertquery = "UPSERT INTO " + TABLE
                + " (entry, a.DynColA VARCHAR,b.DynColB varchar) VALUES('dynEntry','DynColValuea','DynColValueb')";
        String selectquery = "SELECT DynColA,entry,DynColB FROM " + TABLE
                + " (a.DynColA VARCHAR,b.DynColB VARCHAR) where entry='dynEntry'";

        String url = PHOENIX_JDBC_URL + ";";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            int rowsInserted = statement.executeUpdate();
            assertEquals(1, rowsInserted);

            // since the upsert does not alter the schema check with a dynamicolumn
            statement = conn.prepareStatement(selectquery);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("DynColValuea", rs.getString(1));
            assertEquals("dynEntry", rs.getString(2));
            assertEquals("DynColValueb", rs.getString(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * Test an upsert of a full row with dynamic Columns
     */
    @Test
    public void testFullUpsert() throws Exception {
        String upsertquery = "UPSERT INTO "
                + TABLE
                + " (a.DynColA VARCHAR,b.DynColB varchar) VALUES('dynEntry','aValue','bValue','DynColValuea','DynColValueb')";
        String selectquery = "SELECT entry,DynColA,a.dummy,DynColB,b.dummy FROM " + TABLE
                + " (a.DynColA VARCHAR,b.DynColB VARCHAR) where entry='dynEntry'";

        String url = PHOENIX_JDBC_URL + ";";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            int rowsInserted = statement.executeUpdate();
            assertEquals(1, rowsInserted);

            // since the upsert does not alter the schema check with a dynamicolumn
            statement = conn.prepareStatement(selectquery);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("dynEntry", rs.getString(1));
            assertEquals("DynColValuea", rs.getString(2));
            assertEquals("aValue", rs.getString(3));
            assertEquals("DynColValueb", rs.getString(4));
            assertEquals("bValue", rs.getString(5));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * Test an upsert of a full row with dynamic Columns and unbalanced number of values
     */
    @Test
    public void testFullUnbalancedUpsert() throws Exception {
        String upsertquery = "UPSERT INTO " + TABLE
                + " (a.DynCol VARCHAR,b.DynCol varchar) VALUES('dynEntry','aValue','bValue','dyncola')";

        String url = PHOENIX_JDBC_URL + ";";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            statement.execute();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.UPSERT_COLUMN_NUMBERS_MISMATCH.getErrorCode(),e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    /**
     * Test an upsert of prexisting schema defined columns and dynamic ones with different datatypes
     */
    @Test(expected = ColumnAlreadyExistsException.class)
    public void testAmbiguousStaticUpsert() throws Exception {
        String upsertquery = "UPSERT INTO " + TABLE + " (a.dummy INTEGER,b.dummy INTEGER) VALUES(1,2)";
        String url = PHOENIX_JDBC_URL + ";";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            statement.execute();
        } finally {
            conn.close();
        }
    }

    /**
     * Test an upsert of two conflicting dynamic columns
     */
    @Test(expected = ColumnAlreadyExistsException.class)
    public void testAmbiguousDynamicUpsert() throws Exception {
        String upsertquery = "UPSERT INTO " + TABLE + " (a.DynCol VARCHAR,a.DynCol INTEGER) VALUES('dynCol',1)";
        String url = PHOENIX_JDBC_URL + ";";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            statement.execute();
        } finally {
            conn.close();
        }
    }

    /**
     * Test an upsert of an undefined ColumnFamily dynamic columns
     */
    @Test(expected = ColumnFamilyNotFoundException.class)
    public void testFakeCFDynamicUpsert() throws Exception {
        String upsertquery = "UPSERT INTO " + TABLE + " (fakecf.DynCol VARCHAR) VALUES('dynCol')";
        String url = PHOENIX_JDBC_URL + ";";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            statement.execute();
        } finally {
            conn.close();
        }
    }
}
