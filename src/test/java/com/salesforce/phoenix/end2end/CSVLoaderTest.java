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

import static org.junit.Assert.*;

import java.io.StringReader;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.util.*;

import au.com.bytecode.opencsv.CSVReader;

public class CSVLoaderTest extends BaseHBaseManagedTimeTest {

	private static final String DATATYPE_TABLE = "DATATYPE";
	private static final String DATATYPES_CSV_VALUES = "CKEY, CVARCHAR, CINTEGER, CDECIMAL, CUNSIGNED_INT, CBOOLEAN, CBIGINT, CUNSIGNED_LONG, CTIME, CDATE\n" + 
			"KEY1,A,2147483647,1.1,0,TRUE,9223372036854775807,0,1990-12-31 10:59:59,1999-12-31 23:59:59\n" + 
			"KEY2,B,-2147483648,-1.1,2147483647,FALSE,-9223372036854775808,9223372036854775807,2000-01-01 00:00:01,2012-02-29 23:59:59\n";
	private static final String STOCK_TABLE = "STOCK_SYMBOL";
	private static final String STOCK_CSV_VALUES = 
			"AAPL,APPLE Inc.\n" + 
			"CRM,SALESFORCE\n" + 
			"GOOG,Google\n" + 
			"HOG,Harlet-Davidson Inc.\n" + 
			"HPQ,Hewlett Packard\n" + 
			"INTC,Intel\n" + 
			"MSFT,Microsoft\n" + 
			"WAG,Walgreens\n" + 
			"WMT,Walmart\n";
    private static final String[] STOCK_COLUMNS_WITH_BOGUS = new String[] {"SYMBOL", "BOGUS"};
    private static final String[] STOCK_COLUMNS = new String[] {"SYMBOL", "COMPANY"};
    private static final String STOCK_CSV_VALUES_WITH_HEADER =  STOCK_COLUMNS[0] + "," + STOCK_COLUMNS[1] + "\n" + STOCK_CSV_VALUES;

    
    @Test
    public void testCSVUpsert() throws Exception {
    	// Create table
        String statements = "CREATE TABLE IF NOT EXISTS " + STOCK_TABLE + "(SYMBOL VARCHAR NOT NULL PRIMARY KEY, COMPANY VARCHAR);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);
        
        // Upsert CSV file
        CSVLoader csvUtil = new CSVLoader(conn, STOCK_TABLE, Collections.<String>emptyList(), true);
		CSVReader reader = new CSVReader(new StringReader(STOCK_CSV_VALUES_WITH_HEADER));
        csvUtil.upsert(reader);

        // Compare Phoenix ResultSet with CSV file content
        PreparedStatement statement = conn.prepareStatement("SELECT SYMBOL, COMPANY FROM " + STOCK_TABLE);
        ResultSet phoenixResultSet = statement.executeQuery();
        reader = new CSVReader(new StringReader(STOCK_CSV_VALUES_WITH_HEADER));
        reader.readNext();
        String[] csvData;
        while ((csvData = reader.readNext()) != null) {
        	assertTrue (phoenixResultSet.next());
        	for (int i=0; i<csvData.length; i++) {
        		assertEquals(csvData[i], phoenixResultSet.getString(i+1));
        	}
        }
        
        assertFalse(phoenixResultSet.next());
        conn.close();
    }
    
    @Test
    public void testCSVUpsertWithColumns() throws Exception {
        // Create table
        String statements = "CREATE TABLE IF NOT EXISTS " + STOCK_TABLE + "(SYMBOL VARCHAR NOT NULL PRIMARY KEY, COMPANY VARCHAR);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);
        
        // Upsert CSV file
        CSVLoader csvUtil = new CSVLoader(conn, STOCK_TABLE, Arrays.<String>asList(STOCK_COLUMNS), true);
        CSVReader reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        csvUtil.upsert(reader);

        // Compare Phoenix ResultSet with CSV file content
        PreparedStatement statement = conn.prepareStatement("SELECT SYMBOL, COMPANY FROM " + STOCK_TABLE);
        ResultSet phoenixResultSet = statement.executeQuery();
        reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        String[] csvData;
        while ((csvData = reader.readNext()) != null) {
            assertTrue (phoenixResultSet.next());
            for (int i=0; i<csvData.length; i++) {
                assertEquals(csvData[i], phoenixResultSet.getString(i+1));
            }
        }
        
        assertFalse(phoenixResultSet.next());
        conn.close();
    }
    
    
    @Test
    public void testCSVUpsertWithNoColumns() throws Exception {
        // Create table
        String statements = "CREATE TABLE IF NOT EXISTS " + STOCK_TABLE + "(SYMBOL VARCHAR NOT NULL PRIMARY KEY, COMPANY VARCHAR);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);
        
        // Upsert CSV file
        CSVLoader csvUtil = new CSVLoader(conn, STOCK_TABLE, null, true);
        CSVReader reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        csvUtil.upsert(reader);

        // Compare Phoenix ResultSet with CSV file content
        PreparedStatement statement = conn.prepareStatement("SELECT SYMBOL, COMPANY FROM " + STOCK_TABLE);
        ResultSet phoenixResultSet = statement.executeQuery();
        reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        String[] csvData;
        while ((csvData = reader.readNext()) != null) {
            assertTrue (phoenixResultSet.next());
            for (int i=0; i<csvData.length; i++) {
                assertEquals(csvData[i], phoenixResultSet.getString(i+1));
            }
        }
        
        assertFalse(phoenixResultSet.next());
        conn.close();
    }
    
    @Test
    public void testCSVUpsertWithBogusColumn() throws Exception {
        // Create table
        String statements = "CREATE TABLE IF NOT EXISTS " + STOCK_TABLE + "(SYMBOL VARCHAR NOT NULL PRIMARY KEY, COMPANY VARCHAR);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);
        
        // Upsert CSV file
        CSVLoader csvUtil = new CSVLoader(conn, STOCK_TABLE, Arrays.asList(STOCK_COLUMNS_WITH_BOGUS), false);
        CSVReader reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        csvUtil.upsert(reader);

        // Compare Phoenix ResultSet with CSV file content
        PreparedStatement statement = conn.prepareStatement("SELECT SYMBOL, COMPANY FROM " + STOCK_TABLE);
        ResultSet phoenixResultSet = statement.executeQuery();
        reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        String[] csvData;
        while ((csvData = reader.readNext()) != null) {
            assertTrue (phoenixResultSet.next());
            assertEquals(csvData[0], phoenixResultSet.getString(1));
            assertNull(phoenixResultSet.getString(2));
        }
        
        assertFalse(phoenixResultSet.next());
        conn.close();
    }
    
    @Test
    public void testCSVUpsertWithAllColumn() throws Exception {
        // Create table
        String statements = "CREATE TABLE IF NOT EXISTS " + STOCK_TABLE + "(SYMBOL VARCHAR NOT NULL PRIMARY KEY, COMPANY VARCHAR);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);
        
        // Upsert CSV file
        CSVLoader csvUtil = new CSVLoader(conn, STOCK_TABLE, Arrays.asList("FOO","BAR"), false);
        CSVReader reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        try {
            csvUtil.upsert(reader);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 504 (42703): Undefined column. columnName=STOCK_SYMBOL.[FOO, BAR]"));
        }
        conn.close();
    }
    
    @Test
    public void testCSVUpsertWithBogusColumnStrict() throws Exception {
        // Create table
        String statements = "CREATE TABLE IF NOT EXISTS " + STOCK_TABLE + "(SYMBOL VARCHAR NOT NULL PRIMARY KEY, COMPANY VARCHAR);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);
        
        // Upsert CSV file
        CSVLoader csvUtil = new CSVLoader(conn, STOCK_TABLE, Arrays.asList(STOCK_COLUMNS_WITH_BOGUS), true);
        CSVReader reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        try {
            csvUtil.upsert(reader);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 504 (42703): Undefined column. columnName=STOCK_SYMBOL.BOGUS"));
        }
        conn.close();
    }

    @Test
    public void testAllDatatypes() throws Exception {
    	// Create table
        String statements = "CREATE TABLE IF NOT EXISTS " 
        	    + DATATYPE_TABLE +
        		" (CKEY VARCHAR NOT NULL PRIMARY KEY," +
        		"  CVARCHAR VARCHAR, CINTEGER INTEGER, CDECIMAL DECIMAL, CUNSIGNED_INT UNSIGNED_INT, CBOOLEAN BOOLEAN, CBIGINT BIGINT, CUNSIGNED_LONG UNSIGNED_LONG, CTIME TIME, CDATE DATE);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);
        
        // Upsert CSV file
        CSVLoader csvUtil = new CSVLoader(conn, DATATYPE_TABLE, Collections.<String>emptyList(), true); 
		CSVReader reader = new CSVReader(new StringReader(DATATYPES_CSV_VALUES));
        csvUtil.upsert(reader);

        // Compare Phoenix ResultSet with CSV file content
		PreparedStatement statement = conn
				.prepareStatement("SELECT CKEY, CVARCHAR, CINTEGER, CDECIMAL, CUNSIGNED_INT, CBOOLEAN, CBIGINT, CUNSIGNED_LONG, CTIME, CDATE FROM "
						+ DATATYPE_TABLE);
        ResultSet phoenixResultSet = statement.executeQuery();
        reader = new CSVReader(new StringReader(DATATYPES_CSV_VALUES));
        reader.readNext();
        String[] csvData;
        while ((csvData = reader.readNext()) != null) {
        	assertTrue (phoenixResultSet.next());
        	for (int i=0; i<csvData.length - 2; i++) {
        		assertEquals(csvData[i], phoenixResultSet.getObject(i+1).toString().toUpperCase());
        	}
        	// special case for matching date, time values
        	assertEquals(DateUtil.parseTime(csvData[8]), phoenixResultSet.getTime("CTIME"));
        	assertEquals(DateUtil.parseDate(csvData[9]), phoenixResultSet.getDate("CDATE"));
        }
        assertFalse(phoenixResultSet.next());
        conn.close();
    }
}