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
package phoenix.end2end;

import static org.junit.Assert.*;

import java.io.StringReader;
import java.sql.*;

import org.junit.Test;

import au.com.bytecode.opencsv.CSVReader;

import phoenix.jdbc.PhoenixConnection;
import phoenix.util.CSVUtil;
import phoenix.util.DateUtil;
import phoenix.util.PhoenixRuntime;

public class CSVUpsertTest extends BaseHBaseManagedTimeTest {

	private static final String DATATYPE_TABLE = "DATATYPE";
	private static final String DATATYPES_CSV_VALUES = "CKEY, CVARCHAR, CINTEGER, CDECIMAL, CUNSIGNED_INT, CBOOLEAN, CBIGINT, CUNSIGNED_LONG, CTIME, CDATE\n" + 
			"KEY1,A,2147483647,1.1,0,TRUE,9223372036854775807,0,1990-12-31 10:59:59,1999-12-31 23:59:59\n" + 
			"KEY2,B,-2147483648,-1.1,2147483647,FALSE,-9223372036854775808,9223372036854775807,2000-01-01 00:00:01,2012-02-29 23:59:59\n";
	private static final String STOCK_TABLE = "STOCK_SYMBOL";
	private static final String STOCK_CSV_VALUES = "SYMBOL, COMPANY\n" + 
			"AAPL,APPLE Inc.\n" + 
			"CRM,SALESFORCE\n" + 
			"GOOG,Google\n" + 
			"HOG,Harlet-Davidson Inc.\n" + 
			"HPQ,Hewlett Packard\n" + 
			"INTC,Intel\n" + 
			"MSFT,Microsoft\n" + 
			"WAG,Walgreens\n" + 
			"WMT,Walmart\n";
    
    @Test
    public void testCSVUpsert() throws Exception {
    	// Create table
        String statements = "CREATE TABLE IF NOT EXISTS " + STOCK_TABLE + "(SYMBOL VARCHAR NOT NULL) CF (COMPANY VARCHAR);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);
        
        // Upsert CSV file
        CSVUtil csvUtil = new CSVUtil(conn, STOCK_TABLE);
		CSVReader reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        csvUtil.upsert(reader);

        // Compare Phoenix ResultSet with CSV file content
        PreparedStatement statement = conn.prepareStatement("SELECT SYMBOL, COMPANY FROM " + STOCK_TABLE);
        ResultSet phoenixResultSet = statement.executeQuery();
        reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
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
    public void testAllDatatypes() throws Exception {
    	// Create table
        String statements = "CREATE TABLE IF NOT EXISTS " 
        	    + DATATYPE_TABLE +
        		" (CKEY VARCHAR NOT NULL) CF" +
        		" (CVARCHAR VARCHAR, CINTEGER INTEGER, CDECIMAL DECIMAL, CUNSIGNED_INT UNSIGNED_INT, CBOOLEAN BOOLEAN, CBIGINT BIGINT, CUNSIGNED_LONG UNSIGNED_LONG, CTIME TIME, CDATE DATE);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);
        
        // Upsert CSV file
        CSVUtil csvUtil = new CSVUtil(conn, DATATYPE_TABLE); 
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