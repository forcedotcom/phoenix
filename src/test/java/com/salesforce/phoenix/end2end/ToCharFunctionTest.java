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
import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.*;
import java.text.*;
import java.util.Properties;
import java.util.TimeZone;

import org.junit.Before;
import org.junit.Test;

import com.salesforce.phoenix.expression.function.ToCharFunction;
import com.salesforce.phoenix.util.PhoenixRuntime;

/**
 * Tests for the TO_CHAR built-in function.
 * 
 * @see ToCharFunction
 * @author elevine
 * @since 0.1
 */
public class ToCharFunctionTest extends BaseClientMangedTimeTest {
    
    public static final String TO_CHAR_TABLE_NAME = "TO_CHAR_TABLE";
    
    private Date row1Date;
    private Timestamp row1Timestamp;
    private Integer row1Integer;
    private BigDecimal row1Decimal;
    private Date row2Date;
    private Timestamp row2Timestamp;
    private Integer row2Integer;
    private BigDecimal row2Decimal;
    
    public static final String TO_CHAR_TABLE_DDL = "create table " + TO_CHAR_TABLE_NAME +
        "(pk integer not null, \n" + 
        "col_date date not null, \n" +
        "col_timestamp timestamp not null, \n" +
        "col_integer integer not null, \n" + 
        "col_decimal decimal not null \n" + 
        "CONSTRAINT my_pk PRIMARY KEY (pk))";

    @Before
    public void initTable() throws Exception {
        long ts = nextTimestamp();
        createTestTable(getUrl(), TO_CHAR_TABLE_DDL, null, ts-2);
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(false);
        
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " + TO_CHAR_TABLE_NAME +
                "    (pk, " +
                "    col_date," +
                "    col_timestamp," +
                "    col_integer," +
                "    col_decimal)" +
                "VALUES (?, ?, ?, ?, ?)");
        
        row1Date = new Date(System.currentTimeMillis() - 10000);
        row1Timestamp = new Timestamp(System.currentTimeMillis() + 10000);
        row1Integer = 666;
        row1Decimal = new BigDecimal(33.333);
        
        stmt.setInt(1, 1);
        stmt.setDate(2, row1Date);
        stmt.setTimestamp(3, row1Timestamp);
        stmt.setInt(4, row1Integer);
        stmt.setBigDecimal(5, row1Decimal);
        stmt.execute();
        
        row2Date = new Date(System.currentTimeMillis() - 1234567);
        row2Timestamp = new Timestamp(System.currentTimeMillis() + 1234567);
        row2Integer = 10011;
        row2Decimal = new BigDecimal(123456789.66);
        
        stmt.setInt(1, 2);
        stmt.setDate(2, row2Date);
        stmt.setTimestamp(3, row2Timestamp);
        stmt.setInt(4, row2Integer);
        stmt.setBigDecimal(5, row2Decimal);
        
        stmt.execute();
        conn.commit();
        conn.close();
    }
    
    @Test
    public void testDateProjection() throws Exception {
        String pattern = "yyyy.MM.dd G HH:mm:ss z";
        String query = "select to_char(col_date, '" + pattern + "') from " + TO_CHAR_TABLE_NAME + " WHERE pk = 1";
        String expectedString = getGMTDateFormat(pattern).format(row1Date);
        runOneRowProjectionQuery(query, expectedString);
    }

    @Test
    public void testTimestampProjection() throws Exception {
        String pattern = "yyMMddHHmmssZ";
        String query = "select to_char(col_timestamp, '" + pattern + "') from " + TO_CHAR_TABLE_NAME + " WHERE pk = 2";
        String expectedString = getGMTDateFormat(pattern).format(row2Timestamp);
        runOneRowProjectionQuery(query, expectedString);
    }
    
    @Test
    public void testIntegerProjection() throws Exception {
        String pattern = "00";
        String query = "select to_char(col_integer, '" + pattern + "') from " + TO_CHAR_TABLE_NAME + " WHERE pk = 1";
        String expectedString = new DecimalFormat(pattern).format(row1Integer);
        runOneRowProjectionQuery(query, expectedString);
    }
    
    @Test
    public void testDecimalProjection() throws Exception {
        String pattern = "0.###E0";
        String query = "select to_char(col_decimal, '" + pattern + "') from " + TO_CHAR_TABLE_NAME + " WHERE pk = 2";
        String expectedString = new DecimalFormat(pattern).format(row2Decimal);
        runOneRowProjectionQuery(query, expectedString);
    }
    
    @Test 
    public void testDateFilter() throws Exception {
        String pattern = "yyyyMMddHHmmssZ";
        String expectedString = getGMTDateFormat(pattern).format(row1Date);
        String query = "select pk from " + TO_CHAR_TABLE_NAME + " WHERE to_char(col_date, '" + pattern + "') = '" + expectedString + "'";
        runOneRowFilterQuery(query, 1);
    }
    
    @Test 
    public void testTimestampFilter() throws Exception {
        String pattern = "yy.MM.dd G HH:mm:ss z";
        String expectedString = getGMTDateFormat(pattern).format(row2Timestamp);
        String query = "select pk from " + TO_CHAR_TABLE_NAME + " WHERE to_char(col_timestamp, '" + pattern + "') = '" + expectedString + "'";
        runOneRowFilterQuery(query, 2);
    }
    
    @Test 
    public void testIntegerFilter() throws Exception {
        String pattern = "000";
        String expectedString = new DecimalFormat(pattern).format(row1Integer);
        String query = "select pk from " + TO_CHAR_TABLE_NAME + " WHERE to_char(col_integer, '" + pattern + "') = '" + expectedString + "'";
        runOneRowFilterQuery(query, 1);
    }
    
    @Test 
    public void testDecimalFilter() throws Exception {
        String pattern = "00.###E0";
        String expectedString = new DecimalFormat(pattern).format(row2Decimal);
        String query = "select pk from " + TO_CHAR_TABLE_NAME + " WHERE to_char(col_decimal, '" + pattern + "') = '" + expectedString + "'";
        runOneRowFilterQuery(query, 2);
    }
    
    private void runOneRowProjectionQuery(String oneRowQuery, String projectedValue) throws Exception {
    	runOneRowQueryTest(oneRowQuery, null, projectedValue);
    }
    
    private void runOneRowFilterQuery(String oneRowQuery, int pkValue) throws Exception {
    	runOneRowQueryTest(oneRowQuery, pkValue, null);
    }
    
    private void runOneRowQueryTest(String oneRowQuery, Integer pkValue, String projectedValue) throws Exception {
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL);
        try {
            PreparedStatement statement = conn.prepareStatement(oneRowQuery);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            if (pkValue != null)
            	assertEquals(pkValue.intValue(), rs.getInt(1));
            else
            	assertEquals(projectedValue, rs.getString(1));
            assertFalse(rs.next());
        }
        finally {
        	conn.close();
        }
    }
    
    private DateFormat getGMTDateFormat(String pattern) {
        DateFormat result = new SimpleDateFormat(pattern);
        result.setTimeZone(TimeZone.getTimeZone("GMT"));
        return result;
    }
}