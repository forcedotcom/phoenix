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
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.salesforce.phoenix.expression.function.ToNumberFunction;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.PhoenixRuntime;

/**
 * Tests for the TO_NUMBER built-in function.
 * 
 * @see ToNumberFunction
 * @author elevine
 * @since 0.1
 */
public class ToNumberFunctionTest extends BaseClientMangedTimeTest {
    
    public static final String TO_NUMBER_TABLE_NAME = "TO_NUMBER_TABLE";
    
    public static final String TO_NUMBER_TABLE_DDL = "create table " + TO_NUMBER_TABLE_NAME +
        "(a_id integer not null, \n" + 
        "a_string char(4) not null, \n" +
        "b_string char(4) not null \n" + 
        "CONSTRAINT my_pk PRIMARY KEY (a_id, a_string))";

    @Before
    public void initTable() throws Exception {
        long ts = nextTimestamp();
        createTestTable(getUrl(), TO_NUMBER_TABLE_DDL, null, ts-2);
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(false);
        
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " + TO_NUMBER_TABLE_NAME +
                "    (a_id, " +
                "    a_string," +
                "    b_string)" +
                "VALUES (?, ?, ?)");
        
        stmt.setInt(1, 1);
        stmt.setString(2, "   1");
        stmt.setString(3, "   1");
        stmt.execute();
        
        stmt.setInt(1, 2);
        stmt.setString(2, " 2.2");
        stmt.setString(3, " 2.2");
        stmt.execute();
        
        stmt.setInt(1, 3);
        stmt.setString(2, "$3.3");
        stmt.setString(3, "$3.3");
        stmt.execute();
        
        conn.commit();
        conn.close();
    }

    @Test
    public void testKeyFilterWithIntegerValue() throws Exception {
        String query = "SELECT a_id FROM " + TO_NUMBER_TABLE_NAME + " WHERE to_number(a_string) = 1";
        int expectedId = 1;
        runOneRowQueryTest(query, expectedId);
    }
    
    @Test
    public void testKeyFilterWithDoubleValue() throws Exception {
        String query = "SELECT a_id FROM " + TO_NUMBER_TABLE_NAME + " WHERE to_number(a_string) = 2.2";
        int expectedId = 2;
        runOneRowQueryTest(query, expectedId);
    }
    
    @Test
    public void testNonKeyFilterWithIntegerValue() throws Exception {
        String query = "SELECT a_id FROM " + TO_NUMBER_TABLE_NAME + " WHERE to_number(b_string) = 1";
        int expectedId = 1;
        runOneRowQueryTest(query, expectedId);
    }
    
    @Test
    public void testNonKeyFilterWithDoubleValue() throws Exception {
        String query = "SELECT a_id FROM " + TO_NUMBER_TABLE_NAME + " WHERE to_number(b_string) = 2.2";
        int expectedId = 2;
        runOneRowQueryTest(query, expectedId);
    }

    @Test
    public void testKeyProjectionWithIntegerValue() throws Exception {
        String query = "select to_number(a_string) from " + TO_NUMBER_TABLE_NAME + " where a_id = 1";
        int expectedIntValue = 1;
        runOneRowQueryTest(query, expectedIntValue);
    }
    
    @Test
    public void testKeyProjectionWithDecimalValue() throws Exception {
        String query = "select to_number(a_string) from " + TO_NUMBER_TABLE_NAME + " where a_id = 2";
        BigDecimal expectedDecimalValue = (BigDecimal)PDataType.DECIMAL.toObject("2.2");
        runOneRowQueryTest(query, expectedDecimalValue);
    }
    
    @Test
    public void testNonKeyProjectionWithIntegerValue() throws Exception {
        String query = "select to_number(b_string) from " + TO_NUMBER_TABLE_NAME + " where a_id = 1";
        int expectedIntValue = 1;
        runOneRowQueryTest(query, expectedIntValue);
    }
    
    @Test
    public void testNonKeyProjectionWithDecimalValue() throws Exception {
        String query = "select to_number(b_string) from " + TO_NUMBER_TABLE_NAME + " where a_id = 2";
        BigDecimal expectedDecimalValue = (BigDecimal)PDataType.DECIMAL.toObject("2.2");
        runOneRowQueryTest(query, expectedDecimalValue);
    }
    
    @Test
    public void testKeyFilterWithPatternParam() throws Exception {
        String query = "SELECT a_id FROM " + TO_NUMBER_TABLE_NAME + " WHERE to_number(a_string, '\u00A4###.####') = 3.3";
        int expectedId = 3;
        runOneRowQueryTest(query, expectedId);
    }
    
    @Test
    public void testNonKeyFilterWithPatternParam() throws Exception {
        String query = "SELECT a_id FROM " + TO_NUMBER_TABLE_NAME + " WHERE to_number(b_string, '\u00A4#.#') = 3.3";
        int expectedId = 3;
        runOneRowQueryTest(query, expectedId);
    }
    
    private void runOneRowQueryTest(String oneRowQuery, BigDecimal expectedDecimalValue) throws Exception {
    	runOneRowQueryTest(oneRowQuery, false, null, expectedDecimalValue);
    }
    
    private void runOneRowQueryTest(String oneRowQuery, int expectedIntValue) throws Exception {
    	runOneRowQueryTest(oneRowQuery, true, expectedIntValue, null);
    }
    
    private void runOneRowQueryTest(String oneRowQuery, boolean isIntegerColumn, Integer expectedIntValue, BigDecimal expectedDecimalValue) throws Exception {
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL);
        try {
            PreparedStatement statement = conn.prepareStatement(oneRowQuery);
            ResultSet rs = statement.executeQuery();
            
            assertTrue (rs.next());
            if (isIntegerColumn)
            	assertEquals(expectedIntValue.intValue(), rs.getInt(1));
            else
            	assertEquals(expectedDecimalValue, rs.getBigDecimal(1));
            assertFalse(rs.next());
        }
        finally {
        	conn.close();
        }
    }
}