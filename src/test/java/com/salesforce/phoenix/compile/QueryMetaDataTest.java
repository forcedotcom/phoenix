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
package com.salesforce.phoenix.compile;

import static com.salesforce.phoenix.util.TestUtil.PHOENIX_JDBC_URL;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.*;

import org.junit.Test;

import com.salesforce.phoenix.test.BaseConnectionlessQueryTest;
import com.salesforce.phoenix.util.TestUtil;



/**
 * 
 * Tests for getting PreparedStatement meta data
 *
 * @author jtaylor
 * @since 0.1
 */
public class QueryMetaDataTest extends BaseConnectionlessQueryTest {

    @Test
    public void testNoParameterMetaData() throws Exception {
        String query = "SELECT a_string, b_string FROM atable WHERE organization_id='000000000000000'";
        Connection conn = DriverManager.getConnection(getUrl(), TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(0, pmd.getParameterCount());
    }

    @Test
    public void testCaseInsensitive() throws Exception {
        String query = "SELECT A_string, b_striNG FROM ataBle WHERE ORGANIZATION_ID='000000000000000'";
        Connection conn = DriverManager.getConnection(getUrl(), TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(0, pmd.getParameterCount());
    }

    @Test
    public void testParameterMetaData() throws Exception {
        String query = "SELECT a_string, b_string FROM atable WHERE organization_id=? and (a_integer = ? or a_date = ? or b_string = ? or a_string = 'foo')";
        Connection conn = DriverManager.getConnection(getUrl(), TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(4, pmd.getParameterCount());
        assertEquals(String.class.getName(), pmd.getParameterClassName(1));
        assertEquals(Integer.class.getName(), pmd.getParameterClassName(2));
        assertEquals(Date.class.getName(), pmd.getParameterClassName(3));
        assertEquals(String.class.getName(), pmd.getParameterClassName(4));
    }

    @Test
    public void testUpsertParameterMetaData() throws Exception {
        String query = "UPSERT INTO atable VALUES (?, ?, ?, ?, ?)";
        Connection conn = DriverManager.getConnection(getUrl(), TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(5, pmd.getParameterCount());
        assertEquals(String.class.getName(), pmd.getParameterClassName(1));
        assertEquals(String.class.getName(), pmd.getParameterClassName(2));
        assertEquals(String.class.getName(), pmd.getParameterClassName(3));
        assertEquals(String.class.getName(), pmd.getParameterClassName(4));
        assertEquals(Integer.class.getName(), pmd.getParameterClassName(5));
    }

    @Test
    public void testToDateFunctionMetaData() throws Exception {
        String query = "SELECT a_string, b_string FROM atable WHERE a_date > to_date(?)";
        Connection conn = DriverManager.getConnection(getUrl(), TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(1, pmd.getParameterCount());
        assertEquals(String.class.getName(), pmd.getParameterClassName(1));
    }

    @Test
    public void testLimitParameterMetaData() throws Exception {
        String query = "SELECT a_string, b_string FROM atable WHERE organization_id=? and a_string = 'foo' LIMIT ?";
        Connection conn = DriverManager.getConnection(getUrl(), TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(2, pmd.getParameterCount());
        assertEquals(String.class.getName(), pmd.getParameterClassName(1));
        assertEquals(Integer.class.getName(), pmd.getParameterClassName(2));
    }

    @Test
    public void testRoundParameterMetaData() throws Exception {
        String query = "SELECT a_string, b_string FROM atable WHERE round(a_date,?,?) = ?";
        Connection conn = DriverManager.getConnection(getUrl(), TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(3, pmd.getParameterCount());
        assertEquals(String.class.getName(), pmd.getParameterClassName(1));
        assertEquals(Integer.class.getName(), pmd.getParameterClassName(2));
        assertEquals(Date.class.getName(), pmd.getParameterClassName(3));
    }

    @Test
    public void testInListParameterMetaData1() throws Exception {
        String query = "SELECT a_string, b_string FROM atable WHERE a_string IN (?, ?)";
        Connection conn = DriverManager.getConnection(getUrl(), TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(2, pmd.getParameterCount());
        assertEquals(String.class.getName(), pmd.getParameterClassName(1));
        assertEquals(String.class.getName(), pmd.getParameterClassName(2));
    }

    @Test
    public void testInListParameterMetaData2() throws Exception {
        String query = "SELECT a_string, b_string FROM atable WHERE ? IN (2.2, 3)";
        Connection conn = DriverManager.getConnection(getUrl(), TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(1, pmd.getParameterCount());
        assertEquals(BigDecimal.class.getName(), pmd.getParameterClassName(1));
    }

    @Test
    public void testInListParameterMetaData3() throws Exception {
        String query = "SELECT a_string, b_string FROM atable WHERE ? IN ('foo')";
        Connection conn = DriverManager.getConnection(getUrl(), TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(1, pmd.getParameterCount());
        assertEquals(String.class.getName(), pmd.getParameterClassName(1));
    }

    @Test
    public void testInListParameterMetaData4() throws Exception {
        String query = "SELECT a_string, b_string FROM atable WHERE ? IN (?, ?)";
        Connection conn = DriverManager.getConnection(getUrl(), TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(3, pmd.getParameterCount());
        assertEquals(null, pmd.getParameterClassName(1));
        assertEquals(null, pmd.getParameterClassName(2));
        assertEquals(null, pmd.getParameterClassName(3));
    }

    @Test
    public void testCaseMetaData() throws Exception {
        String query1 = "SELECT a_string, b_string FROM atable WHERE case when a_integer = 1 then ? when a_integer > 2 then 2 end > 3";
        Connection conn = DriverManager.getConnection(getUrl(), TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query1);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(1, pmd.getParameterCount());
        assertEquals(BigDecimal.class.getName(), pmd.getParameterClassName(1));
        assertEquals(ParameterMetaData.parameterNullable, pmd.isNullable(1));
        
        String query2 = "SELECT a_string, b_string FROM atable WHERE case when a_integer = 1 then 1 when a_integer > 2 then 2 end > ?";
        PreparedStatement statement2 = conn.prepareStatement(query2);
        ParameterMetaData pmd2 = statement2.getParameterMetaData();
        assertEquals(1, pmd2.getParameterCount());
        assertEquals(Integer.class.getName(), pmd2.getParameterClassName(1));
        assertEquals(ParameterMetaData.parameterNullable, pmd2.isNullable(1));
    }

    @Test
    public void testSubstrParameterMetaData() throws Exception {
        String query = "SELECT a_string, b_string FROM atable WHERE substr(a_string,?,?) = ?";
        Connection conn = DriverManager.getConnection(getUrl(), TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(3, pmd.getParameterCount());
        assertEquals(Long.class.getName(), pmd.getParameterClassName(1));
        assertEquals(Long.class.getName(), pmd.getParameterClassName(2));
        assertEquals(String.class.getName(), pmd.getParameterClassName(3));
    }

    @Test
    public void testKeyPrefixParameterMetaData() throws Exception {
        String query = "SELECT a_string, b_string FROM atable WHERE organization_id='000000000000000' and substr(entity_id,1,3)=? and a_string = 'foo'";
        Connection conn = DriverManager.getConnection(getUrl(), TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(1, pmd.getParameterCount());
        assertEquals(String.class.getName(), pmd.getParameterClassName(1));
    }
    
    @Test
    public void testDateSubstractExpressionMetaData1() throws Exception {
        final String DS4 = "1970-01-01 01:45:00";
        String query = "SELECT entity_id,a_string FROM atable where a_date-2.5-?=a_date";
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, DS4);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(1, pmd.getParameterCount());
        assertEquals(BigDecimal.class.getName(), pmd.getParameterClassName(1));
    }

    @Test
    public void testDateSubstractExpressionMetaData2() throws Exception {
        final String DS4 = "1970-01-01 01:45:00";
        String query = "SELECT entity_id,a_string FROM atable where a_date-?=a_date";
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, DS4);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(1, pmd.getParameterCount());
        // FIXME: Should really be Date, but we currently don't know if we're 
        // comparing to a date or a number where this is being calculated 
        // (which would disambiguate it).
        assertEquals(null, pmd.getParameterClassName(1));
    }

    @Test
    public void testDateSubstractExpressionMetaData3() throws Exception {
        final String DS4 = "1970-01-01 01:45:00";
        String query = "SELECT entity_id,a_string FROM atable where a_date-?=a_integer";
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, DS4);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(1, pmd.getParameterCount());
        // FIXME: Should really be Integer, but we currently don't know if we're 
        // comparing to a date or a number where this is being calculated 
        // (which would disambiguate it).
        assertEquals(null, pmd.getParameterClassName(1));
    }

    @Test
    public void testTwoDateSubstractExpressionMetaData() throws Exception {
        final String DS4 = "1970-01-01 01:45:00";
        String query = "SELECT entity_id,a_string FROM atable where ?-a_date=1";
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, DS4);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(1, pmd.getParameterCount());
        // We know this must be date - anything else would be an error
        assertEquals(Date.class.getName(), pmd.getParameterClassName(1));
    }

    @Test
    public void testDateAdditionExpressionMetaData1() throws Exception {
        final String DS4 = "1970-01-01 01:45:00";
        String query = "SELECT entity_id,a_string FROM atable where 1+a_date+?>a_date";
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, DS4);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(1, pmd.getParameterCount());
        assertEquals(BigDecimal.class.getName(), pmd.getParameterClassName(1));
    }

    @Test
    public void testDateAdditionExpressionMetaData2() throws Exception {
        final String DS4 = "1970-01-01 01:45:00";
        String query = "SELECT entity_id,a_string FROM atable where ?+a_date>a_date";
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, DS4);
        ParameterMetaData pmd = statement.getParameterMetaData();
        assertEquals(1, pmd.getParameterCount());
        assertEquals(BigDecimal.class.getName(), pmd.getParameterClassName(1));
    }

    @Test
    public void testCoerceToDecimalArithmeticMetaData() throws Exception {
        String[] ops = { "+", "-", "*", "/" };
        for (String op : ops) {
            String query = "SELECT entity_id,a_string FROM atable where a_integer" + op + "2.5" + op + "?=0";
            Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, TestUtil.TEST_PROPERTIES);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setInt(1, 4);
            ParameterMetaData pmd = statement.getParameterMetaData();
            assertEquals(1, pmd.getParameterCount());
            assertEquals(BigDecimal.class.getName(), pmd.getParameterClassName(1));
        }
    }

    @Test
    public void testLongArithmeticMetaData() throws Exception {
        String[] ops = { "+", "-", "*", "/" };
        for (String op : ops) {
            String query = "SELECT entity_id,a_string FROM atable where a_integer" + op + "2" + op + "?=0";
            Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, TestUtil.TEST_PROPERTIES);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setInt(1, 4);
            ParameterMetaData pmd = statement.getParameterMetaData();
            assertEquals(1, pmd.getParameterCount());
            assertEquals(Long.class.getName(), pmd.getParameterClassName(1));
        }
    }

    @Test
    public void testBasicResultSetMetaData() throws Exception {
        String query = "SELECT organization_id, a_string, b_string, a_integer i, a_date FROM atable WHERE organization_id='000000000000000' and substr(entity_id,1,3)=? and a_string = 'foo'";
        Connection conn = DriverManager.getConnection(getUrl(), TestUtil.TEST_PROPERTIES);
        PreparedStatement statement = conn.prepareStatement(query);
        ResultSetMetaData md = statement.getMetaData();
        assertEquals(5, md.getColumnCount());
        
        assertEquals("organization_id".toUpperCase(),md.getColumnName(1));
        assertEquals("a_string".toUpperCase(),md.getColumnName(2));
        assertEquals("b_string".toUpperCase(),md.getColumnName(3));
        assertEquals("i".toUpperCase(),md.getColumnName(4));
        assertEquals("a_date".toUpperCase(),md.getColumnName(5));
        
        assertEquals(String.class.getName(),md.getColumnClassName(1));
        assertEquals(String.class.getName(),md.getColumnClassName(2));
        assertEquals(String.class.getName(),md.getColumnClassName(3));
        assertEquals(Integer.class.getName(),md.getColumnClassName(4));
        assertEquals(Date.class.getName(),md.getColumnClassName(5));
        
        assertEquals("atable".toUpperCase(),md.getTableName(1));
        assertEquals(java.sql.Types.INTEGER,md.getColumnType(4));
        assertEquals(true,md.isReadOnly(1));
        assertEquals(false,md.isDefinitelyWritable(1));
        assertEquals("i".toUpperCase(),md.getColumnLabel(4));
        assertEquals("a_date".toUpperCase(),md.getColumnLabel(5));
        assertEquals(ResultSetMetaData.columnNoNulls,md.isNullable(1));
        assertEquals(ResultSetMetaData.columnNullable,md.isNullable(5));
    }
    @Test
    public void testStringConcatMetaData() throws Exception {
    	String query = "SELECT entity_id,a_string FROM atable where 2 || a_integer || ? like '2%'";
    	Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, TestUtil.TEST_PROPERTIES);
    	PreparedStatement statement = conn.prepareStatement(query);
    	statement.setString(1, "foo");
    	ParameterMetaData pmd = statement.getParameterMetaData();
    	assertEquals(1, pmd.getParameterCount());
    	assertEquals(String.class.getName(), pmd.getParameterClassName(1));

    }
}
