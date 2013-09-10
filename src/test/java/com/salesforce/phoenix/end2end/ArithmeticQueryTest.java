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

import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;

import org.junit.Test;


public class ArithmeticQueryTest extends BaseHBaseManagedTimeTest {

    @Test
    public void testDecimalUpsertValue() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE IF NOT EXISTS testDecimalArithmetic" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, " +
                    "col1 DECIMAL(31,0), col2 DECIMAL(5), col3 DECIMAL(5,2), col4 DECIMAL)";
            createTestTable(getUrl(), ddl);
            
            // Test upsert correct values 
            String query = "UPSERT INTO testDecimalArithmetic(pk, col1, col2, col3, col4) VALUES(?,?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "valueOne");
            stmt.setBigDecimal(2, new BigDecimal("123456789123456789"));
            stmt.setBigDecimal(3, new BigDecimal("12345"));
            stmt.setBigDecimal(4, new BigDecimal("12.34"));
            stmt.setBigDecimal(5, new BigDecimal("12345.6789"));
            stmt.execute();
            conn.commit();
            
            query = "SELECT col1, col2, col3, col4 FROM testDecimalArithmetic WHERE pk = 'valueOne'";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("123456789123456789"), rs.getBigDecimal(1));
            assertEquals(new BigDecimal("12345"), rs.getBigDecimal(2));
            assertEquals(new BigDecimal("12.34"), rs.getBigDecimal(3));
            assertEquals(new BigDecimal("12345.6789"), rs.getBigDecimal(4));
            assertFalse(rs.next());
            
            query = "UPSERT INTO testDecimalArithmetic(pk, col1, col2, col3) VALUES(?,?,?,?)";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, "valueTwo");
            stmt.setBigDecimal(2, new BigDecimal("1234567890123456789012345678901.12345"));
            stmt.setBigDecimal(3, new BigDecimal("12345.6789"));
            stmt.setBigDecimal(4, new BigDecimal("123.45678"));
            stmt.execute();
            conn.commit();
            
            query = "SELECT col1, col2, col3 FROM testDecimalArithmetic WHERE pk = 'valueTwo'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("1234567890123456789012345678901"), rs.getBigDecimal(1));
            assertEquals(new BigDecimal("12345"), rs.getBigDecimal(2));
            assertEquals(new BigDecimal("123.45"), rs.getBigDecimal(3));
            assertFalse(rs.next());
            
            // Test upsert incorrect values and confirm exceptions would be thrown.
            try {
                query = "UPSERT INTO testDecimalArithmetic(pk, col1, col2, col3) VALUES(?,?,?,?)";
                stmt = conn.prepareStatement(query);
                stmt.setString(1, "badValues");
                // one more than max_precision
                stmt.setBigDecimal(2, new BigDecimal("12345678901234567890123456789012"));
                stmt.setBigDecimal(3, new BigDecimal("12345")); 
                stmt.setBigDecimal(4, new BigDecimal("123.45"));
                stmt.execute();
                conn.commit();
                fail("Should have caught bad values.");
            } catch (Exception e) {
                assertTrue(e.getMessage(), e.getMessage().contains("ERROR 206 (22003): The value is outside the range for the data type. value=12345678901234567890123456789012 columnName=COL1"));
            }
            try {
                query = "UPSERT INTO testDecimalArithmetic(pk, col1, col2, col3) VALUES(?,?,?,?)";
                stmt = conn.prepareStatement(query);
                stmt.setString(1, "badValues");
                stmt.setBigDecimal(2, new BigDecimal("123456"));
                // Exceeds specified precision by 1
                stmt.setBigDecimal(3, new BigDecimal("123456"));
                stmt.setBigDecimal(4, new BigDecimal("123.45"));
                stmt.execute();
                conn.commit();
                fail("Should have caught bad values.");
            } catch (Exception e) {
                assertTrue(e.getMessage(), e.getMessage().contains("ERROR 206 (22003): The value is outside the range for the data type. value=123456 columnName=COL2"));
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDecimalUpsertSelect() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE IF NOT EXISTS source" + 
                    " (pk VARCHAR NOT NULL PRIMARY KEY, col1 DECIMAL(5,2), col2 DECIMAL(5,1), col3 DECIMAL(5,2), col4 DECIMAL(4,4))";
            createTestTable(getUrl(), ddl);
            ddl = "CREATE TABLE IF NOT EXISTS target" + 
                    " (pk VARCHAR NOT NULL PRIMARY KEY, col1 DECIMAL(5,1), col2 DECIMAL(5,2), col3 DECIMAL(4,4))";
            createTestTable(getUrl(), ddl);
            
            String query = "UPSERT INTO source(pk, col1) VALUES(?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "1");
            stmt.setBigDecimal(2, new BigDecimal("100.12"));
            stmt.execute();
            conn.commit();
            stmt.setString(1, "2");
            stmt.setBigDecimal(2, new BigDecimal("100.34"));
            stmt.execute();
            conn.commit();
            
            // Evaluated on client side.
            // source and target in different tables, values scheme compatible.
            query = "UPSERT INTO target(pk, col2) SELECT pk, col1 from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT col2 FROM target";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.12"), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.34"), rs.getBigDecimal(1));
            assertFalse(rs.next());
            // source and target in different tables, values requires scale chopping.
            query = "UPSERT INTO target(pk, col1) SELECT pk, col1 from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT col1 FROM target";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.1"), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.3"), rs.getBigDecimal(1));
            assertFalse(rs.next());
            // source and target in different tables, values scheme incompatible.
            try {
                query = "UPSERT INTO target(pk, col3) SELECT pk, col1 from source";
                stmt = conn.prepareStatement(query);
                stmt.execute();
                conn.commit();
                fail("Should have caught bad upsert.");
            } catch (Exception e) {
                assertTrue(e.getMessage(), e.getMessage().contains("ERROR 206 (22003): The value is outside the range for the data type. columnName=COL3"));
            }
            
            // Evaluate on server side.
            conn.setAutoCommit(true);
            // source and target in same table, values scheme compatible.
            query = "UPSERT INTO source(pk, col3) SELECT pk, col1 from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT col3 FROM source";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.12"), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.34"), rs.getBigDecimal(1));
            assertFalse(rs.next());
            // source and target in same table, values requires scale chopping.
            query = "UPSERT INTO source(pk, col2) SELECT pk, col1 from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT col2 FROM source";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.1"), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.3"), rs.getBigDecimal(1));
            assertFalse(rs.next());
            // source and target in same table, values scheme incompatible.
            query = "UPSERT INTO source(pk, col4) SELECT pk, col1 from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT col4 FROM source";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertNull(rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertNull(rs.getBigDecimal(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDecimalAveraging() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE IF NOT EXISTS testDecimalArithmatic" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, col1 DECIMAL(31, 11), col2 DECIMAL(31,1), col3 DECIMAL(38,1))";
            createTestTable(getUrl(), ddl);
            
            String query = "UPSERT INTO testDecimalArithmatic(pk, col1, col2, col3) VALUES(?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "1");
            stmt.setBigDecimal(2, new BigDecimal("99999999999999999999.1"));
            stmt.setBigDecimal(3, new BigDecimal("99999999999999999999.1"));
            stmt.setBigDecimal(4, new BigDecimal("9999999999999999999999999999999999999.1"));
            stmt.execute();
            conn.commit();
            stmt.setString(1, "2");
            stmt.setBigDecimal(2, new BigDecimal("0"));
            stmt.setBigDecimal(3, new BigDecimal("0"));
            stmt.setBigDecimal(4, new BigDecimal("0"));
            stmt.execute();
            conn.commit();
            stmt.setString(1, "3");
            stmt.setBigDecimal(2, new BigDecimal("0"));
            stmt.setBigDecimal(3, new BigDecimal("0"));
            stmt.setBigDecimal(4, new BigDecimal("0"));
            stmt.execute();
            conn.commit();
            
            // Averaging
            // result scale should be: max(max(ls, rs), 4).
            // We are not imposing restriction on precisioin.
            query = "SELECT avg(col1) FROM testDecimalArithmatic";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            BigDecimal result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("33333333333333333333.03333333333"), result);
            
            query = "SELECT avg(col2) FROM testDecimalArithmatic";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("33333333333333333333.0333"), result);
            
            // We cap our decimal to a precision of 38.
            query = "SELECT avg(col3) FROM testDecimalArithmatic";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("3333333333333333333333333333333333333"), result);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDecimalArithmeticWithIntAndLong() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE IF NOT EXISTS testDecimalArithmatic" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, " +
                    "col1 DECIMAL(38,0), col2 DECIMAL(5, 2), col3 INTEGER, col4 BIGINT, col5 DECIMAL)";
            createTestTable(getUrl(), ddl);
            
            String query = "UPSERT INTO testDecimalArithmatic(pk, col1, col2, col3, col4, col5) VALUES(?,?,?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "testValueOne");
            stmt.setBigDecimal(2, new BigDecimal("1234567890123456789012345678901"));
            stmt.setBigDecimal(3, new BigDecimal("123.45"));
            stmt.setInt(4, 10);
            stmt.setLong(5, 10L);
            stmt.setBigDecimal(6, new BigDecimal("111.111"));
            stmt.execute();
            conn.commit();

            stmt.setString(1, "testValueTwo");
            stmt.setBigDecimal(2, new BigDecimal("12345678901234567890123456789012345678"));
            stmt.setBigDecimal(3, new BigDecimal("123.45"));
            stmt.setInt(4, 10);
            stmt.setLong(5, 10L);
            stmt.setBigDecimal(6, new BigDecimal("123456789.0123456789"));
            stmt.execute();
            conn.commit();
            
            // INT has a default precision and scale of (10, 0)
            // LONG has a default precision and scale of (19, 0)
            query = "SELECT col1 + col3 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            BigDecimal result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1234567890123456789012345678911"), result);
            
            query = "SELECT col1 + col4 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1234567890123456789012345678911"), result);
            
            query = "SELECT col2 + col3 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("133.45"), result);
            
            query = "SELECT col2 + col4 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("133.45"), result);
            
            query = "SELECT col5 + col3 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("121.111"), result);
            
            query = "SELECT col5 + col4 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("121.111"), result);
            
            query = "SELECT col1 - col3 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1234567890123456789012345678891"), result);
            
            query = "SELECT col1 - col4 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1234567890123456789012345678891"), result);
            
            query = "SELECT col2 - col3 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("113.45"), result);
            
            query = "SELECT col2 - col4 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("113.45"), result);
            
            query = "SELECT col5 - col3 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("101.111"), result);
            
            query = "SELECT col5 - col4 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("101.111"), result);
            
            query = "SELECT col1 * col3 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1.234567890123456789012345678901E+31"), result);
            
            query = "SELECT col1 * col4 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1.234567890123456789012345678901E+31"), result);

            query = "SELECT col1 * col3 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1.234567890123456789012345678901E+31"), result);
            
            try {
            	query = "SELECT col1 * col3 FROM testDecimalArithmatic WHERE pk='testValueTwo'";
            	stmt = conn.prepareStatement(query);
            	rs = stmt.executeQuery();
            	assertTrue(rs.next());
            	result = rs.getBigDecimal(1);
            	fail("Should have caught error.");
            } catch (Exception e) {
            	assertTrue(e.getMessage(), e.getMessage().contains("ERROR 206 (22003): The value is outside the range for the data type. DECIMAL(38,0)"));
            }
            
            try {
            	query = "SELECT col1 * col4 FROM testDecimalArithmatic WHERE pk='testValueTwo'";
            	stmt = conn.prepareStatement(query);
            	rs = stmt.executeQuery();
            	assertTrue(rs.next());
            	result = rs.getBigDecimal(1);
            	fail("Should have caught error.");
            } catch (Exception e) {
            	assertTrue(e.getMessage(), e.getMessage().contains("ERROR 206 (22003): The value is outside the range for the data type. DECIMAL(38,0)"));
            }
            
            query = "SELECT col4 * col5 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(0, result.compareTo(new BigDecimal("1111.11")));

            query = "SELECT col3 * col5 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(0, result.compareTo(new BigDecimal("1111.11")));
            
            query = "SELECT col2 * col4 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1234.5"), result);
            
            // Result scale has value of 0
            query = "SELECT col1 / col3 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1.2345678901234567890123456789E+29"), result);
            
            query = "SELECT col1 / col4 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1.2345678901234567890123456789E+29"), result);
            
            // Result scale is 2.
            query = "SELECT col2 / col3 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("12.34"), result);
            
            query = "SELECT col2 / col4 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("12.34"), result);
            
            // col5 has NO_SCALE, so the result's scale is not expected to be truncated to col5 value's scale of 4
            query = "SELECT col5 / col3 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("11.1111"), result);
            
            query = "SELECT col5 / col4 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("11.1111"), result);
        } finally {
            conn.close();
        }
    }
}