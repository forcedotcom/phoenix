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
package com.salesforce.phoenix.arithmatic;

import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.end2end.BaseHBaseManagedTimeTest;


public class ArithmeticOperationTest extends BaseHBaseManagedTimeTest {

    @Test
    public void testDecimalDefinition() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE IF NOT EXISTS testDecimalArithmatic" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, " +
                    "col1 DECIMAL, col2 DECIMAL(5), col3 DECIMAL(5,2))";
            createTestTable(getUrl(), ddl);
            
            // Test upsert correct values 
            String query = "UPSERT INTO testDecimalArithmatic(pk, col1, col2, col3) VALUES(?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "valueOne");
            stmt.setBigDecimal(2, new BigDecimal("123456789123456789"));
            stmt.setBigDecimal(3, new BigDecimal("12345"));
            stmt.setBigDecimal(4, new BigDecimal("12.34"));
            stmt.execute();
            conn.commit();
            
            query = "SELECT col1, col2, col3 FROM testDecimalArithmatic WHERE pk = 'valueOne' LIMIT 1";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("123456789123456789"), rs.getBigDecimal(1));
            assertEquals(new BigDecimal("12345"), rs.getBigDecimal(2));
            assertEquals(new BigDecimal("12.34"), rs.getBigDecimal(3));
            assertFalse(rs.next());
            
            query = "UPSERT INTO testDecimalArithmatic(pk, col1, col2, col3) VALUES(?,?,?,?)";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, "valueTwo");
            stmt.setBigDecimal(2, new BigDecimal("1234567890123456789012345678901.12345"));
            stmt.setBigDecimal(3, new BigDecimal("12345.6789"));
            stmt.setBigDecimal(4, new BigDecimal("123.45678"));
            stmt.execute();
            conn.commit();
            
            query = "SELECT col1, col2, col3 FROM testDecimalArithmatic WHERE pk = 'valueTwo' LIMIT 1";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("1234567890123456789012345678901"), rs.getBigDecimal(1));
            assertEquals(new BigDecimal("12345"), rs.getBigDecimal(2));
            assertEquals(new BigDecimal("123.45"), rs.getBigDecimal(3));
            assertFalse(rs.next());
            
            // Test upsert incorrect values and confirm exceptions would be thrown.
            try {
                query = "UPSERT INTO testDecimalArithmatic(pk, col1, col2, col3) VALUES(?,?,?,?)";
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
                assertTrue(e.getMessage(), e.getMessage().contains("ERROR 206 (22003): The value does not fit into the column. value=12345678901234567890123456789012 columnName=COL1"));
            }
            try {
                query = "UPSERT INTO testDecimalArithmatic(pk, col1, col2, col3) VALUES(?,?,?,?)";
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
                assertTrue(e.getMessage(), e.getMessage().contains("ERROR 206 (22003): The value does not fit into the column. value=123456 columnName=COL2"));
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDecimalArithmetic() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE IF NOT EXISTS testDecimalArithmatic" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, " +
                    "col1 DECIMAL, col2 DECIMAL(5), col3 DECIMAL(5,2), col4 DECIMAL(5))";
            createTestTable(getUrl(), ddl);
            
            String query = "UPSERT INTO testDecimalArithmatic(pk, col1, col2, col3) VALUES(?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "testValueOne");
            stmt.setBigDecimal(2, new BigDecimal("1234567890123456789012345678901"));
            stmt.setBigDecimal(3, new BigDecimal("12345"));
            stmt.setBigDecimal(4, new BigDecimal("123.45"));
            stmt.execute();
            conn.commit();
            
            query = "UPSERT INTO testDecimalArithmatic(pk, col1, col2, col3) VALUES(?,?,?,?)";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, "testValueTwo");
            stmt.setBigDecimal(2, new BigDecimal("1234567890123456789012345678901"));
            stmt.setBigDecimal(3, new BigDecimal("12345"));
            stmt.setBigDecimal(4, new BigDecimal("0.01"));
            stmt.execute();
            conn.commit();
            
            query = "UPSERT INTO testDecimalArithmatic(pk, col1, col2, col3, col4) VALUES(?,?,?,?,?)";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, "testValueThree");
            stmt.setBigDecimal(2, new BigDecimal("9999999999999999999999999999999"));
            stmt.setBigDecimal(3, new BigDecimal("123"));
            stmt.setBigDecimal(4, new BigDecimal("123.45"));
            stmt.setBigDecimal(5, new BigDecimal("-123"));
            stmt.execute();
            conn.commit();
            
            // Addition
            // result scale should be: max(ls, rs)
            // result precision should be: max(lp - ls, rp - rs) + 1 + max(ls, rs)
            //
            // good with precision 31 and scale 0.
            query = "SELECT col1 + col2 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            BigDecimal result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1234567890123456789012345691246"), result);
            // good with precision 8 and scale 2.
            query = "SELECT col2 + col3 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("12468.45"), result);
            // Should pass since we roll up and impose the scale and precision at the end of
            // consecutive operations of the same kind.
            query = "SELECT col1 + col2 + col4 FROM testDecimalArithmatic WHERE pk='testValueThree'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("9999999999999999999999999999999"), result);
            // bad due to exceeding scale.
            // TODO:: Throw an exception for a bad value instead of returning null.
            query = "SELECT col1 + col3 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertNull(result);
            // bad due to exceeding precision.
            query = "SELECT col1 + col2 FROM testDecimalArithmatic WHERE pk='testValueThree'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertNull(result);
            
            // Subtraction
            // result scale should be: max(ls, rs)
            // result precision should be: max(lp - ls, rp - rs) + 1 + max(ls, rs)
            //
            // good with precision 31 and scale 0
            query = "SELECT col1 - col2 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1234567890123456789012345666556"), result);
            // good with precision 8 and scale 2.
            query = "SELECT col2 - col3 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("12221.55"), result);
            // Should pass since we roll up and impose the scale and precision at the end of
            // consecutive operations of the same kind.
            query = "SELECT col1 - col4 - col2 FROM testDecimalArithmatic WHERE pk='testValueThree'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("9999999999999999999999999999999"), result);
            // bad due to exceeding scale.
            query = "SELECT col1 - col3 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            query = "SELECT col1 - col2 - col3 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertNull(result);
            // bad due to exceeding precision.
            query = "SELECT col1 - col3 FROM testDecimalArithmatic WHERE pk='testValueThree'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertNull(result);
            
            // Multiplication
            // result scale should be: ls + rs
            // result precision should be: lp + rp
            //
            // col1 * col2 should be good with precision 31 and scale 0
            query = "SELECT col1 * col2 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertNull(result); // Value too big.
            // col2 * col3 should be good with precision 10 and scale 2.
            query = "SELECT col2 * col3 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1523990.25"), result);
            // col1 * col3 would be bad due to exceeding scale.
            query = "SELECT col1 * col3 FROM testDecimalArithmatic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertNull(result); // Value too big.
            
            // Division
            // result scale should be: 31 - lp + ls - rs
            // result precision should be: lp - ls + rp + scale
            //
            // col1 / col2 should be good with precision ? and scale 0.
            query = "SELECT col1 / col2 FROM testDecimalArithmatic WHERE pk='testValueTwo'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("100005499402467135602458135"), result);
            // col2 / col3 should be good with precision ? and scale ?.
            query = "SELECT col2 / col3 FROM testDecimalArithmatic WHERE pk='testValueTwo'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            // The value should be 1234500.0000...00 because we set to scale to be 24. However, in
            // PhoenixResultSet.getBigDecimal, the case to (BigDecimal) actually cause the scale to be eradicated. As
            // a result, the resulting value does not have the right form.
            assertEquals(0, new BigDecimal("1234500").compareTo(result));
            // col1 / col3 would be bad due to exceeding scale.
            query = "SELECT col1 / col3 FROM testDecimalArithmatic WHERE pk='testValueTwo'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertNull(result);
        } finally {
            conn.close();
        }
    }
}
