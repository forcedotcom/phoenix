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
    public void testDecimalArithmetic() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE IF NOT EXISTS testDecimalArithmatic" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, " +
                    "col1 DECIMAL, col2 DECIMAL(5), col3 DECIMAL(5,2))";
            createTestTable(getUrl(), ddl);
            
            // Test upsert correct values.
            String query = "UPSERT INTO testDecimalArithmatic(pk, col1, col2, col3) VALUES(?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "insertGoodValues");
            stmt.setBigDecimal(2, new BigDecimal("123456789.123456789"));
            stmt.setBigDecimal(3, new BigDecimal("123.45"));
            stmt.setBigDecimal(4, new BigDecimal("1234.5"));
            stmt.execute();
            conn.commit();
            
            query = "SELECT col1, col2, col3 FROM testDecimalArithmatic WHERE pk = 'insertGoodValues' LIMIT 1";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("123456789.123456789"), rs.getBigDecimal(1));
            assertEquals(new BigDecimal("123.45"), rs.getBigDecimal(2));
            assertEquals(new BigDecimal("1234.5"), rs.getBigDecimal(3));
            
            // Test upsert incorrect values and confirm exceptions would be thrown.
            try {
                query = "UPSERT INTO testDecimalArithmatic(pk, col1, col2, col3) VALUES(?,?,?,?)";
                stmt = conn.prepareStatement(query);
                stmt.setString(1, "badValues");
                // one more than max_precision
                stmt.setBigDecimal(2, new BigDecimal("12345678901234567890123456789012"));
                stmt.setBigDecimal(3, new BigDecimal("123.45")); 
                stmt.setBigDecimal(4, new BigDecimal("1234.5"));
                stmt.execute();
                conn.commit();
                fail("Should have caught bad values.");
            } catch (Exception e) {
                assertTrue(e.getMessage(), e.getMessage().contains("ERROR 206 (22003): The value does not fit into the column schema. 12345678901234567890123456789012 columnName=COL1"));
            }
            try {
                query = "UPSERT INTO testDecimalArithmatic(pk, col1, col2, col3) VALUES(?,?,?,?)";
                stmt = conn.prepareStatement(query);
                stmt.setString(1, "badValues");
                stmt.setBigDecimal(2, new BigDecimal("123456"));
                // Exceeds specified precision by 1
                stmt.setBigDecimal(3, new BigDecimal("123456"));
                stmt.setBigDecimal(4, new BigDecimal("1234.5"));
                stmt.execute();
                conn.commit();
                fail("Should have caught bad values.");
            } catch (Exception e) {
                assertTrue(e.getMessage(), e.getMessage().contains("ERROR 206 (22003): The value does not fit into the column schema. 123456 columnName=COL2"));
            }
            try {
                query = "UPSERT INTO testDecimalArithmatic(pk, col1, col2, col3) VALUES(?,?,?,?)";
                stmt = conn.prepareStatement(query);
                stmt.setString(1, "badValues");
                stmt.setBigDecimal(2, new BigDecimal("123456"));
                stmt.setBigDecimal(3, new BigDecimal("12345"));
                // Exceeds specified scale by 1
                stmt.setBigDecimal(4, new BigDecimal("12.345"));
                stmt.execute();
                conn.commit();
                fail("Should have caught bad values.");
            } catch (Exception e) {
                assertTrue(e.getMessage(), e.getMessage().contains("ERROR 206 (22003): The value does not fit into the column schema. 12.345 columnName=COL3"));
            }
        } finally {
            conn.close();
        }
    }
}
