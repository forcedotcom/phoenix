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

import static com.salesforce.phoenix.util.TestUtil.JOIN_CUSTOMER_TABLE;
import static com.salesforce.phoenix.util.TestUtil.JOIN_ITEM_TABLE;
import static com.salesforce.phoenix.util.TestUtil.JOIN_ORDER_TABLE;
import static com.salesforce.phoenix.util.TestUtil.JOIN_SUPPLIER_TABLE;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import org.junit.Test;

import com.salesforce.phoenix.query.BaseConnectionlessQueryTest;
import com.salesforce.phoenix.util.QueryUtil;

/**
 * Test compilation of queries containing joins.
 */
public class JoinQueryCompileTest extends BaseConnectionlessQueryTest {
    
    @Test
    public void testExplainPlan() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String query = "EXPLAIN SELECT s.supplier_id, order_id, c.name, i.name, quantity, o.date FROM " + JOIN_ORDER_TABLE + " o LEFT JOIN " 
    	+ JOIN_CUSTOMER_TABLE + " c ON o.customer_id = c.customer_id AND c.name LIKE 'C%' LEFT JOIN " 
    	+ JOIN_ITEM_TABLE + " i ON o.item_id = i.item_id RIGHT JOIN " 
    	+ JOIN_SUPPLIER_TABLE + " s ON s.supplier_id = i.supplier_id WHERE i.name LIKE 'T%'";
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertEquals(
        		"CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_SUPPLIER_TABLE\n" +
        		"    SERVER FILTER BY FIRST KEY ONLY\n" +
        		"    PARALLEL EQUI-JOIN 1 HASH TABLES:\n" +
        		"    BUILD HASH TABLE 0\n" +
        		"        CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_ORDER_TABLE\n" +
        		"            PARALLEL EQUI-JOIN 2 HASH TABLES:\n" +
        		"            BUILD HASH TABLE 0\n" +
        		"                CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_CUSTOMER_TABLE\n" +
        		"                    SERVER FILTER BY NAME LIKE 'C%'\n" +
        		"            BUILD HASH TABLE 1\n" +
        		"                CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_ITEM_TABLE\n" +
        		"    AFTER-JOIN SERVER FILTER BY I.NAME(PROJECTED[7]) LIKE 'T%'", QueryUtil.getExplainPlan(rs));
    }

}
