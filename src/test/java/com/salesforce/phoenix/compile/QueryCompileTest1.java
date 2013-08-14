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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.*;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.client.Scan;
import org.junit.Test;

import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.SQLParser;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.test.BaseConnectionlessQueryTest;
import com.salesforce.phoenix.util.TestUtil;

public class QueryCompileTest1 extends BaseConnectionlessQueryTest {

    @Test
    public void testPercentileWrongQueryWithMixOfAggrAndNonAggrExps() throws Exception {
        String query = "select a_integer, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY a_integer ASC) from ATABLE";
        try {
            compileQuery(query, null, new Scan());
            fail();
        } catch (SQLException e) {
            assertEquals("ERROR 1018 (42Y27): Aggregate may not contain columns not in GROUP BY. A_INTEGER",
                    e.getMessage());
        }
    }

    @Test
    public void testPercentileWrongQuery1() throws Exception {
        String query = "select PERCENTILE_CONT('*') WITHIN GROUP (ORDER BY a_integer ASC) from ATABLE";
        try {
            compileQuery(query, null, new Scan());
            fail();
        } catch (SQLException e) {
            assertEquals(
                    "ERROR 203 (22005): Type mismatch. expected: [DECIMAL] but was: VARCHAR at PERCENTILE_CONT argument 3",
                    e.getMessage());
        }
    }

    @Test
    public void testPercentileWrongQuery2() throws Exception {
        String query = "select PERCENTILE_CONT(1.1) WITHIN GROUP (ORDER BY a_integer ASC) from ATABLE";
        try {
            compileQuery(query, null, new Scan());
            fail();
        } catch (SQLException e) {
            assertEquals(
                    "ERROR 213 (22003): Value outside range. expected: [0 , 1] but was: 1.1 at PERCENTILE_CONT argument 3",
                    e.getMessage());
        }
    }

    @Test
    public void testPercentileWrongQuery3() throws Exception {
        String query = "select PERCENTILE_CONT(-1) WITHIN GROUP (ORDER BY a_integer ASC) from ATABLE";
        try {
            compileQuery(query, null, new Scan());
            fail();
        } catch (Exception e) {
            assertEquals(
                    "ERROR 213 (22003): Value outside range. expected: [0 , 1] but was: -1 at PERCENTILE_CONT argument 3",
                    e.getMessage());
        }
    }

    private QueryPlan compileQuery(String query, List<Object> binds, Scan scan) throws SQLException {
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Properties props = new Properties(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
            QueryCompiler compiler = new QueryCompiler(pconn, 0, scan);
            QueryPlan plan = compiler.compile(statement, binds);
            return plan;
        } finally {
            conn.close();
        }
    }
}
