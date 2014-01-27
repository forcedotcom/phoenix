/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.compile;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.assertDegenerate;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.GroupedAggregateRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.CountAggregator;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.expression.function.TimeUnit;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnAlreadyExistsException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;



/**
 * 
 * Tests for compiling a query
 * The compilation stage finds additional errors that can't be found at parse
 * time so this is a good place for negative tests (since the mini-cluster
 * is not necessary enabling the tests to run faster).
 *
 * @author jtaylor
 * @since 0.1
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
        value="RV_RETURN_VALUE_IGNORED",
        justification="Test code.")
public class QueryCompileTest extends BaseConnectionlessQueryTest {

    @Test
    public void testParameterUnbound() throws Exception {
        try {
            String query = "SELECT a_string, b_string FROM atable WHERE organization_id=? and a_integer = ?";
            Properties props = new Properties(TestUtil.TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Parameter 2 is unbound"));
        }
    }

    @Test
    public void testMultiPKDef() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            String query = "CREATE TABLE foo (pk1 integer not null primary key, pk2 bigint not null primary key)";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 510 (42889): The table already has a primary key. columnName=PK2"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPKDefAndPKConstraint() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            String query = "CREATE TABLE foo (pk integer not null primary key, col1 decimal, col2 decimal constraint my_pk primary key (col1,col2))";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 510 (42889): The table already has a primary key. columnName=PK"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testFamilyNameInPK() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            String query = "CREATE TABLE foo (a.pk integer not null primary key, col1 decimal, col2 decimal)";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) {
            assertEquals(e.getErrorCode(), SQLExceptionCode.PRIMARY_KEY_WITH_FAMILY_NAME.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testVarBinaryInMultipartPK() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        // When the VARBINARY key is the last column, it is allowed.
        String query = "CREATE TABLE foo (a_string varchar not null, b_string varchar not null, a_binary varbinary not null, " +
                "col1 decimal, col2 decimal CONSTRAINT pk PRIMARY KEY (a_string, b_string, a_binary))";
        PreparedStatement statement = conn.prepareStatement(query);
        statement.execute();
        try {
            // VARBINARY key is not allowed in the middle of the key.
            query = "CREATE TABLE foo (a_binary varbinary not null, a_string varchar not null, col1 decimal, col2 decimal CONSTRAINT pk PRIMARY KEY (a_binary, a_string))";
            statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1005 (42J03): The VARBINARY type can only be used as the last part of a multi-part row key. columnName=FOO.A_BINARY"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testNoPK() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            String query = "CREATE TABLE foo (pk integer not null, col1 decimal, col2 decimal)";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 509 (42888): The table does not have a primary key. tableName=FOO"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUnknownFamilyNameInTableOption() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            String query = "CREATE TABLE foo (pk integer not null primary key, a.col1 decimal, b.col2 decimal) c.my_property='foo'";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Properties may not be defined for an unused family name"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testInvalidGroupedAggregation() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "SELECT count(1),a_integer FROM atable WHERE organization_id=? GROUP BY a_string";
            Properties props = new Properties(TestUtil.TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1018 (42Y27): Aggregate may not contain columns not in GROUP BY. A_INTEGER"));
        }
    }

    @Test
    public void testInvalidGroupExpressionAggregation() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "SELECT sum(a_integer) + a_integer FROM atable WHERE organization_id=? GROUP BY a_string";
            Properties props = new Properties(TestUtil.TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1018 (42Y27): Aggregate may not contain columns not in GROUP BY. A_INTEGER"));
        }
    }

    @Test
    public void testAggInWhereClause() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "SELECT a_integer FROM atable WHERE organization_id=? AND count(1) > 2";
            Properties props = new Properties(TestUtil.TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1017 (42Y26): Aggregate may not be used in WHERE."));
        }
    }

    @Test
    public void testHavingAggregateQuery() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "SELECT a_integer FROM atable WHERE organization_id=? HAVING count(1) > 2";
            Properties props = new Properties(TestUtil.TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1018 (42Y27): Aggregate may not contain columns not in GROUP BY. A_INTEGER"));
        }
    }

    @Test
    public void testNonAggInHavingClause() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "SELECT a_integer FROM atable WHERE organization_id=? HAVING a_integer = 5";
            Properties props = new Properties(TestUtil.TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1019 (42Y26): Only aggregate maybe used in the HAVING clause."));
        }
    }

    @Test
    public void testTypeMismatchInCase() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "SELECT a_integer FROM atable WHERE organization_id=? HAVING CASE WHEN a_integer <= 2 THEN 'foo' WHEN a_integer = 3 THEN 2 WHEN a_integer <= 5 THEN 5 ELSE 5 END  = 5";
            Properties props = new Properties(TestUtil.TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Case expressions must have common type"));
        }
    }

    @Test
    public void testNonBooleanWhereExpression() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "SELECT a_integer FROM atable WHERE organization_id=? and CASE WHEN a_integer <= 2 THEN 'foo' WHEN a_integer = 3 THEN 'bar' WHEN a_integer <= 5 THEN 'bas' ELSE 'blah' END";
            Properties props = new Properties(TestUtil.TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("ERROR 203 (22005): Type mismatch. BOOLEAN and VARCHAR for CASE WHEN A_INTEGER <= 2 THEN 'foo'WHEN A_INTEGER = 3 THEN 'bar'WHEN A_INTEGER <= 5 THEN 'bas' ELSE 'blah' END"));
        }
    }

    @Test
    public void testNoSCNInConnectionProps() throws Exception {
        Properties props = new Properties();
        DriverManager.getConnection(getUrl(), props);
    }
    

    @Test
    public void testPercentileWrongQueryWithMixOfAggrAndNonAggrExps() throws Exception {
        String query = "select a_integer, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY a_integer ASC) from ATABLE";
        try {
            compileQuery(query, Collections.emptyList());
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
            compileQuery(query, Collections.emptyList());
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
            compileQuery(query, Collections.emptyList());
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
            compileQuery(query, Collections.emptyList());
            fail();
        } catch (Exception e) {
            assertEquals(
                    "ERROR 213 (22003): Value outside range. expected: [0 , 1] but was: -1 at PERCENTILE_CONT argument 3",
                    e.getMessage());
        }
    }

    private Scan compileQuery(String query, List<Object> binds) throws SQLException {
        Properties props = new Properties(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PhoenixPreparedStatement statement = conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class);
            for (Object bind : binds) {
                statement.setObject(1, bind);
            }
            QueryPlan plan = statement.compileQuery(query);
            return plan.getContext().getScan();
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testKeyOrderedGroupByOptimization() throws Exception {
        // Select columns in PK
        String[] queries = new String[] {
            "SELECT count(1) FROM atable GROUP BY organization_id,entity_id",
            "SELECT count(1) FROM atable GROUP BY organization_id,substr(entity_id,1,3),entity_id",
            "SELECT count(1) FROM atable GROUP BY entity_id,organization_id",
            "SELECT count(1) FROM atable GROUP BY substr(entity_id,1,3),organization_id",
            "SELECT count(1) FROM ptsdb GROUP BY host,inst,round(date,'HOUR')",
            "SELECT count(1) FROM atable GROUP BY organization_id",
        };
        List<Object> binds = Collections.emptyList();
        for (String query : queries) {
            Scan scan = compileQuery(query, binds);
            assertTrue(query, scan.getAttribute(GroupedAggregateRegionObserver.KEY_ORDERED_GROUP_BY_EXPRESSIONS) != null);
            assertTrue(query, scan.getAttribute(GroupedAggregateRegionObserver.UNORDERED_GROUP_BY_EXPRESSIONS) == null);
        }
    }

    @Test
    public void testNullInScanKey() throws Exception {
        // Select columns in PK
        String query = "select val from ptsdb where inst is null and host='a'";
        List<Object> binds = Collections.emptyList();
        Scan scan = compileQuery(query, binds);
        // Projects column family with not null column
        assertNull(scan.getFilter());
        assertEquals(1,scan.getFamilyMap().keySet().size());
        assertArrayEquals(Bytes.toBytes(SchemaUtil.normalizeIdentifier(QueryConstants.DEFAULT_COLUMN_FAMILY)), scan.getFamilyMap().keySet().iterator().next());
    }

    @Test
    public void testOnlyNullInScanKey() throws Exception {
        // Select columns in PK
        String query = "select val from ptsdb where inst is null";
        List<Object> binds = Collections.emptyList();
        Scan scan = compileQuery(query, binds);
        // Projects column family with not null column
        assertEquals(1,scan.getFamilyMap().keySet().size());
        assertArrayEquals(Bytes.toBytes(SchemaUtil.normalizeIdentifier(QueryConstants.DEFAULT_COLUMN_FAMILY)), scan.getFamilyMap().keySet().iterator().next());
    }

    @Test
    public void testIsNullOnNotNullable() throws Exception {
        // Select columns in PK
        String query = "select a_string from atable where entity_id is null";
        List<Object> binds = Collections.emptyList();
        Scan scan = compileQuery(query, binds);
        assertDegenerate(scan);
    }

    @Test
    public void testIsNotNullOnNotNullable() throws Exception {
        // Select columns in PK
        String query = "select a_string from atable where entity_id is not null";
        List<Object> binds = Collections.emptyList();
        Scan scan = compileQuery(query, binds);
        assertNull(scan.getFilter());
        assertTrue(scan.getStartRow().length == 0);
        assertTrue(scan.getStopRow().length == 0);
    }

    @Test
    public void testUpsertTypeMismatch() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "upsert into ATABLE VALUES (?, ?, ?)";
            Properties props = new Properties(TestUtil.TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.setString(2, "00D300000000XHP");
                statement.setInt(3, 1);
                statement.executeUpdate();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) { // TODO: use error codes
            assertTrue(e.getMessage().contains("Type mismatch"));
        }
    }

    @Test
    public void testUpsertMultiByteIntoChar() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "upsert into ATABLE VALUES (?, ?, ?)";
            Properties props = new Properties(TestUtil.TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.setString(2, "繰り返し曜日マスク");
                statement.setInt(3, 1);
                statement.executeUpdate();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 201 (22000): Illegal data."));
            assertTrue(e.getCause().getMessage().contains("CHAR types may only contain single byte characters"));
        }
    }

    @Test
    public void testSelectStarOnGroupBy() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "select * from ATABLE group by a_string";
            Properties props = new Properties(TestUtil.TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1018 (42Y27): Aggregate may not contain columns not in GROUP BY."));
        }
    }

    @Test
    public void testOrderByAggSelectNonAgg() throws Exception {
        try {
            // Order by in select with no limit or group by
            String query = "select a_string from ATABLE order by max(b_string)";
            Properties props = new Properties(TestUtil.TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1018 (42Y27): Aggregate may not contain columns not in GROUP BY. A_STRING"));
        }
    }

    @Test
    public void testOrderByAggAndNonAgg() throws Exception {
        try {
            // Order by in select with no limit or group by
            String query = "select max(a_string) from ATABLE order by max(b_string),a_string";
            Properties props = new Properties(TestUtil.TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1018 (42Y27): Aggregate may not contain columns not in GROUP BY. A_STRING"));
        }
    }

    @Test
    public void testOrderByNonAggSelectAgg() throws Exception {
        try {
            // Order by in select with no limit or group by
            String query = "select max(a_string) from ATABLE order by b_string LIMIT 5";
            Properties props = new Properties(TestUtil.TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1018 (42Y27): Aggregate may not contain columns not in GROUP BY. B_STRING"));
        }
    }

    @Test
    public void testNotKeyOrderedGroupByOptimization() throws Exception {
        // Select columns in PK
        String[] queries = new String[] {
            "SELECT count(1) FROM atable GROUP BY entity_id",
            "SELECT count(1) FROM atable GROUP BY substr(organization_id,2,3)",
            "SELECT count(1) FROM atable GROUP BY substr(entity_id,1,3)",
            "SELECT count(1) FROM atable GROUP BY to_date(organization_id)",
            "SELECT count(1) FROM atable GROUP BY regexp_substr(organization_id, '.*foo.*'),entity_id",
            "SELECT count(1) FROM atable GROUP BY substr(organization_id,1),entity_id",
        };
        List<Object> binds = Collections.emptyList();
        for (String query : queries) {
            Scan scan = compileQuery(query, binds);
            assertTrue(query, scan.getAttribute(GroupedAggregateRegionObserver.KEY_ORDERED_GROUP_BY_EXPRESSIONS) == null);
            assertTrue(query, scan.getAttribute(GroupedAggregateRegionObserver.UNORDERED_GROUP_BY_EXPRESSIONS) != null);
        }
    }
    
    @Test
    public void testFunkyColumnNames() throws Exception {
        // Select columns in PK
        String[] queries = new String[] {
            "SELECT \"foo!\",\"foo.bar-bas\",\"#@$\",\"_blah^\" FROM FUNKY_NAMES",
            "SELECT count(\"foo!\"),\"_blah^\" FROM FUNKY_NAMES WHERE \"foo.bar-bas\"='x' GROUP BY \"#@$\",\"_blah^\"",
        };
        List<Object> binds = Collections.emptyList();
        for (String query : queries) {
            compileQuery(query, binds);
        }
    }
    
    @Test
    public void testCountAggregatorFirst() throws Exception {
        String[] queries = new String[] {
            "SELECT sum(2.5),organization_id FROM atable GROUP BY organization_id,entity_id",
            "SELECT avg(a_integer) FROM atable GROUP BY organization_id,substr(entity_id,1,3),entity_id",
            "SELECT count(a_string) FROM atable GROUP BY substr(organization_id,1),entity_id",
            "SELECT min('foo') FROM atable GROUP BY entity_id,organization_id",
            "SELECT min('foo'),sum(a_integer),avg(2.5),4.5,max(b_string) FROM atable GROUP BY substr(organization_id,1),entity_id",
            "SELECT sum(2.5) FROM atable",
            "SELECT avg(a_integer) FROM atable",
            "SELECT count(a_string) FROM atable",
            "SELECT min('foo') FROM atable LIMIT 5",
            "SELECT min('foo'),sum(a_integer),avg(2.5),4.5,max(b_string) FROM atable",
        };
        List<Object> binds = Collections.emptyList();
        String query = null;
        try {
            for (int i = 0; i < queries.length; i++) {
                query = queries[i];
                Scan scan = compileQuery(query, binds);
                ServerAggregators aggregators = ServerAggregators.deserialize(scan.getAttribute(GroupedAggregateRegionObserver.AGGREGATORS), null);
                Aggregator aggregator = aggregators.getAggregators()[0];
                assertTrue(aggregator instanceof CountAggregator);
            }
        } catch (Exception e) {
            throw new Exception(query, e);
        }
    }

    @Test
    public void testInvalidArithmetic() throws Exception {
        String[] queries = new String[] { 
                "SELECT entity_id,organization_id FROM atable where A_STRING - 5.5 < 0",
                "SELECT entity_id,organization_id FROM atable where A_DATE - 'transaction' < 0",
                "SELECT entity_id,organization_id FROM atable where A_DATE * 45 < 0",
                "SELECT entity_id,organization_id FROM atable where A_DATE / 45 < 0",
                "SELECT entity_id,organization_id FROM atable where 45 - A_DATE < 0",
                "SELECT entity_id,organization_id FROM atable where A_DATE - to_date('2000-01-01 12:00:00') < to_date('2000-02-01 12:00:00')", // RHS must be number
                "SELECT entity_id,organization_id FROM atable where A_DATE - A_DATE + 1 < A_DATE", // RHS must be number
                "SELECT entity_id,organization_id FROM atable where A_DATE + 2 < 0", // RHS must be date
                "SELECT entity_id,organization_id FROM atable where 45.5 - A_DATE < 0",
                "SELECT entity_id,organization_id FROM atable where 1 + A_DATE + A_DATE < A_DATE",
                "SELECT entity_id,organization_id FROM atable where A_STRING - 45 < 0",
                "SELECT entity_id,organization_id FROM atable where A_STRING / 45 < 0",
                "SELECT entity_id,organization_id FROM atable where A_STRING * 45 < 0",
                "SELECT entity_id,organization_id FROM atable where A_STRING + 45 < 0",
                "SELECT entity_id,organization_id FROM atable where A_STRING - 45 < 0",
                "SELECT entity_id,organization_id FROM atable where A_STRING - 'transaction' < 0", };

        Properties props = new Properties(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        for (String query : queries) {
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.executeQuery();
                fail(query);
            } catch (SQLException e) {
                if (e.getMessage().contains("ERROR 203 (22005): Type mismatch.")) {
                    continue;
                }
                throw new IllegalStateException("Didn't find type mismatch: " + query, e);
            }
        }
    }
    
    
    @Test
    public void testAmbiguousColumn() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT * from multi_cf G where RESPONSE_TIME = 2222";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (AmbiguousColumnException e) { // expected
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTableAliasMatchesCFName() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from multi_cf G where G.RESPONSE_TIME-1 = F.RESPONSE_TIME";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (AmbiguousColumnException e) { // expected
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCoelesceFunctionTypeMismatch() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT coalesce(x_integer,'foo') from atable";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (SQLException e) { // expected
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 507 (42846): Cannot convert type. COALESCE expected INTEGER, but got VARCHAR"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testOrderByNotInSelectDistinct() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT distinct a_string,b_string from atable order by x_integer";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (SQLException e) { // expected
            assertEquals(SQLExceptionCode.ORDER_BY_NOT_IN_SELECT_DISTINCT.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectDistinctAndAll() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT all distinct a_string,b_string from atable order by x_integer";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (SQLException e) { // expected
            assertEquals(SQLExceptionCode.PARSER_ERROR.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testOrderByNotInSelectDistinctAgg() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT distinct count(1) from atable order by x_integer";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (SQLException e) { // expected
            assertEquals(SQLExceptionCode.ORDER_BY_NOT_IN_SELECT_DISTINCT.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectDistinctWithAggregation() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT distinct a_string,count(*) from atable";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (SQLException e) { // expected
            assertEquals(SQLExceptionCode.AGGREGATE_WITH_NOT_GROUP_BY_COLUMN.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test 
    public void testRegexpSubstrSetScanKeys() throws Exception {
        // First test scan keys are set when the offset is 0 or 1. 
        String query = "SELECT host FROM ptsdb WHERE regexp_substr(inst, '[a-zA-Z]+') = 'abc'";
        List<Object> binds = Collections.emptyList();
        Scan scan = compileQuery(query, binds);
        assertArrayEquals(ByteUtil.concat(Bytes.toBytes("abc")), scan.getStartRow());
        assertArrayEquals(ByteUtil.concat(ByteUtil.nextKey(Bytes.toBytes("abc")),QueryConstants.SEPARATOR_BYTE_ARRAY), scan.getStopRow());
        assertTrue(scan.getFilter() != null);

        query = "SELECT host FROM ptsdb WHERE regexp_substr(inst, '[a-zA-Z]+', 0) = 'abc'";
        binds = Collections.emptyList();
        scan = compileQuery(query, binds);
        assertArrayEquals(ByteUtil.concat(Bytes.toBytes("abc")), scan.getStartRow());
        assertArrayEquals(ByteUtil.concat(ByteUtil.nextKey(Bytes.toBytes("abc")),QueryConstants.SEPARATOR_BYTE_ARRAY), scan.getStopRow());
        assertTrue(scan.getFilter() != null);

        // Test scan keys are not set when the offset is not 0 or 1.
        query = "SELECT host FROM ptsdb WHERE regexp_substr(inst, '[a-zA-Z]+', 3) = 'abc'";
        binds = Collections.emptyList();
        scan = compileQuery(query, binds);
        assertTrue(scan.getStartRow().length == 0);
        assertTrue(scan.getStopRow().length == 0);
        assertTrue(scan.getFilter() != null);
    }
    
    @Test
    public void testStringConcatExpression() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT entity_id,a_string FROM atable where 2 || a_integer || ? like '2%'";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        byte []x=new byte[]{127,127,0,0};//Binary data
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setBytes(1, x);
            statement.executeQuery();
            fail();
        } catch (SQLException e) { // expected
            assertTrue(e.getMessage().contains("Concatenation does not support"));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDivideByBigDecimalZero() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT a_integer/x_integer/0.0 FROM atable";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Connection conn = DriverManager.getConnection(url);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (SQLException e) { // expected
            assertTrue(e.getMessage().contains("Divide by zero"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDivideByIntegerZero() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT a_integer/0 FROM atable";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Connection conn = DriverManager.getConnection(url);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (SQLException e) { // expected
            assertTrue(e.getMessage().contains("Divide by zero"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCreateNullableInPKMiddle() throws Exception {
        long ts = nextTimestamp();
        String query = "CREATE TABLE foo(i integer not null, j integer null, k integer not null CONSTRAINT pk PRIMARY KEY(i,j,k))";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Connection conn = DriverManager.getConnection(url);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) { // expected
            assertTrue(e.getMessage().contains("PK columns may not be both fixed width and nullable"));
        }
    }

    @Test
    public void testSetSaltBucketOnAlterTable() throws Exception {
        long ts = nextTimestamp();
        String query = "ALTER TABLE atable ADD xyz INTEGER SALT_BUCKETS=4";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Connection conn = DriverManager.getConnection(url);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) { // expected
            assertTrue(e.getErrorCode() == SQLExceptionCode.SALT_ONLY_ON_CREATE_TABLE.getErrorCode());
        }
    }

    @Test
    public void testSubstrSetScanKey() throws Exception {
        String query = "SELECT inst FROM ptsdb WHERE substr(inst, 0, 3) = 'abc'";
        List<Object> binds = Collections.emptyList();
        Scan scan = compileQuery(query, binds);
        assertArrayEquals(ByteUtil.concat(Bytes.toBytes("abc")), scan.getStartRow());
        assertArrayEquals(ByteUtil.concat(Bytes.toBytes("abd"),QueryConstants.SEPARATOR_BYTE_ARRAY), scan.getStopRow());
        assertTrue(scan.getFilter() == null); // Extracted.
    }

    @Test
    public void testRTrimSetScanKey() throws Exception {
        String query = "SELECT inst FROM ptsdb WHERE rtrim(inst) = 'abc'";
        List<Object> binds = Collections.emptyList();
        Scan scan = compileQuery(query, binds);
        assertArrayEquals(ByteUtil.concat(Bytes.toBytes("abc")), scan.getStartRow());
        assertArrayEquals(ByteUtil.concat(ByteUtil.nextKey(Bytes.toBytes("abc ")),QueryConstants.SEPARATOR_BYTE_ARRAY), scan.getStopRow());
        assertNotNull(scan.getFilter());
    }
    
    @Test
    public void testCastingIntegerToDecimalInSelect() throws Exception {
        String query = "SELECT CAST a_integer AS DECIMAL/2 FROM aTable WHERE 5=a_integer";
        List<Object> binds = Collections.emptyList();
        compileQuery(query, binds);
    }
    
    @Test
    public void testCastingStringToDecimalInSelect() throws Exception {
        String query = "SELECT CAST b_string AS DECIMAL/2 FROM aTable WHERE 5=a_integer";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since casting a string to decimal isn't supported");
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == SQLExceptionCode.TYPE_MISMATCH.getErrorCode());
        }
    }
    
    @Test
    public void testCastingStringToDecimalInWhere() throws Exception {
        String query = "SELECT a_integer FROM aTable WHERE 2.5=CAST b_string AS DECIMAL/2 ";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since casting a string to decimal isn't supported");
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == SQLExceptionCode.TYPE_MISMATCH.getErrorCode());
        }  
    }
    
    @Test
    public void testUsingNonComparableDataTypesInRowValueConstructorFails() throws Exception {
        String query = "SELECT a_integer, x_integer FROM aTable WHERE (a_integer, x_integer) > (2, 'abc')";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since casting a integer to string isn't supported");
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == SQLExceptionCode.TYPE_MISMATCH.getErrorCode());
        }
    }
    
    @Test
    public void testUsingNonComparableDataTypesOfColumnRefOnLHSAndRowValueConstructorFails() throws Exception {
        String query = "SELECT a_integer, x_integer FROM aTable WHERE a_integer > ('abc', 2)";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since casting a integer to string isn't supported");
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == SQLExceptionCode.TYPE_MISMATCH.getErrorCode());
        }
    }
    
    @Test
    public void testUsingNonComparableDataTypesOfLiteralOnLHSAndRowValueConstructorFails() throws Exception {
        String query = "SELECT a_integer, x_integer FROM aTable WHERE 'abc' > (a_integer, x_integer)";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since casting a integer to string isn't supported");
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == SQLExceptionCode.TYPE_MISMATCH.getErrorCode());
        }
    }
    
    @Test
    public void testUsingNonComparableDataTypesOfColumnRefOnRHSAndRowValueConstructorFails() throws Exception {
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ('abc', 2) < a_integer ";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since casting a integer to string isn't supported");
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == SQLExceptionCode.TYPE_MISMATCH.getErrorCode());
        }
    }
    
    @Test
    public void testUsingNonComparableDataTypesOfLiteralOnRHSAndRowValueConstructorFails() throws Exception {
        String query = "SELECT a_integer, x_integer FROM aTable WHERE (a_integer, x_integer) < 'abc'";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since casting a integer to string isn't supported");
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == SQLExceptionCode.TYPE_MISMATCH.getErrorCode());
        }
    }
    
    @Test
    public void testNonConstantInList() throws Exception {
        String query = "SELECT a_integer, x_integer FROM aTable WHERE a_integer IN (x_integer)";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since non constants in IN is not valid");
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == SQLExceptionCode.VALUE_IN_LIST_NOT_CONSTANT.getErrorCode());
        }
    }
    
    @Test
    public void testKeyValueColumnInPKConstraint() throws Exception {
        String ddl = "CREATE TABLE t (a.k VARCHAR, b.v VARCHAR CONSTRAINT pk PRIMARY KEY(k))";
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == SQLExceptionCode.PRIMARY_KEY_WITH_FAMILY_NAME.getErrorCode());
        }
    }
    
    @Test
    public void testUnknownColumnInPKConstraint() throws Exception {
        String ddl = "CREATE TABLE t (k1 VARCHAR, b.v VARCHAR CONSTRAINT pk PRIMARY KEY(k1, k2))";
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (ColumnNotFoundException e) {
            assertEquals("K2",e.getColumnName());
        }
    }
    
    
    @Test
    public void testDuplicatePKColumn() throws Exception {
        String ddl = "CREATE TABLE t (k1 VARCHAR, k1 VARCHAR CONSTRAINT pk PRIMARY KEY(k1))";
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (ColumnAlreadyExistsException e) {
            assertEquals("K1",e.getColumnName());
        }
    }
    
    
    @Test
    public void testDuplicateKVColumn() throws Exception {
        String ddl = "CREATE TABLE t (k1 VARCHAR, v1 VARCHAR, v2 VARCHAR, v1 INTEGER CONSTRAINT pk PRIMARY KEY(k1))";
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (ColumnAlreadyExistsException e) {
            assertEquals("V1",e.getColumnName());
        }
    }
    
    @Test
    public void testDeleteFromImmutableWithKV() throws Exception {
        String ddl = "CREATE TABLE t (k1 VARCHAR, v1 VARCHAR, v2 VARCHAR CONSTRAINT pk PRIMARY KEY(k1)) immutable_rows=true";
        String indexDDL = "CREATE INDEX i ON t (v1)";
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute(indexDDL);
            conn.createStatement().execute("DELETE FROM t");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.NO_DELETE_IF_IMMUTABLE_INDEX.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testInvalidNegativeArrayIndex() throws Exception {
    	String query = "SELECT a_double_array[-20] FROM table_with_array";
    	Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute(query);
            fail();
        } catch (Exception e) {
        	
        }
    }
    @Test
    public void testWrongDataTypeInRoundFunction() throws Exception {
        String query = "SELECT ROUND(a_string, 'day', 1) FROM aTable";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since VARCHAR is not a valid data type for ROUND");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testNonArrayColumnWithIndex() throws Exception {
    	String query = "SELECT a_float[1] FROM table_with_array";
    	Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute(query);
            fail();
        } catch (Exception e) {
        }
    }

    public void testWrongTimeUnitInRoundDateFunction() throws Exception {
        String query = "SELECT ROUND(a_date, 'dayss', 1) FROM aTable";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since dayss is not a valid time unit type");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(TimeUnit.VALID_VALUES));
        }
    }
    
    @Test
    public void testWrongMultiplierInRoundDateFunction() throws Exception {
        String query = "SELECT ROUND(a_date, 'days', 1.23) FROM aTable";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since multiplier can be an INTEGER only");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testTypeMismatchForArrayElem() throws Exception {
        String query = "SELECT (a_string,a_date)[1] FROM aTable";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since a row value constructor is not an array");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testTypeMismatch2ForArrayElem() throws Exception {
        String query = "SELECT ROUND(a_date, 'days', 1.23)[1] FROM aTable";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since ROUND does not return an array");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testInvalidArrayTypeAsPK () throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            String query = "CREATE TABLE foo (col1 INTEGER[10] NOT NULL PRIMARY KEY)";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) {
                assertEquals(SQLExceptionCode.ARRAY_NOT_ALLOWED_IN_PRIMARY_KEY.getErrorCode(), e.getErrorCode());
        } finally {
                conn.close();
        }

        conn = DriverManager.getConnection(getUrl());
        try {
            String query = "CREATE TABLE foo (col1 VARCHAR, col2 INTEGER ARRAY[10] CONSTRAINT pk PRIMARY KEY (col1, col2))";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.ARRAY_NOT_ALLOWED_IN_PRIMARY_KEY.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testInvalidArraySize() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            String query = "CREATE TABLE foo (col1 INTEGER[-1] NOT NULL PRIMARY KEY)";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) {
                assertEquals(SQLExceptionCode.MISMATCHED_TOKEN.getErrorCode(), e.getErrorCode());
        } finally {
                conn.close();
        }
    }

    @Test
    public void testInvalidArrayInQuery () throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k VARCHAR PRIMARY KEY, a INTEGER[10], B INTEGER[10])");
        try {
            conn.createStatement().execute("SELECT * FROM t ORDER BY a");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.ORDER_BY_ARRAY_NOT_SUPPORTED.getErrorCode(), e.getErrorCode());
        }
        try {
            conn.createStatement().execute("SELECT * FROM t WHERE a < b");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.NON_EQUALITY_ARRAY_COMPARISON.getErrorCode(), e.getErrorCode());
        }
        conn.close();
    }

    @Test
    public void testInvalidNextValueFor() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE SEQUENCE alpha.zeta");
        String[] queries = {
                "SELECT * FROM aTable WHERE a_integer < next value for alpha.zeta",
                "SELECT * FROM aTable GROUP BY a_string,next value for alpha.zeta",
                "SELECT * FROM aTable GROUP BY 1 + next value for alpha.zeta",
                "SELECT * FROM aTable GROUP BY a_integer HAVING a_integer < next value for alpha.zeta",
                "SELECT * FROM aTable WHERE a_integer < 3 GROUP BY a_integer HAVING a_integer < next value for alpha.zeta",
                "SELECT * FROM aTable ORDER BY next value for alpha.zeta",
                "SELECT max(next value for alpha.zeta) FROM aTable",
        };
        for (String query : queries) {
            List<Object> binds = Collections.emptyList();
            try {
                compileQuery(query, binds);
                fail("Compilation should have failed since this is an invalid usage of NEXT VALUE FOR: " + query);
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.INVALID_USE_OF_NEXT_VALUE_FOR.getErrorCode(), e.getErrorCode());
            }
        }
    }
    
}
