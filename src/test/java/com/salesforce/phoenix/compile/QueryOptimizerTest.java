package com.salesforce.phoenix.compile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;

import org.junit.Test;

import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.jdbc.PhoenixStatement;
import com.salesforce.phoenix.query.BaseConnectionlessQueryTest;

public class QueryOptimizerTest extends BaseConnectionlessQueryTest {

    public QueryOptimizerTest() {
    }

    @Test
    public void testOrderByDropped() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE foo (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR) IMMUTABLE_ROWS=true");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.compileQuery("SELECT * FROM foo ORDER BY k");
        assertEquals(OrderBy.EMPTY_ORDER_BY,plan.getOrderBy());
    }

    @Test
    public void testOrderByNotDropped() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE foo (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR) IMMUTABLE_ROWS=true");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.compileQuery("SELECT * FROM foo ORDER BY v");
        assertTrue(OrderBy.EMPTY_ORDER_BY != plan.getOrderBy());
    }
    
    @Test
    public void testOrderByDroppedCompositeKey() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE foo (j INTEGER NOT NULL, k BIGINT NOT NULL, v VARCHAR CONSTRAINT pk PRIMARY KEY (j,k)) IMMUTABLE_ROWS=true");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.compileQuery("SELECT * FROM foo ORDER BY j,k");
        assertEquals(OrderBy.EMPTY_ORDER_BY,plan.getOrderBy());
    }

    @Test
    public void testOrderByNotDroppedCompositeKey() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE foo (j INTEGER NOT NULL, k BIGINT NOT NULL, v VARCHAR CONSTRAINT pk PRIMARY KEY (j,k)) IMMUTABLE_ROWS=true");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.compileQuery("SELECT * FROM foo ORDER BY k,j");
        assertTrue(OrderBy.EMPTY_ORDER_BY != plan.getOrderBy());
    }

    @Test
    public void testChooseIndexOverTable() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.compileQuery("SELECT k FROM t WHERE v1 = 'bar'");
        assertEquals("IDX", plan.getTableRef().getTable().getName().getString());
    }

    @Test
    public void testChooseTableOverIndex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.compileQuery("SELECT v1 FROM t WHERE k = 1");
        assertEquals("T", plan.getTableRef().getTable().getName().getString());
    }
    
    @Test
    public void testChooseTableForSelection() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.compileQuery("SELECT v1,v2 FROM t WHERE v1 = 'bar'");
        // Choose T because v2 is not in index
        assertEquals("T", plan.getTableRef().getTable().getName().getString());
    }
    
    @Test
    public void testChooseTableForDynCols() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.compileQuery("SELECT k FROM t(v3 VARCHAR) WHERE v1 = 'bar'");
        assertEquals("T", plan.getTableRef().getTable().getName().getString());
    }
    
    @Test
    public void testChooseTableForSelectionStar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.compileQuery("SELECT * FROM t WHERE v1 = 'bar'");
        // Choose T because v2 is not in index
        assertEquals("T", plan.getTableRef().getTable().getName().getString());
    }

    @Test
    public void testChooseIndexEvenWithSelectionStar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1) INCLUDE (v2)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.compileQuery("SELECT * FROM t WHERE v1 = 'bar'");
        assertEquals("IDX", plan.getTableRef().getTable().getName().getString());
    }

    @Test
    public void testChooseIndexFromOrderBy() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.compileQuery("SELECT k FROM t WHERE k = 30 ORDER BY v1 LIMIT 5");
        assertEquals("IDX", plan.getTableRef().getTable().getName().getString());
    }
    

    @Test
    public void testChooseIndexWithLongestRowKey() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx1 ON t(v1) INCLUDE(v2)");
        conn.createStatement().execute("CREATE INDEX idx2 ON t(v1,v2)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.compileQuery("SELECT k FROM t WHERE v1 = 'foo' AND v2 = 'bar'");
        assertEquals("IDX2", plan.getTableRef().getTable().getName().getString());
    }
}
