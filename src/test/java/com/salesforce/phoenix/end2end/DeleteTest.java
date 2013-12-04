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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.salesforce.phoenix.util.QueryUtil;

public class DeleteTest extends BaseHBaseManagedTimeTest {
    private static final int NUMBER_OF_ROWS = 20;
    private static final int NTH_ROW_NULL = 5;
    
    private static void initTableValues(Connection conn) throws SQLException {
        ensureTableCreated(getUrl(),"IntIntKeyTest");
        String upsertStmt = "UPSERT INTO IntIntKeyTest VALUES(?,?)";
        PreparedStatement stmt = conn.prepareStatement(upsertStmt);
        for (int i = 0; i < NUMBER_OF_ROWS; i++) {
            stmt.setInt(1, i);
            if (i % NTH_ROW_NULL != 0) {
                stmt.setInt(2, i * 10);
            } else {
                stmt.setNull(2, Types.INTEGER);
            }
            stmt.execute();
        }
        conn.commit();
    }

    @Test
    public void testDeleteFilterNoAutoCommit() throws Exception {
        testDeleteFilter(false);
    }
    
    @Test
    public void testDeleteFilterAutoCommit() throws Exception {
        testDeleteFilter(true);
    }
    
    private void testDeleteFilter(boolean autoCommit) throws Exception {
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL);
        initTableValues(conn);
        
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT count(*) FROM IntIntKeyTest");
        assertTrue(rs.next());
        assertEquals(NUMBER_OF_ROWS, rs.getInt(1));

        String deleteStmt ;
        conn.setAutoCommit(autoCommit);
        deleteStmt = "DELETE FROM IntIntKeyTest WHERE j = 20";
        assertEquals(1,conn.createStatement().executeUpdate(deleteStmt));
        if (!autoCommit) {
            conn.commit();
        }
        
        String query = "SELECT count(*) FROM IntIntKeyTest";
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(NUMBER_OF_ROWS - 1, rs.getInt(1));
    }
    
    private static void assertIndexUsed (Connection conn, String query, String indexName, boolean expectedToBeUsed) throws SQLException {
        assertIndexUsed(conn, query, Collections.emptyList(), indexName, expectedToBeUsed);
    }

    private static void assertIndexUsed (Connection conn, String query, List<Object> binds, String indexName, boolean expectedToBeUsed) throws SQLException {
            PreparedStatement stmt = conn.prepareStatement("EXPLAIN " + query);
            for (int i = 0; i < binds.size(); i++) {
                stmt.setObject(i+1, binds.get(i));
            }
            ResultSet rs = stmt.executeQuery();
            String explainPlan = QueryUtil.getExplainPlan(rs);
            assertEquals(expectedToBeUsed, explainPlan.contains(" SCAN OVER " + indexName));
   }
    
    private void testDeleteRange(boolean autoCommit, boolean createIndex) throws Exception {
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL);
        initTableValues(conn);
        
        String indexName = "IDX";
        if (createIndex) {
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS idx ON IntIntKeyTest(j)");
        }
        
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT count(*) FROM IntIntKeyTest");
        assertTrue(rs.next());
        assertEquals(NUMBER_OF_ROWS, rs.getInt(1));

        rs = conn.createStatement().executeQuery("SELECT i FROM IntIntKeyTest WHERE j IS NULL");
        int i = 0, isNullCount = 0;
        while (rs.next()) {
            assertEquals(i,rs.getInt(1));
            i += NTH_ROW_NULL;
            isNullCount++;
        }
        rs = conn.createStatement().executeQuery("SELECT count(*) FROM IntIntKeyTest WHERE j IS NOT NULL");
        assertTrue(rs.next());
        assertEquals(NUMBER_OF_ROWS-isNullCount, rs.getInt(1));

        String deleteStmt ;
        PreparedStatement stmt;
        conn.setAutoCommit(autoCommit);
        deleteStmt = "DELETE FROM IntIntKeyTest WHERE i >= ? and i < ?";
        assertIndexUsed(conn, deleteStmt, Arrays.<Object>asList(5,10), indexName, false);
        stmt = conn.prepareStatement(deleteStmt);
        stmt.setInt(1, 5);
        stmt.setInt(2, 10);
        stmt.execute();
        if (!autoCommit) {
            conn.commit();
        }
        
        String query = "SELECT count(*) FROM IntIntKeyTest";
        assertIndexUsed(conn, query, indexName, createIndex);
        query = "SELECT count(*) FROM IntIntKeyTest";
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(NUMBER_OF_ROWS - (10-5), rs.getInt(1));
        
        deleteStmt = "DELETE FROM IntIntKeyTest WHERE j IS NULL";
        stmt = conn.prepareStatement(deleteStmt);
        assertIndexUsed(conn, deleteStmt, indexName, createIndex);
        stmt.execute();
        if (!autoCommit) {
            conn.commit();
        }
        rs = conn.createStatement().executeQuery("SELECT count(*) FROM IntIntKeyTest");
        assertTrue(rs.next());
        assertEquals(NUMBER_OF_ROWS - (10-5)-isNullCount+1, rs.getInt(1));
    }
    
    @Test
    public void testDeleteRangeNoAutoCommitNoIndex() throws Exception {
        testDeleteRange(false, false);
    }
    
    @Test
    public void testDeleteRangeAutoCommitNoIndex() throws Exception {
        testDeleteRange(true, false);
    }
    
    @Test
    public void testDeleteRangeNoAutoCommitWithIndex() throws Exception {
        testDeleteRange(false, true);
    }
    
    @Test
    public void testDeleteRangeAutoCommitWithIndex() throws Exception {
        testDeleteRange(true, true);
    }
    
    @Test
    public void testDeleteAllFromTableWithIndexAutoCommitSalting() throws SQLException {
        testDeleteAllFromTableWithIndex(true, true);
    }
    
    @Test
    public void testDeleteAllFromTableWithIndexAutoCommitNoSalting() throws SQLException {
        testDeleteAllFromTableWithIndex(true, false);
    }
    
    @Test
    public void testDeleteAllFromTableWithIndexNoAutoCommitNoSalting() throws SQLException {
        testDeleteAllFromTableWithIndex(false,false);
    }
    
    @Test
    public void testDeleteAllFromTableWithIndexNoAutoCommitSalted() throws SQLException {
        testDeleteAllFromTableWithIndex(false, true);
    }
    
    private void testDeleteAllFromTableWithIndex(boolean autoCommit, boolean isSalted) throws SQLException {
        Connection con = null;
        try {
            con = DriverManager.getConnection(getUrl());
            con.setAutoCommit(autoCommit);

            Statement stm = con.createStatement();
            stm.execute("CREATE TABLE IF NOT EXISTS web_stats (" +
            		"HOST CHAR(2) NOT NULL," +
            		"DOMAIN VARCHAR NOT NULL, " +
            		"FEATURE VARCHAR NOT NULL, " +
            		"DATE DATE NOT NULL, \n" + 
            		"USAGE.CORE BIGINT," +
            		"USAGE.DB BIGINT," +
            		"STATS.ACTIVE_VISITOR INTEGER " +
            		"CONSTRAINT PK PRIMARY KEY (HOST, DOMAIN, FEATURE, DATE))" + (isSalted ? " SALT_BUCKETS=3" : ""));
            stm.execute("CREATE INDEX web_stats_idx ON web_stats (CORE,DB,ACTIVE_VISITOR)");
            stm.close();

            PreparedStatement psInsert = con
                    .prepareStatement("UPSERT INTO web_stats(HOST, DOMAIN, FEATURE, DATE, CORE, DB, ACTIVE_VISITOR) VALUES(?,?, ? , ?, ?, ?, ?)");
            psInsert.setString(1, "AA");
            psInsert.setString(2, "BB");
            psInsert.setString(3, "CC");
            psInsert.setDate(4, new Date(0));
            psInsert.setLong(5, 1L);
            psInsert.setLong(6, 2L);
            psInsert.setLong(7, 3);
            psInsert.execute();
            psInsert.close();
            if (!autoCommit) {
                con.commit();
            }
            
            con.createStatement().execute("DELETE FROM web_stats");
            if (!autoCommit) {
                con.commit();
            }
            
            ResultSet rs = con.createStatement().executeQuery("SELECT /*+ NO_INDEX */ count(*) FROM web_stats");
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));

            rs = con.createStatement().executeQuery("SELECT count(*) FROM web_stats_idx");
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));

        } finally {
            try {
                con.close();
            } catch (Exception ex) {
            }
        }
    }
    
    @Test
    public void testDeleteRowFromTableWithImmutableIndex() throws SQLException {
        Connection con = null;
        try {
            boolean autoCommit = false;
            con = DriverManager.getConnection(getUrl());
            con.setAutoCommit(autoCommit);

            Statement stm = con.createStatement();
            stm.execute("CREATE TABLE IF NOT EXISTS web_stats (" +
                    "HOST CHAR(2) NOT NULL," +
                    "DOMAIN VARCHAR NOT NULL, " +
                    "FEATURE VARCHAR NOT NULL, " +
                    "DATE DATE NOT NULL, \n" + 
                    "USAGE.CORE BIGINT," +
                    "USAGE.DB BIGINT," +
                    "STATS.ACTIVE_VISITOR INTEGER " +
                    "CONSTRAINT PK PRIMARY KEY (HOST, DOMAIN, FEATURE, DATE)) IMMUTABLE_ROWS=true");
            stm.execute("CREATE INDEX web_stats_idx ON web_stats (DATE, FEATURE)");
            stm.close();

            Date date = new Date(0);
            PreparedStatement psInsert = con
                    .prepareStatement("UPSERT INTO web_stats(HOST, DOMAIN, FEATURE, DATE, CORE, DB, ACTIVE_VISITOR) VALUES(?,?, ? , ?, ?, ?, ?)");
            psInsert.setString(1, "AA");
            psInsert.setString(2, "BB");
            psInsert.setString(3, "CC");
            psInsert.setDate(4, date);
            psInsert.setLong(5, 1L);
            psInsert.setLong(6, 2L);
            psInsert.setLong(7, 3);
            psInsert.execute();
            psInsert.close();
            if (!autoCommit) {
                con.commit();
            }
            
            psInsert = con.prepareStatement("DELETE FROM web_stats WHERE (HOST, DOMAIN, FEATURE, DATE) = (?,?,?,?)");
            psInsert.setString(1, "AA");
            psInsert.setString(2, "BB");
            psInsert.setString(3, "CC");
            psInsert.setDate(4, date);
            psInsert.execute();
            if (!autoCommit) {
                con.commit();
            }
            
            ResultSet rs = con.createStatement().executeQuery("SELECT /*+ NO_INDEX */ count(*) FROM web_stats");
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));

            rs = con.createStatement().executeQuery("SELECT count(*) FROM web_stats_idx");
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));

        } finally {
            try {
                con.close();
            } catch (Exception ex) {
            }
        }
    }
    
    
    @Test
    public void testDeleteAllFromTableNoAutoCommit() throws SQLException {
        testDeleteAllFromTable(false);
    }

    @Test
    public void testDeleteAllFromTableAutoCommit() throws SQLException {
        testDeleteAllFromTable(true);
    }
    
    private void testDeleteAllFromTable(boolean autoCommit) throws SQLException {
        Connection con = null;
        try {
            con = DriverManager.getConnection(getUrl());
            con.setAutoCommit(autoCommit);

            Statement stm = con.createStatement();
            stm.execute("CREATE TABLE IF NOT EXISTS web_stats (" +
                    "HOST CHAR(2) NOT NULL," +
                    "DOMAIN VARCHAR NOT NULL, " +
                    "FEATURE VARCHAR NOT NULL, " +
                    "DATE DATE NOT NULL, \n" + 
                    "USAGE.CORE BIGINT," +
                    "USAGE.DB BIGINT," +
                    "STATS.ACTIVE_VISITOR INTEGER " +
                    "CONSTRAINT PK PRIMARY KEY (HOST, DOMAIN, FEATURE, DATE))");
            stm.close();

            PreparedStatement psInsert = con
                    .prepareStatement("UPSERT INTO web_stats(HOST, DOMAIN, FEATURE, DATE, CORE, DB, ACTIVE_VISITOR) VALUES(?,?, ? , ?, ?, ?, ?)");
            psInsert.setString(1, "AA");
            psInsert.setString(2, "BB");
            psInsert.setString(3, "CC");
            psInsert.setDate(4, new Date(0));
            psInsert.setLong(5, 1L);
            psInsert.setLong(6, 2L);
            psInsert.setLong(7, 3);
            psInsert.execute();
            psInsert.close();
            if (!autoCommit) {
                con.commit();
            }
            
            con.createStatement().execute("DELETE FROM web_stats");
            if (!autoCommit) {
                con.commit();
            }
            
            ResultSet rs = con.createStatement().executeQuery("SELECT /*+ NO_INDEX */ count(*) FROM web_stats");
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));
        } finally {
            try {
                con.close();
            } catch (Exception ex) {
            }
        }
    }
}


