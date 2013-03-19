package com.salesforce.phoenix.end2end;

import static org.junit.Assert.*;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class SkipScanQueryTest extends BaseHBaseManagedTimeTest {
    
    private void initInTable(Connection conn, List<Integer> data) throws SQLException {
        String ddl = "CREATE TABLE IF NOT EXISTS inTest (" + 
                     "  i INTEGER NOT NULL PRIMARY KEY)";
        conn.createStatement().executeUpdate(ddl);
        
        // Test upsert correct values 
        String query = "UPSERT INTO inTest VALUES(?)";
        PreparedStatement stmt = conn.prepareStatement(query);
        for (Integer i : data) {
            stmt.setInt(1, i);
            stmt.execute();
        }
        conn.commit();
    }
    
    @Test
    public void testInQuery() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(false);
        initInTable(conn,Arrays.asList(2,7));
        try {
            String query;
            query = "SELECT i FROM inTest WHERE i IN (1,2,4,5,7,8)";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(7, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }


}
