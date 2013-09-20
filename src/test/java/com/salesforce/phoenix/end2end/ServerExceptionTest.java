package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.*;
import java.util.Properties;

import org.junit.Test;


public class ServerExceptionTest extends BaseHBaseManagedTimeTest {

    @Test
    public void testServerExceptionBackToClient() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE IF NOT EXISTS t1(pk VARCHAR NOT NULL PRIMARY KEY, " +
                    "col1 INTEGER, col2 INTEGER)";
            createTestTable(getUrl(), ddl);
            
            String query = "UPSERT INTO t1 VALUES(?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "1");
            stmt.setInt(2, 1);
            stmt.setInt(3, 0);
            stmt.execute();
            conn.commit();
            
            query = "SELECT * FROM t1 where col1/col2 > 0";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            rs.getInt(1);
            fail("Should have caught exception.");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("ERROR 212 (22012): Arithmetic error on server. / by zero"));
        } finally {
            conn.close();
        }
    }

}
