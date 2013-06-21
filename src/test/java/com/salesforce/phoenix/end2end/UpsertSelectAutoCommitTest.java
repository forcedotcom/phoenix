package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.*;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

import org.junit.Test;

public class UpsertSelectAutoCommitTest extends BaseHBaseManagedTimeTest {

    public UpsertSelectAutoCommitTest() {
    }

    @Test
    public void testAutoCommitUpsertSelect() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        conn.createStatement().execute("CREATE TABLE atable (ORGANIZATION_ID CHAR(15) NOT NULL, ENTITY_ID CHAR(15) NOT NULL, A_STRING VARCHAR\n" +
        "CONSTRAINT pk PRIMARY KEY (organization_id, entity_id))");
        
        String tenantId = getOrganizationId();
       // Insert all rows at ts
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " +
                "ATABLE(" +
                "    ORGANIZATION_ID, " +
                "    ENTITY_ID, " +
                "    A_STRING " +
                "    )" +
                "VALUES (?, ?, ?)");
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW1);
        stmt.setString(3, A_VALUE);
        stmt.execute();
        
        String query = "SELECT entity_id, a_string FROM ATABLE";
        PreparedStatement statement = conn.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        
        assertTrue(rs.next());
        assertEquals(ROW1, rs.getString(1));
        assertEquals(A_VALUE, rs.getString(2));
        assertFalse(rs.next());
        
        conn.createStatement().execute("CREATE TABLE atable2 (ORGANIZATION_ID CHAR(15) NOT NULL, ENTITY_ID CHAR(15) NOT NULL, A_STRING VARCHAR\n" +
        "CONSTRAINT pk PRIMARY KEY (organization_id, entity_id))");
        
        conn.createStatement().execute("UPSERT INTO atable2 SELECT * FROM ATABLE");
        query = "SELECT entity_id, a_string FROM ATABLE2";
        statement = conn.prepareStatement(query);
        rs = statement.executeQuery();
        
        assertTrue(rs.next());
        assertEquals(ROW1, rs.getString(1));
        assertEquals(A_VALUE, rs.getString(2));
        assertFalse(rs.next());
        
    }
}
