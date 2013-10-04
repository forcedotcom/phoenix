package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.ENTITYHISTID1;
import static com.salesforce.phoenix.util.TestUtil.ENTITYHISTIDS;
import static com.salesforce.phoenix.util.TestUtil.ENTITY_HISTORY_TABLE_NAME;
import static com.salesforce.phoenix.util.TestUtil.PARENTID1;
import static com.salesforce.phoenix.util.TestUtil.PARENTIDS;
import static com.salesforce.phoenix.util.TestUtil.PHOENIX_JDBC_URL;
import static com.salesforce.phoenix.util.TestUtil.ROW1;
import static com.salesforce.phoenix.util.TestUtil.ROW2;
import static com.salesforce.phoenix.util.TestUtil.ROW3;
import static com.salesforce.phoenix.util.TestUtil.ROW4;
import static com.salesforce.phoenix.util.TestUtil.ROW5;
import static com.salesforce.phoenix.util.TestUtil.ROW6;
import static com.salesforce.phoenix.util.TestUtil.ROW7;
import static com.salesforce.phoenix.util.TestUtil.ROW8;
import static com.salesforce.phoenix.util.TestUtil.ROW9;
import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.util.PhoenixRuntime;

public class RowValueConstructorTest extends BaseClientMangedTimeTest {
    
    @Test
    public void testRowValueConstructorInWhereWithEqualsExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ?=organization_id  AND (a_integer, x_integer) = (7, 5)";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                assertTrue(rs.getInt(1) == 7);
                assertTrue(rs.getInt(2) == 5);
                count++;
            }
            assertTrue(count == 1);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRowValueConstructorInWhereWithGreaterThanExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ?=organization_id  AND (a_integer, x_integer) >= (4, 4)";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                assertTrue(rs.getInt(1) >= 4);
                assertTrue(rs.getInt(1) == 4 ? rs.getInt(2) >= 4 : rs.getInt(2) >= 0);
                count++;
            }
            // we have 6 values for a_integer present in the atable where a >= 4. x_integer is null for a_integer = 4. So the query should have returned 5 rows.
            assertTrue(count == 5);   
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRowValueConstructorInWhereWithUnEqualNumberArgs() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ?=organization_id  AND (a_integer, x_integer, y_integer) >= (7, 5)";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                assertTrue(rs.getInt(1) >= 7);
                assertTrue(rs.getInt(1) == 7 ? rs.getInt(2) >= 5 : rs.getInt(2) >= 0);
                count++;
            }
            // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
            assertTrue(count == 3); 
        } finally {
            conn.close();
        }
    }

    @Test
    public void testBindVarsInRowValueConstructor() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ?=organization_id  AND (a_integer, x_integer) = (?, ?)";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setInt(2, 7);
            statement.setInt(3, 5);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                assertTrue(rs.getInt(1) == 7);
                assertTrue(rs.getInt(2) == 5);
                count++;
            }
            assertTrue(count == 1); 
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRowValueConstructorOnLHSAndLiteralExpressionOnRHS() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ?=organization_id  AND (a_integer, x_integer) >= 7";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                count++;
            }
            // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
            assertTrue(count == 3); 
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRowValueConstructorOnRHSLiteralExpressionOnLHS() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ?=organization_id  AND 7 <= (a_integer, x_integer)";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                count++;
            }
            // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
            assertTrue(count == 3); 
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRowValueConstructorOnLHSBuiltInFunctionOperatingOnIntegerLiteralRHS() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ?=organization_id  AND (a_integer, x_integer) >= to_number('7')";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                count++;
            }
            // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
            assertTrue(count == 3); 
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRowValueConstructorOnRHSWithBuiltInFunctionOperatingOnIntegerLiteralOnLHS() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ?=organization_id  AND to_number('7') <= (a_integer, x_integer)";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                count++;
            }
            // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
            assertTrue(count == 3); 
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRowValueConstructorOnLHSWithBuiltInFunctionOperatingOnColumnRefOnRHS() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts - 1);
        String upsertQuery = "UPSERT INTO aTable(organization_id, entity_id, a_string) values (?, ?, ?)";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.setAutoCommit(true);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertQuery);
            statement.setString(1, tenantId);
            statement.setString(2, ROW1);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW2);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW3);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW4);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW5);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW6);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW7);
            statement.setString(3, "7");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW8);
            statement.setString(3, "7");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW9);
            statement.setString(3, "7");
            statement.executeUpdate();
            conn.commit();

            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
            conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
            statement = conn.prepareStatement("select a_string from atable where organization_id = ? and (6, x_integer) <= to_number(a_string)");
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                count++;
            }
            // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
            assertTrue(count == 3); 
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRowValueConstructorOnRHSWithBuiltInFunctionOperatingOnColumnRefOnLHS() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts - 1);
        String upsertQuery = "UPSERT INTO aTable(organization_id, entity_id, a_string) values (?, ?, ?)";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.setAutoCommit(true);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertQuery);
            statement.setString(1, tenantId);
            statement.setString(2, ROW1);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW2);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW3);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW4);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW5);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW6);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW7);
            statement.setString(3, "7");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW8);
            statement.setString(3, "7");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW9);
            statement.setString(3, "7");
            statement.executeUpdate();
            conn.commit();

            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
            conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
            statement = conn.prepareStatement("select a_string from atable where organization_id = ? and to_number(a_string) >= (6, 6)");
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                count++;
            }
            // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
            assertTrue(count == 3); 
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testQueryMoreFunctionalityUsingRowValueConstructor() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        Date date = new Date(System.currentTimeMillis());
        initEntityHistoryTableValues(tenantId, getDefaultSplits(tenantId), date, ts - 1);
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        String startingOrgId = tenantId;
        String startingParentId = PARENTID1;
        Date startingDate = date;
        String startingEntityHistId = ENTITYHISTID1;
        PreparedStatement statement = conn.prepareStatement("select organization_id, parent_id, created_date, entity_history_id, old_value, new_value from " + ENTITY_HISTORY_TABLE_NAME + 
                     " WHERE (organization_id, parent_id, created_date, entity_history_id) > (?, ?, ?, ?) ORDER BY organization_id, parent_id, created_date, entity_history_id LIMIT 3 ");
        statement.setString(1, startingOrgId);
        statement.setString(2, startingParentId);
        statement.setDate(3, startingDate);
        statement.setString(4, startingEntityHistId);
        ResultSet rs = statement.executeQuery();
        int count = 0;
        //this loop should work on rows 2, 3, 4.
        int i = 1;
        while(rs.next()) {
            assertTrue(rs.getString(2) == PARENTIDS.get(i));
            assertTrue(rs.getString(4) == ENTITYHISTIDS.get(i));
            i++;
            count++;
            if(count == 2) {
                startingOrgId = rs.getString(1);
                startingParentId = rs.getString(2);
                date = rs.getDate(3);
                startingEntityHistId = rs.getString(4);
            }
        }
        //We will now use the row 4's pk values for bind variables. 
        statement.setString(1, startingOrgId);
        statement.setString(2, startingParentId);
        statement.setDate(3, startingDate);
        statement.setString(4, startingEntityHistId);
        rs = statement.executeQuery();
        //this loop now should work on rows 5, 6, 7.
        while(rs.next()) {
            assertTrue(rs.getString(2) == PARENTIDS.get(i));
            assertTrue(rs.getString(4) == ENTITYHISTIDS.get(i));
            i++;
        }
    }
}
