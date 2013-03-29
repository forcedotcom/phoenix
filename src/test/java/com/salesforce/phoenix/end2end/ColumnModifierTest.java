package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

public class ColumnModifierTest extends BaseHBaseManagedTimeTest {
    
    private static final String TABLE = "testColumnSortOrder";

    @Test
    public void testNoOder() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (pk VARCHAR NOT NULL PRIMARY KEY)";
        runQueryTest(ddl, "pk", new Object[][]{{"a"}, {"b"}, {"c"}}, new Object[][]{{"a"}, {"b"}, {"c"}});
    }                                                           

    @Test
    public void testNoOrderCompositePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid, code))";
        Object[][] rows = new Object[][]{{"o1", 1}, {"o2", 2}, {"o3", 3}};
        runQueryTest(ddl, ar("oid", "code"), rows, rows);
    }
    
    @Test
    public void testAscOrderInlinePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (pk VARCHAR NOT NULL PRIMARY KEY ASC)";
        runQueryTest(ddl, "pk", new Object[][]{{"a"}, {"b"}, {"c"}}, new Object[][]{{"a"}, {"b"}, {"c"}});
    }
    
    @Test
    public void testAscOrderCompositePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid ASC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o1", 2}, {"o1", 3}};
        Object[][] expectedRows = new Object[][]{{"o1", 3}, {"o1", 2}, {"o1", 1}};
        runQueryTest(ddl, ar("oid", "code"), insertedRows, expectedRows);        
    }

    @Test
    public void testDescOrderInlinePK() throws Exception {
        for (String type : new String[]{"CHAR(2)", "VARCHAR"}) {
            String ddl = "CREATE TABLE " + TABLE + " (pk ${type} NOT NULL PRIMARY KEY DESC)".replace("${type}", type);
            runQueryTest(ddl, "pk", new Object[][]{{"aa"}, {"bb"}, {"cc"}}, new Object[][]{{"cc"}, {"bb"}, {"aa"}});
        }
    }
    
    @Test
    public void testDescOrderCompositePK1() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o2", 2}, {"o3", 3}};
        Object[][] expectedRows = new Object[][]{{"o3", 3}, {"o2", 2}, {"o1", 1}};
        runQueryTest(ddl, ar("oid", "code"), insertedRows, expectedRows);        
    }
    
    @Test
    public void testDescOrderCompositePK2() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o1", 2}, {"o1", 3}};
        Object[][] expectedRows = new Object[][]{{"o1", 3}, {"o1", 2}, {"o1", 1}};
        runQueryTest(ddl, ar("oid", "code"), insertedRows, expectedRows);        
    }    

    @Test
    public void testEqDescInlinePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (pk VARCHAR NOT NULL PRIMARY KEY DESC)";
        runQueryTest(ddl, ar("pk"), new Object[][]{{"a"}, {"b"}, {"c"}}, new Object[][]{{"b"}}, new Condition("pk", "=", "'b'"));
    }
    
    @Test
    public void testEqDescCompositePK1() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o2", 2}, {"o3", 3}};
        runQueryTest(ddl, ar("oid", "code"), insertedRows, new Object[][]{{"o2", 2}}, new Condition("oid", "=", "'o2'"));        
    }
    
    @Test
    public void testEqDescCompositePK2() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o1", 2}, {"o1", 3}};
        runQueryTest(ddl, ar("oid", "code"), insertedRows, new Object[][]{{"o1", 2}}, new Condition("code", "=", "2"));        
    }
    
    @Test
    public void testGtDescCompositePK3() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid ASC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o1", 2}, {"o1", 3}};
        Object[][] expectedRows = new Object[][]{{"o1", 2}, {"o1", 1}};
        runQueryTest(ddl, ar("oid", "code"), insertedRows, expectedRows, new Condition("code", "<", "3"));        
    }      

    private void runQueryTest(String ddl, String columnName, Object[][] values, Object[][] expectedValues) throws Exception {
        runQueryTest(ddl, new String[]{columnName}, values, expectedValues, null);
    }
    
    private void runQueryTest(String ddl, String[] columnNames, Object[][] values, Object[][] expectedValues) throws Exception {
        runQueryTest(ddl, columnNames, values, expectedValues, null);
    }

    private void runQueryTest(String ddl, String[] columnNames, Object[][] values, Object[][] expectedValues, Condition condition) throws Exception {

        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        try {

            conn.setAutoCommit(false);

            createTestTable(getUrl(), ddl);

            String colList = "";
            String placeholderList = "";
            for (int i = 0; i < columnNames.length; i++) {                
                colList += columnNames[i];
                placeholderList += "?";
                if (i < columnNames.length - 1) {
                    colList += ",";
                    placeholderList += ",";
                }
            }
            String dml = "UPSERT INTO " + TABLE + " (" + colList + ") VALUES(" + placeholderList +")";
            PreparedStatement stmt = conn.prepareStatement(dml);

            for (int row = 0; row < values.length; row++) {
                for (int col = 0; col < values[row].length; col++) {
                    Object value = values[row][col];
                    stmt.setObject(col + 1, value);
                }
                stmt.execute();
            }
            conn.commit();
            
            String selectClause = "SELECT " + colList + " FROM " + TABLE;
            
            if (condition != null) {
                String query = condition.appendWhere(selectClause);
                runQuery(conn, query, expectedValues);
                query = condition.reverse().appendWhere(selectClause);
                runQuery(conn, query, expectedValues);
            } else {            
                runQuery(conn, selectClause, expectedValues);
            }
            
        } finally {
            conn.close();
        }
    }
    
    private static void runQuery(Connection connection, String query, Object[][] expectedValues) throws Exception {
        PreparedStatement stmt = connection.prepareStatement(query);

        ResultSet rs = stmt.executeQuery();
        int rowCounter = 0;
        while (rs.next()) {
            Object[] cols = new Object[expectedValues[rowCounter].length];
            for (int colCounter = 0; colCounter < expectedValues[rowCounter].length; colCounter++) {
                cols[colCounter] = rs.getObject(colCounter+1);
            }
            Assert.assertArrayEquals("Unexpected result for query " + query, expectedValues[rowCounter], cols);
            rowCounter++;
        }
        Assert.assertEquals("Unexpected number of rows for query " + query, expectedValues.length, rowCounter);
    }
    
    private static String[] ar(String...args) {
        return args;
    }
    
    private static class Condition {
        final String lhs;
        final String operator;
        final String rhs;
    
        Condition(String lhs, String operator, String rhs) {
            this.lhs = lhs;
            this.operator = operator;
            this.rhs = rhs;
        }
        
        Condition reverse() {
            return new Condition(rhs, getReversedOperator(), lhs);
        }
        
        String appendWhere(String query) {
            return query + " WHERE " + lhs + " " + operator + " " + rhs;
        }
        
        private String getReversedOperator() {
            if (operator.equals("<")) {
                return ">";
            }
            return operator;
        }
    }
}
