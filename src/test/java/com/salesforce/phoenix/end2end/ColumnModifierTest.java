package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.math.BigDecimal;
import java.math.BigInteger;
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
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o1", 2}, {"o1", 3}};
        Object[][] expectedRows = new Object[][]{{"o1", 2}, {"o1", 1}};
        runQueryTest(ddl, ar("oid", "code"), insertedRows, expectedRows, new Condition("code", "<", "3"));        
    }
    
    @Test
    public void testSubstDescCompositePK1() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code ASC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o2", 2}, {"o3", 3}};
        Object[][] expectedRows = new Object[][]{{"o3", 3}, {"o2", 2}};
        runQueryTest(ddl, ar("oid", "code"), insertedRows, expectedRows, new Condition("SUBSTR(oid, 2, 1)", ">", "'1'"));
    }
    
    @Test
    public void testSubstDescCompositePK2() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(4) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code ASC))";
        Object[][] insertedRows = new Object[][]{{"aaaa", 1}, {"bbbb", 2}, {"cccd", 3}};
        Object[][] expectedRows = new Object[][]{{"cccd", 3}};
        runQueryTest(ddl, ar("oid", "code"), insertedRows, expectedRows, new Condition("SUBSTR(oid, 4, 1)", "=", "'d'"));
    }    
    
    @Test
    public void testLTrimDescCompositePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(4) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code ASC))";
        Object[][] insertedRows = new Object[][]{{" o1 ", 1}, {"  o2", 2}, {"  o3", 3}};
        Object[][] expectedRows = new Object[][]{{"  o2", 2}};
        runQueryTest(ddl, ar("oid", "code"), insertedRows, expectedRows, new Condition("LTRIM(oid)", "=", "'o2'"));
    }

    @Test
    public void testCountDescCompositePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code ASC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o2", 2}, {"o3", 3}};
        Object[][] expectedRows = new Object[][]{{3l}};
        runQueryTest(ddl, ar("oid", "code"), ar("COUNT(oid)"), insertedRows, expectedRows);
    }
    
    @Test
    public void testSumDescCompositePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid ASC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 10}, {"o1", 20}, {"o1", 30}};
        Object[][] expectedRows = new Object[][]{{60l}};
        runQueryTest(ddl, ar("oid", "code"), ar("SUM(code)"), insertedRows, expectedRows);
    }    
    
    @Test
    public void testAvgDescCompositePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid ASC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 10}, {"o1", 20}, {"o1", 30}};
        Object[][] expectedRows = new Object[][]{{new BigDecimal(BigInteger.valueOf(2), -1)}};
        runQueryTest(ddl, ar("oid", "code"), ar("AVG(code)"), insertedRows, expectedRows);
    }
    
    private void runQueryTest(String ddl, String columnName, Object[][] rows, Object[][] expectedRows) throws Exception {
        runQueryTest(ddl, new String[]{columnName}, rows, expectedRows, null);
    }
    
    private void runQueryTest(String ddl, String[] columnNames, Object[][] rows, Object[][] expectedRows) throws Exception {
        runQueryTest(ddl, columnNames, rows, expectedRows, null);
    }
    
    private void runQueryTest(String ddl, String[] columnNames, Object[][] rows, Object[][] expectedRows, Condition condition) throws Exception {
        runQueryTest(ddl, columnNames, columnNames, rows, expectedRows, condition);
    }
    
    private void runQueryTest(String ddl, String[] columnNames, String[] projections, Object[][] rows, Object[][] expectedRows) throws Exception {
        runQueryTest(ddl, columnNames, projections, rows, expectedRows, null);
    }

    private void runQueryTest(String ddl, String[] columnNames, String[] projections, Object[][] rows, Object[][] expectedRows, Condition condition) throws Exception {

        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        try {

            conn.setAutoCommit(false);

            createTestTable(getUrl(), ddl);

            String columns = appendColumns(columnNames);
            String placeholders = appendPlaceholders(columnNames);
            String dml = "UPSERT INTO " + TABLE + " (" + columns + ") VALUES(" + placeholders +")";
            PreparedStatement stmt = conn.prepareStatement(dml);

            for (int row = 0; row < rows.length; row++) {
                for (int col = 0; col < rows[row].length; col++) {
                    Object value = rows[row][col];
                    stmt.setObject(col + 1, value);
                }
                stmt.execute();
            }
            conn.commit();
            
            String selectClause = "SELECT " + appendColumns(projections) + " FROM " + TABLE;
            
            if (condition != null) {
                String query = condition.appendWhere(selectClause);
                runQuery(conn, query, expectedRows);
                query = condition.reverse().appendWhere(selectClause);
                runQuery(conn, query, expectedRows);
            } else {            
                runQuery(conn, selectClause, expectedRows);
            }
            
        } finally {
            conn.close();
        }
    }
    
    private String appendColumns(String[] columnNames) {
        String appendedColumns = "";
        for (int i = 0; i < columnNames.length; i++) {                
            appendedColumns += columnNames[i];
            if (i < columnNames.length - 1) {
                appendedColumns += ",";
            }
        }
        return appendedColumns;
    }
    
    private String appendPlaceholders(String[] columnNames) {
        String placeholderList = "";
        for (int i = 0; i < columnNames.length; i++) {                
            placeholderList += "?";
            if (i < columnNames.length - 1) {
                placeholderList += ",";
            }
        }
        return placeholderList;        
    }
    
    private static void runQuery(Connection connection, String query, Object[][] expectedValues) throws Exception {
        PreparedStatement stmt = connection.prepareStatement(query);

        ResultSet rs = stmt.executeQuery();
        int rowCounter = 0;
        while (rs.next()) {
            if (rowCounter == expectedValues.length) {
                Assert.assertEquals("Exceeded number of expected rows for query" + query, expectedValues.length, rowCounter+1);
            }
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
            } else if (operator.equals(">")) {
                return "<";
            }
            return operator;
        }
    }
}
