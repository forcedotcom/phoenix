package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Calendar;
import java.sql.Date;
import java.util.GregorianCalendar;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

public class DescColumnSortOrderTest extends BaseHBaseManagedTimeTest {
    
    private static final String TABLE = "DescColumnSortOrderTest";

    @Test
    public void noOder() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (pk VARCHAR NOT NULL PRIMARY KEY)";
        runQueryTest(ddl, "pk", new Object[][]{{"a"}, {"b"}, {"c"}}, new Object[][]{{"a"}, {"b"}, {"c"}});
    }                                                           

    @Test
    public void noOrderCompositePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid, code))";
        Object[][] rows = new Object[][]{{"o1", 1}, {"o2", 2}, {"o3", 3}};
        runQueryTest(ddl, upsert("oid", "code"), rows, rows);
    }
    
    @Test
    public void ascOrderInlinePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (pk VARCHAR NOT NULL PRIMARY KEY ASC)";
        runQueryTest(ddl, "pk", new Object[][]{{"a"}, {"b"}, {"c"}}, new Object[][]{{"a"}, {"b"}, {"c"}});
    }
    
    @Test
    public void ascOrderCompositePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid ASC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o1", 2}, {"o1", 3}};
        Object[][] expectedRows = new Object[][]{{"o1", 3}, {"o1", 2}, {"o1", 1}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, expectedRows);        
    }

    @Test
    public void descOrderInlinePK() throws Exception {
        for (String type : new String[]{"CHAR(2)", "VARCHAR"}) {
            String ddl = "CREATE TABLE " + TABLE + " (pk ${type} NOT NULL PRIMARY KEY DESC)".replace("${type}", type);
            runQueryTest(ddl, "pk", new Object[][]{{"aa"}, {"bb"}, {"cc"}}, new Object[][]{{"cc"}, {"bb"}, {"aa"}});
        }
    }
    
    @Test
    public void descOrderCompositePK1() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o2", 2}, {"o3", 3}};
        Object[][] expectedRows = new Object[][]{{"o3", 3}, {"o2", 2}, {"o1", 1}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, expectedRows);        
    }
    
    @Test
    public void descOrderCompositePK2() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o1", 2}, {"o1", 3}};
        Object[][] expectedRows = new Object[][]{{"o1", 3}, {"o1", 2}, {"o1", 1}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, expectedRows);        
    }    

    @Test
    public void equalityDescInlinePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (pk VARCHAR NOT NULL PRIMARY KEY DESC)";
        runQueryTest(ddl, upsert("pk"), new Object[][]{{"a"}, {"b"}, {"c"}}, new Object[][]{{"b"}}, new WhereCondition("pk", "=", "'b'"));
    }
    
    @Test
    public void equalityDescCompositePK1() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o2", 2}, {"o3", 3}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, new Object[][]{{"o2", 2}}, new WhereCondition("oid", "=", "'o2'"));        
    }
    
    @Test
    public void equalityDescCompositePK2() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o1", 2}, {"o1", 3}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, new Object[][]{{"o1", 2}}, new WhereCondition("code", "=", "2"));        
    }
    
    @Test
    public void greaterThanDescCompositePK3() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o1", 2}, {"o1", 3}};
        Object[][] expectedRows = new Object[][]{{"o1", 2}, {"o1", 1}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, expectedRows, new WhereCondition("code", "<", "3"));        
    }
    
    @Test
    public void substrDescCompositePK1() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(3) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code ASC))";
        Object[][] insertedRows = new Object[][]{{"ao1", 1}, {"bo2", 2}, {"co3", 3}};
        Object[][] expectedRows = new Object[][]{{"co3", 3}, {"bo2", 2}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, expectedRows, new WhereCondition("SUBSTR(oid, 3, 1)", ">", "'1'"));
    }
        
    @Test
    public void substrDescCompositePK2() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(4) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code ASC))";
        Object[][] insertedRows = new Object[][]{{"aaaa", 1}, {"bbbb", 2}, {"cccd", 3}};
        Object[][] expectedRows = new Object[][]{{"cccd", 3}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, expectedRows, new WhereCondition("SUBSTR(oid, 4, 1)", "=", "'d'"));
    }    
    
    @Test
    public void lTrimDescCompositePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(4) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code ASC))";
        Object[][] insertedRows = new Object[][]{{" o1 ", 1}, {"  o2", 2}, {"  o3", 3}};
        Object[][] expectedRows = new Object[][]{{"  o2", 2}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, expectedRows, new WhereCondition("LTRIM(oid)", "=", "'o2'"));
    }

    @Test
    public void countDescCompositePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code ASC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o2", 2}, {"o3", 3}};
        Object[][] expectedRows = new Object[][]{{3l}};
        runQueryTest(ddl, upsert("oid", "code"), select("COUNT(oid)"), insertedRows, expectedRows);
    }
    
    @Test
    public void sumDescCompositePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (n1 INTEGER NOT NULL, n2 DECIMAL(10, 2) NOT NULL, n3 BIGINT NOT NULL " + 
            "constraint pk primary key (n1 DESC, n2 DESC, n3 DESC))";
        Object[][] insertedRows = new Object[][]{{10, bdec(10.2), 21l}, {20, bdec(20.2), 32l}, {30, bdec(30.2), 43l}};
        Object[][] expectedRows = new Object[][]{{60l, bdec(60.6), 96l}};
        runQueryTest(ddl, upsert("n1", "n2", "n3"), select("SUM(n1), SUM(n2), SUM(n3)"), insertedRows, expectedRows);
    }    
    
    @Test
    public void avgDescCompositePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (n1 INTEGER NOT NULL, n2 DECIMAL(10, 2) NOT NULL, n3 BIGINT NOT NULL " + 
            "constraint pk primary key (n1 DESC, n2 DESC, n3 DESC))";
        Object[][] insertedRows = new Object[][]{{10, bdec(10.2), 21l}, {20, bdec(20.2), 32l}, {30, bdec(30.2), 43l}};
        Object[][] expectedRows = new Object[][]{{new BigDecimal(bint(2), -1), bdec(20.2), BigDecimal.valueOf(32)}};
        runQueryTest(ddl, upsert("n1", "n2", "n3"), select("AVG(n1), AVG(n2), AVG(n3)"), insertedRows, expectedRows);
    }
    
    @Test
    public void minDescCompositePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (n1 INTEGER NOT NULL, n2 DECIMAL(10, 2) NOT NULL, n3 BIGINT NOT NULL " + 
            "constraint pk primary key (n1 DESC, n2 DESC, n3 DESC))";
        Object[][] insertedRows = new Object[][]{{10, bdec(10.2), 21l}, {20, bdec(20.2), 32l}, {30, bdec(30.2), 43l}};
        Object[][] expectedRows = new Object[][]{{10, bdec(10.2), 21l}};
        runQueryTest(ddl, upsert("n1", "n2", "n3"), select("MIN(n1), MIN(n2), MIN(n3)"), insertedRows, expectedRows);
    }
    
    @Test
    public void maxDescCompositePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (n1 INTEGER NOT NULL, n2 DECIMAL(10, 2) NOT NULL, n3 BIGINT NOT NULL " + 
            "constraint pk primary key (n1 DESC, n2 DESC, n3 DESC))";
        Object[][] insertedRows = new Object[][]{{10, bdec(10.2), 21l}, {20, bdec(20.2), 32l}, {30, bdec(30.2), 43l}};
        Object[][] expectedRows = new Object[][]{{30, bdec(30.2), 43l}};
        runQueryTest(ddl, upsert("n1", "n2", "n3"), select("MAX(n1), MAX(n2), MAX(n3)"), insertedRows, expectedRows);
    }
    
    @Test
    public void havingSumDescCompositePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (name CHAR(1) NOT NULL, code INTEGER NOT NULL " + 
            "constraint pk primary key (name DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"a", 10}, {"a", 20}, {"b", 100}}; 
        Object[][] expectedRows = new Object[][]{{"a", 30l}};
        runQueryTest(ddl, upsert("name", "code"), select("name", "SUM(code)"), insertedRows, expectedRows, 
            new HavingCondition("name", "SUM(code) = 30"));
    }
    
    @Test
    public void additionOnDescCompositePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (n1 INTEGER NOT NULL, n2 DECIMAL(10, 2) NOT NULL, n3 BIGINT NOT NULL, d1 DATE NOT NULL " + 
            "constraint pk primary key (n1 DESC, n2 DESC, n3 DESC, d1 DESC))";
        Object[][] insertedRows = new Object[][]{
            {10, bdec(10.2), 21l, date(1, 10, 2001)}, {20, bdec(20.2), 32l, date(2, 6, 2001)}, {30, bdec(30.2), 43l, date(3, 1, 2001)}};
        Object[][] expectedRows = new Object[][]{
            {31l, bdec(32.2), 46l, date(3, 5, 2001)}, {21l, bdec(22.2), 35l, date(2, 10, 2001)}, {11l, bdec(12.2), 24l, date(1, 14, 2001)}};
        runQueryTest(ddl, upsert("n1", "n2", "n3", "d1"), select("n1+1, n2+2, n3+3", "d1+4"), insertedRows, expectedRows);
    }
    
    @Test
    public void subtractionOnDescCompositePK() throws Exception {
        String ddl = "CREATE TABLE " + TABLE + " (n1 INTEGER NOT NULL, n2 DECIMAL(10, 2) NOT NULL, n3 BIGINT NOT NULL, d1 DATE NOT NULL " + 
            "constraint pk primary key (n1 DESC, n2 DESC, n3 DESC, d1 DESC))";
        Object[][] insertedRows = new Object[][]{
            {10, bdec(10.2), 21l, date(1, 10, 2001)}, {20, bdec(20.2), 32l, date(2, 6, 2001)}, {30, bdec(30.2), 43l, date(3, 10, 2001)}};
        Object[][] expectedRows = new Object[][]{
            {29l, bdec(28.2), 40l, date(3, 6, 2001)}, {19l, bdec(18.2), 29l, date(2, 2, 2001)}, {9l, bdec(8.2), 18l, date(1, 6, 2001)}};
        runQueryTest(ddl, upsert("n1", "n2", "n3", "d1"), select("n1-1, n2-2, n3-3", "d1-4"), insertedRows, expectedRows);
    }
    
    private void runQueryTest(String ddl, String columnName, Object[][] rows, Object[][] expectedRows) throws Exception {
        runQueryTest(ddl, new String[]{columnName}, rows, expectedRows, null);
    }
    
    private void runQueryTest(String ddl, String[] columnNames, Object[][] rows, Object[][] expectedRows) throws Exception {
        runQueryTest(ddl, columnNames, rows, expectedRows, null);
    }
    
    private void runQueryTest(String ddl, String[] columnNames, Object[][] rows, Object[][] expectedRows, WhereCondition condition) throws Exception {
        runQueryTest(ddl, columnNames, columnNames, rows, expectedRows, condition, null);
    }
    
    private void runQueryTest(String ddl, String[] columnNames, String[] projections, Object[][] rows, Object[][] expectedRows) throws Exception {
        runQueryTest(ddl, columnNames, projections, rows, expectedRows, null, null);
    }
    
    private void runQueryTest(String ddl, String[] columnNames, String[] projections, Object[][] rows, Object[][] expectedRows, HavingCondition havingCondition) throws Exception {
        runQueryTest(ddl, columnNames, projections, rows, expectedRows, null, havingCondition);
    }
    

    private void runQueryTest(
        String ddl, 
        String[] columnNames, 
        String[] projections, 
        Object[][] rows, Object[][] expectedRows, 
        WhereCondition whereCondition, 
        HavingCondition havingCondition) throws Exception 
    {
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
            
            if (whereCondition != null) {
                String query = whereCondition.appendWhere(selectClause);
                HavingCondition.appendHaving(havingCondition, query);
                runQuery(conn, query, expectedRows);
                query = whereCondition.reverse().appendWhere(selectClause);
                HavingCondition.appendHaving(havingCondition, query);
                runQuery(conn, query, expectedRows);
            } else {
                String query = HavingCondition.appendHaving(havingCondition, selectClause);
                runQuery(conn, query, expectedRows);
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
    
    private static Date date(int month, int day, int year) {
        Calendar cal = new GregorianCalendar();
        cal.set(Calendar.MONTH, month-1);
        cal.set(Calendar.DAY_OF_MONTH, day);
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.HOUR_OF_DAY, 10);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Date d = new Date(cal.getTimeInMillis()); 
        return d;
    }
    
    private static String[] upsert(String...args) {
        return args;
    }
    
    private static String[] select(String...args) {
        return args;
    }
    
    private static BigDecimal bdec(double d) {
        return BigDecimal.valueOf(d);
    }
    
    private static BigInteger bint(long l) {
        return BigInteger.valueOf(l);
    }    
    
    private static class WhereCondition {
        final String lhs;
        final String operator;
        final String rhs;
    
        WhereCondition(String lhs, String operator, String rhs) {
            this.lhs = lhs;
            this.operator = operator;
            this.rhs = rhs;
        }
        
        WhereCondition reverse() {
            return new WhereCondition(rhs, getReversedOperator(), lhs);
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
    
    private static class HavingCondition {
        
        private String groupby;
        private String having;
        
        HavingCondition(String groupby, String having) {
            this.groupby = groupby;
            this.having = having;
        }
        
        static String appendHaving(HavingCondition havingCondition, String query) {
            if (havingCondition == null) {
                return query;
            }
            return query + " GROUP BY " + havingCondition.groupby + " HAVING " + havingCondition.having;
        }
    }
}
