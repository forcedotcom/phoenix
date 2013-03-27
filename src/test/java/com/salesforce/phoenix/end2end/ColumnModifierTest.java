package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ColumnModifierTest extends BaseHBaseManagedTimeTest {
	
	@Test
	public void testNoOder() throws Exception {
		String ddl = "CREATE TABLE IF NOT EXISTS testColumnSortOrder (pk VARCHAR NOT NULL PRIMARY KEY)";		
		runTest(ddl, "pk", Lists.<Object>newArrayList("a", "b", "c"), Lists.<Object>newArrayList("a", "b", "c"));
	}
	
	@Test
	public void testAscOder() throws Exception {
		String ddl = "CREATE TABLE IF NOT EXISTS testColumnSortOrder (pk VARCHAR NOT NULL PRIMARY KEY ASC)";		
		runTest(ddl, "pk", Lists.<Object>newArrayList("a", "b", "c"), Lists.<Object>newArrayList("a", "b", "c"));
	}	

	@Test
	public void testDescOrder() throws Exception {
		String ddl = "CREATE TABLE IF NOT EXISTS testColumnSortOrder (pk VARCHAR NOT NULL PRIMARY KEY DESC)";		
		runTest(ddl, "pk", Lists.<Object>newArrayList("a", "b", "c"), Lists.<Object>newArrayList("c", "b", "a"));
	}
	
	@Test
	public void testWhere() throws Exception {
		String ddl = "CREATE TABLE IF NOT EXISTS testColumnSortOrder (pk VARCHAR NOT NULL PRIMARY KEY DESC)";		
		runQueryTest(ddl, "pk", Lists.<Object>newArrayList("a", "b", "c"), Lists.<Object>newArrayList("b"), " WHERE pk = 'b'");
	}

	private void runTest(String ddl, String columnName, List<Object> values, List<Object> expectedValues) throws Exception {
		runQueryTest(ddl, columnName, values, expectedValues, null);
	}
	
	private void runQueryTest(String ddl, String columnName, List<Object> values, List<Object> expectedValues, String optionalWhereClause) throws Exception {
		
		Properties props = new Properties(TEST_PROPERTIES);
		Connection conn = DriverManager.getConnection(getUrl(), props);
		
		try {
			
			conn.setAutoCommit(false);
	
			createTestTable(getUrl(), ddl);		
			
			String dml = "UPSERT INTO testColumnSortOrder(" + columnName + ") VALUES(?)";
			PreparedStatement stmt = conn.prepareStatement(dml);
			
			for (int i = 0; i < values.size(); i++) {				
				stmt.setObject(1, values.get(i));
				stmt.execute();			
			}
			conn.commit();
			
			String query = "SELECT " + columnName + " FROM testColumnSortOrder";
			if (optionalWhereClause != null) {
				query += " " + optionalWhereClause;
			}
			
			stmt = conn.prepareStatement(query);
			
			List<Object> results = Lists.newArrayListWithExpectedSize(values.size());
			
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				results.add(rs.getObject(columnName));
			}		
	
			Assert.assertEquals(expectedValues, results);
			
		} finally {
			conn.close();
		}
	}
}
