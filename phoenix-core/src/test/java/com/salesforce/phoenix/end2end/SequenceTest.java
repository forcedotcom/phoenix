package com.salesforce.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.jdbc.PhoenixStatement;
import com.salesforce.phoenix.parse.TableName;
import com.salesforce.phoenix.schema.SequenceAlreadyExistsException;
import com.salesforce.phoenix.schema.SequenceNotFoundException;
import com.salesforce.phoenix.util.TestUtil;

public class SequenceTest extends BaseHBaseManagedTimeTest {	

	@Test
	public void testSystemTable() throws Exception {		
		Connection conn = getConnection();
		String query = "SELECT sequence_schema, sequence_name, current_value, increment_by FROM SYSTEM.\"SEQUENCE\"";
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
	}

	@Test
	public void testDuplicateSequences() throws Exception {
		Connection conn = getConnection();		
		conn.createStatement().execute("CREATE SEQUENCE alpha.beta START WITH 2 INCREMENT BY 4\n");

		try {
			conn.createStatement().execute("CREATE SEQUENCE alpha.beta START WITH 2 INCREMENT BY 4\n");
			Assert.fail("Duplicate sequences");
		} catch (SequenceAlreadyExistsException e){

		}
	}

	@Test
	public void testSequenceNotFound() throws Exception {
		Connection conn = getConnection();
		String query = "SELECT NEXT value FOR qwert.asdf FROM SYSTEM.\"SEQUENCE\"";
		try {
			conn.prepareStatement(query).executeQuery();
			Assert.fail("Sequence not found");
		}catch(SequenceNotFoundException e){

		}
	}

	@Test
	public void testCreateSequence() throws Exception {	
		Connection conn = getConnection();
		conn.createStatement().execute("CREATE SEQUENCE alpha.omega START WITH 2 INCREMENT BY 4");
		String query = "SELECT sequence_schema, sequence_name, current_value, increment_by FROM SYSTEM.\"SEQUENCE\" WHERE sequence_name='OMEGA'";
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals("ALPHA", rs.getString("sequence_schema"));
		assertEquals("OMEGA", rs.getString("sequence_name"));
		assertEquals(2-4, rs.getInt("current_value"));
		assertEquals(-4, rs.getInt("increment_by"));
		assertFalse(rs.next());
	}

	@Test
	public void testSelectNextValueFor() throws Exception {
		Connection conn = getConnection();
		conn.createStatement().execute("CREATE SEQUENCE foo.bar START WITH 3 INCREMENT BY 2");
		String query = "SELECT NEXT VALUE FOR foo.bar FROM SYSTEM.\"SEQUENCE\"";
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals(3, rs.getInt(1));

		rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals(5, rs.getInt(1));

		rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals(7, rs.getInt(1));
	}

	@Test
	public void testInsertNextValueFor() throws Exception {
		Connection conn = getConnection();
		conn.createStatement().execute("CREATE TABLE test.sequence_number ( id INTEGER NOT NULL PRIMARY KEY)");
		conn.createStatement().execute("CREATE SEQUENCE alpha.tau START WITH 2 INCREMENT BY 1");
		conn.createStatement().execute("UPSERT INTO test.sequence_number (id) VALUES (NEXT VALUE FOR alpha.tau)");
        conn.createStatement().execute("UPSERT INTO test.sequence_number (id) VALUES (NEXT VALUE FOR alpha.tau)");
		conn.commit();
		String query = "SELECT id FROM test.sequence_number";		
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
	}

	@Test
	public void testSequenceCaching() throws Exception {		
		Connection conn = getConnection();
		conn.createStatement().execute("CREATE SEQUENCE alpha.gamma START WITH 2 INCREMENT BY 1");
		TableName tableName = TableName.createNormalized("ALPHA", "GAMMA");
		Long value = conn.unwrap(PhoenixConnection.class).getPMetaData().getSequenceIncrementValue(tableName);
		assertEquals(Long.valueOf(1), value);
		final String query = "SELECT NEXT VALUE FOR alpha.gamma FROM SYSTEM.\"SEQUENCE\"";
		conn.prepareStatement(query).executeQuery();
		value = conn.unwrap(PhoenixConnection.class).getPMetaData().getSequenceIncrementValue(tableName);
        assertEquals(Long.valueOf(1), value);
	}

	@Test
	public void testMultipleSequenceValues() throws Exception {
		Connection conn = getConnection();
		conn.createStatement().execute("CREATE SEQUENCE alpha.zeta START WITH 4 INCREMENT BY 7");
		conn.createStatement().execute("CREATE SEQUENCE alpha.kappa START WITH 9 INCREMENT BY 2");
		String query = "SELECT NEXT VALUE FOR alpha.zeta, NEXT VALUE FOR alpha.kappa FROM SYSTEM.\"SEQUENCE\"";
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals(4, rs.getInt(1));
		assertEquals(9, rs.getInt(2));
	}
	
	@Test
	public void testCompilerOptimization() throws Exception {
		Connection conn = getConnection();
		conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1) INCLUDE (v2)");
        conn.createStatement().execute("CREATE SEQUENCE seq.perf START WITH 3 INCREMENT BY 2");        
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        stmt.optimizeQuery("SELECT k, NEXT VALUE FOR seq.perf FROM t WHERE v1 = 'bar'");
	}
	
	@Test
	public void testSelectRowAndSequence() throws Exception {
		Connection conn = getConnection();
		conn.createStatement().execute("CREATE TABLE test.foo ( id INTEGER NOT NULL PRIMARY KEY)");
		conn.createStatement().execute("CREATE SEQUENCE alpha.epsilon START WITH 1 INCREMENT BY 4");
		conn.createStatement().execute("UPSERT INTO test.foo (id) VALUES (NEXT VALUE FOR alpha.epsilon)");
		conn.commit();
		String query = "SELECT NEXT VALUE FOR alpha.epsilon, id FROM test.foo";
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals(5, rs.getInt(1));
		assertEquals(1, rs.getInt(2));
        assertFalse(rs.next());
	}

	private Connection getConnection() throws Exception {
		Properties props = new Properties(TestUtil.TEST_PROPERTIES);
		Connection conn = DriverManager.getConnection(getUrl(), props);
		return conn;
	}	
}