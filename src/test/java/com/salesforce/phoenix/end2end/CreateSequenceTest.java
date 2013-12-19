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

import com.salesforce.phoenix.schema.SequenceAlreadyExistsException;
import com.salesforce.phoenix.util.PhoenixRuntime;

public class CreateSequenceTest extends BaseClientMangedTimeTest {	

	@Test
	public void testSystemTable() throws Exception {		
		Connection conn = getConnectionNextTimestamp();
		String query = "SELECT sequence_schema, sequence_name, current_value, increment_by FROM SYSTEM.\"SEQUENCE\"";
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
	}

	@Test
	public void testDuplicateSequences() throws Exception {
		Connection conn = getConnectionNextTimestamp();		
		conn.createStatement().execute("CREATE SEQUENCE alpha.beta START WITH 2 INCREMENT BY 4\n");

		conn = getConnectionNextTimestamp();
		try {
			conn.createStatement().execute("CREATE SEQUENCE alpha.beta START WITH 2 INCREMENT BY 4\n");
			Assert.fail("Duplicate sequences");
		} catch (SequenceAlreadyExistsException e){

		}
	}

	@Test
	public void testCreateSequence() throws Exception {	
		Connection conn = getConnectionNextTimestamp();
		conn.createStatement().execute("CREATE SEQUENCE alpha.omega START WITH 2 INCREMENT BY 4");
		conn = getConnectionNextTimestamp();		
		String query = "SELECT sequence_schema, sequence_name, current_value, increment_by FROM SYSTEM.\"SEQUENCE\" WHERE sequence_name='OMEGA'";
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals("ALPHA", rs.getString("sequence_schema"));
		assertEquals("OMEGA", rs.getString("sequence_name"));
		assertEquals(2, rs.getInt("current_value"));
		assertEquals(4, rs.getInt("increment_by"));
		assertFalse(rs.next());
	}

	@Test
	public void testSelectNextValueFor() throws Exception {
		Connection conn = getConnectionNextTimestamp();
		conn.createStatement().execute(
				"CREATE SEQUENCE foo.bar\n" + "START WITH 3\n"
						+ "INCREMENT BY 2\n");
		conn = getConnectionNextTimestamp();
		String query = "SELECT NEXT VALUE FOR foo.bar FROM SYSTEM.\"SEQUENCE\"";
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals(3, rs.getInt(1));

		conn = getConnectionNextTimestamp();
		query = "SELECT NEXT VALUE FOR foo.bar FROM SYSTEM.\"SEQUENCE\"";
		rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals(5, rs.getInt(1));

		conn = getConnectionNextTimestamp();
		query = "SELECT NEXT VALUE FOR foo.bar FROM SYSTEM.\"SEQUENCE\"";
		rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals(7, rs.getInt(1));
	}

	@Test
	public void testInsertNextValueFor() throws Exception {
		Connection conn = getConnectionNextTimestamp();
		conn.createStatement().execute("CREATE TABLE test.sequence_number ( id INTEGER NOT NULL PRIMARY KEY)");
		conn = getConnectionNextTimestamp();
		conn.createStatement().execute("CREATE SEQUENCE alpha.tau START WITH 2 INCREMENT BY 1");
		conn = getConnectionNextTimestamp();
		conn.createStatement().execute("UPSERT INTO test.sequence_number (id) VALUES (NEXT VALUE FOR alpha.tau)");
		conn.commit();
		conn = getConnectionNextTimestamp();
		String query = "SELECT id FROM test.sequence_number";		
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals(2, rs.getInt(1));
	}

	private Connection getConnectionNextTimestamp() throws Exception {
		long ts = nextTimestamp();
		Properties props = new Properties();
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
		Connection conn = DriverManager.getConnection(getUrl(), props);
		return conn;
	}
}