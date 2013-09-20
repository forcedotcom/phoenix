package com.salesforce.phoenix.end2end;

import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.util.PhoenixRuntime;

public class CreateSequenceTest extends BaseClientMangedTimeTest {

	@Test
	public void testCreateTable() throws Exception {
		long ts = nextTimestamp();
		Properties props = new Properties();
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
		Connection conn = DriverManager.getConnection(getUrl(), props);
		conn.createStatement().execute(
				"CREATE SEQUENCE foo.bar\n" + "START WITH 2\n"
						+ "INCREMENT BY 4\n");
		ts = nextTimestamp();
		props = new Properties();
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
		conn = DriverManager.getConnection(getUrl(), props);
		String query = "SELECT sequence_schema, sequence_name, current_value, increment_by FROM SYSTEM.\"SEQUENCE\"";
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals("FOO", rs.getString("sequence_schema"));
		assertEquals("BAR", rs.getString("sequence_name"));
		assertEquals(2, rs.getInt("current_value"));
		assertEquals(4, rs.getInt("increment_by"));
		assertFalse(rs.next());

		ts = nextTimestamp();
		props = new Properties();
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
		conn = DriverManager.getConnection(getUrl(), props);
		query = "SELECT NEXT VALUE FOR foo.bar, sequence_name FROM SYSTEM.\"SEQUENCE\"";
		rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals(2, rs.getInt(1));	
		
		ts = nextTimestamp();
        props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        query = "SELECT NEXT VALUE FOR foo.bar, sequence_name FROM SYSTEM.\"SEQUENCE\"";
        rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(6, rs.getInt(1));
        
        ts = nextTimestamp();
        props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        query = "SELECT NEXT VALUE FOR foo.bar, sequence_name FROM SYSTEM.\"SEQUENCE\"";
        rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));
	}
}