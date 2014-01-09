package com.salesforce.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.jdbc.PhoenixStatement;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.schema.SequenceAlreadyExistsException;
import com.salesforce.phoenix.schema.SequenceNotFoundException;
import com.salesforce.phoenix.util.ReadOnlyProps;
import com.salesforce.phoenix.util.TestUtil;

public class SequenceTest extends BaseHBaseManagedTimeTest {
    private static final long BATCH_SIZE = 3;
    
    @BeforeClass 
    public static void doSetup() throws Exception {
        
        Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
        // Make a small batch size to test multiple calls to reserve sequences
        props.put(QueryServices.SEQUENCE_CACHE_SIZE_ATTRIB, Long.toString(BATCH_SIZE));
        // Must update config before starting server
        startServer(getUrl(), new ReadOnlyProps(props.entrySet().iterator()));
    }
    

	@Test
	public void testSystemTable() throws Exception {		
		Connection conn = getConnection();
		String query = "SELECT sequence_schema, sequence_name, current_value, increment_by FROM SYSTEM.\"SEQUENCE\"";
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertFalse(rs.next());
	}

	@Test
	public void testDuplicateSequences() throws Exception {
		Connection conn = getConnection();		
		conn.createStatement().execute("CREATE SEQUENCE alpha.beta START WITH 2 INCREMENT BY 4\n");
		Thread.sleep(1); // Seems that in mini cluster, this executes too quickly and flaps

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
			fail("Sequence not found");
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
		assertEquals(2, rs.getInt("current_value"));
		assertEquals(4, rs.getInt("increment_by"));
		assertFalse(rs.next());
	}
		
    @Test
    public void testCurrentValueFor() throws Exception {
        ResultSet rs;
        Connection conn = getConnection();
        conn.createStatement().execute("CREATE SEQUENCE used.nowhere START WITH 2 INCREMENT BY 4");
        try {
            rs = conn.createStatement().executeQuery("SELECT CURRENT VALUE FOR used.nowhere FROM SYSTEM.\"SEQUENCE\"");
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_CALL_CURRENT_BEFORE_NEXT_VALUE.getErrorCode(), e.getErrorCode());
        }
        
        rs = conn.createStatement().executeQuery("SELECT NEXT VALUE FOR used.nowhere FROM SYSTEM.\"SEQUENCE\"");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        rs = conn.createStatement().executeQuery("SELECT CURRENT VALUE FOR used.nowhere FROM SYSTEM.\"SEQUENCE\"");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
	}

    @Test
    public void testDropSequence() throws Exception { 
        Connection conn = getConnection();
        conn.createStatement().execute("CREATE SEQUENCE alpha.omega START WITH 2 INCREMENT BY 4");
        String query = "SELECT sequence_schema, sequence_name, current_value, increment_by FROM SYSTEM.\"SEQUENCE\" WHERE sequence_name='OMEGA'";
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals("ALPHA", rs.getString("sequence_schema"));
        assertEquals("OMEGA", rs.getString("sequence_name"));
        assertEquals(2, rs.getInt("current_value"));
        assertEquals(4, rs.getInt("increment_by"));
        assertFalse(rs.next());

        conn.createStatement().execute("DROP SEQUENCE alpha.omega");
        query = "SELECT sequence_schema, sequence_name, current_value, increment_by FROM SYSTEM.\"SEQUENCE\" WHERE sequence_name='OMEGA'";
        rs = conn.prepareStatement(query).executeQuery();
        assertFalse(rs.next());

        try {
            conn.createStatement().execute("DROP SEQUENCE alpha.omega");
            fail();
        } catch (SequenceNotFoundException ignore) {
        }
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
	public void testSequenceCreation() throws Exception {		
		Connection conn = getConnection();
		conn.createStatement().execute("CREATE SEQUENCE alpha.gamma START WITH 2 INCREMENT BY 3 CACHE 5");
        ResultSet rs = conn.createStatement().executeQuery("SELECT start_with, increment_by, cache_size, sequence_schema, sequence_name FROM SYSTEM.\"SEQUENCE\"");
        assertTrue(rs.next());
        assertEquals(2, rs.getLong(1));
        assertEquals(3, rs.getLong(2));
        assertEquals(5, rs.getLong(3));
        assertEquals("ALPHA", rs.getString(4));
        assertEquals("GAMMA", rs.getString(5));
        assertFalse(rs.next());
		rs = conn.createStatement().executeQuery("SELECT NEXT VALUE FOR alpha.gamma, CURRENT VALUE FOR alpha.gamma FROM SYSTEM.\"SEQUENCE\"");
        assertTrue(rs.next());
        assertEquals(2, rs.getLong(1));
        assertEquals(2, rs.getLong(2));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("SELECT CURRENT VALUE FOR alpha.gamma, NEXT VALUE FOR alpha.gamma FROM SYSTEM.\"SEQUENCE\"");
        assertTrue(rs.next());
        assertEquals(5, rs.getLong(1));
        assertEquals(5, rs.getLong(2));
        assertFalse(rs.next());
	}

    @Test
    public void testSameMultipleSequenceValues() throws Exception {
        Connection conn = getConnection();
        conn.createStatement().execute("CREATE SEQUENCE alpha.zeta START WITH 4 INCREMENT BY 7");
        String query = "SELECT NEXT VALUE FOR alpha.zeta, NEXT VALUE FOR alpha.zeta FROM SYSTEM.\"SEQUENCE\"";
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals(4, rs.getInt(2));
        assertFalse(rs.next());
        conn.close();
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
        assertTrue(rs.next());
        assertEquals(4+7, rs.getInt(1));
        assertEquals(9+2, rs.getInt(2));
        assertFalse(rs.next());
        conn.close();
        // Test that sequences don't have gaps (if no other client request the same sequence before we close it)
        conn = getConnection();
        rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(4+7*2, rs.getInt(1));
        assertEquals(9+2*2, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(4+7*3, rs.getInt(1));
        assertEquals(9+2*3, rs.getInt(2));
        assertFalse(rs.next());
        conn.close();
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
        conn.createStatement().execute("CREATE SEQUENCE alpha.epsilon START WITH 1 INCREMENT BY 4");
		conn.createStatement().execute("CREATE TABLE test.foo ( id INTEGER NOT NULL PRIMARY KEY)");
		conn.createStatement().execute("UPSERT INTO test.foo (id) VALUES (NEXT VALUE FOR alpha.epsilon)");
		conn.commit();
		String query = "SELECT NEXT VALUE FOR alpha.epsilon, id FROM test.foo";
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals(5, rs.getInt(1));
		assertEquals(1, rs.getInt(2));
        assertFalse(rs.next());
	}

    @Test
    public void testSelectNextValueForOverMultipleBatches() throws Exception {
        Connection conn = getConnection();
        conn.createStatement().execute("CREATE TABLE foo (k BIGINT NOT NULL PRIMARY KEY)");
        conn.createStatement().execute("CREATE SEQUENCE foo.bar");
        
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar)");
        for (int i = 0; i < BATCH_SIZE  * 2 + 1; i++) {
            stmt.execute();
        }
        conn.commit();
        ResultSet rs = conn.createStatement().executeQuery("SELECT count(*),max(k) FROM foo");
        assertTrue(rs.next());
        assertEquals(BATCH_SIZE * 2 + 1, rs.getInt(1));
        assertEquals(BATCH_SIZE * 2 + 1, rs.getInt(2));
    }

    @Test
    public void testSelectNextValueForGroupBy() throws Exception {
        Connection conn = getConnection();
        conn.createStatement().execute("CREATE TABLE foo (k BIGINT NOT NULL PRIMARY KEY, v VARCHAR)");
        conn.createStatement().execute("CREATE TABLE bar (k BIGINT NOT NULL PRIMARY KEY, v VARCHAR)");
        conn.createStatement().execute("CREATE SEQUENCE foo.bar");
        
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar, ?)");
        stmt.setString(1, "a");
        stmt.execute();
        stmt.setString(1, "a");
        stmt.execute();
        stmt.setString(1, "b");
        stmt.execute();
        stmt.setString(1, "b");
        stmt.execute();
        stmt.setString(1, "c");
        stmt.execute();
        conn.commit();
        
        conn.setAutoCommit(true);;
        conn.createStatement().execute("UPSERT INTO bar SELECT NEXT VALUE FOR foo.bar,v FROM foo GROUP BY v");
        ResultSet rs = conn.createStatement().executeQuery("SELECT * from bar");
        assertTrue(rs.next());
        assertEquals(6, rs.getInt(1));
        assertEquals("a", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(7, rs.getInt(1));
        assertEquals("b", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(8, rs.getInt(1));
        assertEquals("c", rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    public void testSelectNextValueForMultipleConn() throws Exception {
        Connection conn1 = getConnection();
        conn1.createStatement().execute("CREATE TABLE foo (k BIGINT NOT NULL PRIMARY KEY)");
        conn1.createStatement().execute("CREATE SEQUENCE foo.bar");
        
        PreparedStatement stmt1 = conn1.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar)");
        for (int i = 0; i < BATCH_SIZE+ 1; i++) {
            stmt1.execute();
        }
        conn1.commit();
        Connection conn2 = getConnection();
        PreparedStatement stmt2 = conn2.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar)");
        stmt2.execute();
        stmt1.close(); // Should still continue with next value, even on separate connection
        for (int i = 0; i < BATCH_SIZE; i++) {
            stmt2.execute();
        }
        conn2.commit();
        
        ResultSet rs = conn2.createStatement().executeQuery("SELECT k FROM foo");
        for (int i = 0; i < BATCH_SIZE+ 1; i++) {
            assertTrue(rs.next());
            assertEquals(i+1, rs.getInt(1));
        }
        // Gaps exist b/c sequences were generated from different connections
        for (int i = 0; i < BATCH_SIZE+ 1; i++) {
            assertTrue(rs.next());
            assertEquals(BATCH_SIZE+1+i+1, rs.getInt(1));
        }
        assertFalse(rs.next());
    }

    @Test
    public void testSelectNextValueForMultipleConnWithStmtClose() throws Exception {
        Connection conn1 = getConnection();
        // Create sequence before table, as with mini-cluster using a sequence right after
        // it's created seems to cause the sequence not to be found.
        conn1.createStatement().execute("CREATE SEQUENCE foo.bar");
        conn1.createStatement().execute("CREATE TABLE foo (k BIGINT NOT NULL PRIMARY KEY)");
        
        PreparedStatement stmt1 = conn1.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar)");
        for (int i = 0; i < BATCH_SIZE+ 1; i++) {
            stmt1.execute();
        }
        conn1.commit();
        stmt1.close(); // will return unused sequences, so no gaps now
        
        Connection conn2 = getConnection();
        PreparedStatement stmt2 = conn2.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar)");
        for (int i = 0; i < BATCH_SIZE + 1; i++) {
            stmt2.execute();
        }
        conn2.commit();
        
        ResultSet rs = conn2.createStatement().executeQuery("SELECT k FROM foo");
        for (int i = 0; i < 2*(BATCH_SIZE + 1); i++) {
            assertTrue(rs.next());
            assertEquals(i+1, rs.getInt(1));
        }
        assertFalse(rs.next());
    }

    @Test
    public void testSelectNextValueForMultipleConnWithConnClose() throws Exception {
        Connection conn1 = getConnection();
        conn1.createStatement().execute("CREATE TABLE foo (k BIGINT NOT NULL PRIMARY KEY)");
        conn1.createStatement().execute("CREATE SEQUENCE foo.bar");
        
        PreparedStatement stmt1 = conn1.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar)");
        for (int i = 0; i < BATCH_SIZE+ 1; i++) {
            stmt1.execute();
        }
        conn1.commit();
        conn1.close(); // will return unused sequences, so no gaps now
        
        Connection conn2 = getConnection();
        PreparedStatement stmt2 = conn2.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar)");
        for (int i = 0; i < BATCH_SIZE + 1; i++) {
            stmt2.execute();
        }
        conn2.commit();
        
        ResultSet rs = conn2.createStatement().executeQuery("SELECT k FROM foo");
        for (int i = 0; i < 2*(BATCH_SIZE + 1); i++) {
            assertTrue(rs.next());
            assertEquals(i+1, rs.getInt(1));
        }
        assertFalse(rs.next());
    }

    @Test
    public void testDropCachedSeq1() throws Exception {
        testDropCachedSeq(false);
    }
    
    @Test
    public void testDropCachedSeq2() throws Exception {
        testDropCachedSeq(true);
    }
    
    private void testDropCachedSeq(boolean detectDeleteSeqInEval) throws Exception {
        Connection conn1 = getConnection();
        conn1.createStatement().execute("CREATE TABLE foo (k BIGINT NOT NULL PRIMARY KEY)");
        conn1.createStatement().execute("CREATE SEQUENCE foo.bar");
        conn1.createStatement().execute("CREATE SEQUENCE bar.bas START WITH 101");
        
        PreparedStatement stmt1a = conn1.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar)");
        stmt1a.execute();
        stmt1a.execute();
        PreparedStatement stmt1b = conn1.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR bar.bas)");
        stmt1b.execute();
        stmt1b.execute();
        stmt1b.execute();
        conn1.commit();
        
        Connection conn2 = getConnection();
        PreparedStatement stmt2 = conn2.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR bar.bas)");
        stmt2.execute();
        conn2.commit();
        
        ResultSet rs = conn2.createStatement().executeQuery("SELECT k FROM foo");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(101, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(102, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(103, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(104, rs.getInt(1));
        assertFalse(rs.next());
        
        conn2.createStatement().execute("DROP SEQUENCE bar.bas");

        stmt1a.execute();
        if (!detectDeleteSeqInEval) {
            stmt1a.execute(); // Will allocate new batch for foo.bar and get error for bar.bas, but ignore it
        }
        
        try {
            stmt1b.execute(); // Will try to get new batch, but fail b/c sequence has been dropped
            fail();
        } catch (SequenceNotFoundException e) {
        }
    }

	private Connection getConnection() throws Exception {
		Properties props = new Properties(TestUtil.TEST_PROPERTIES);
		Connection conn = DriverManager.getConnection(getUrl(), props);
		return conn;
	}	
}