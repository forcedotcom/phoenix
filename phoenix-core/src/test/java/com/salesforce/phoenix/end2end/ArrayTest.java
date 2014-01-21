/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *     Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *     Neither the name of Salesforce.com nor the names of its contributors may 
 *     be used to endorse or promote products derived from this software without 
 *     specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.B_VALUE;
import static com.salesforce.phoenix.util.TestUtil.PHOENIX_JDBC_URL;
import static com.salesforce.phoenix.util.TestUtil.ROW1;
import static com.salesforce.phoenix.util.TestUtil.TABLE_WITH_ARRAY;
import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.primitives.Floats;
import com.salesforce.phoenix.query.BaseTest;
import com.salesforce.phoenix.schema.PhoenixArray;
import com.salesforce.phoenix.util.PhoenixRuntime;

public class ArrayTest extends BaseClientManagedTimeTest {

	private static final String SIMPLE_TABLE_WITH_ARRAY = "SIMPLE_TABLE_WITH_ARRAY";

	@Test
	public void testScanByArrayValue() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT a_double_array, /* comment ok? */ b_string, a_float FROM table_with_array WHERE ?=organization_id and ?=a_float";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			statement.setString(1, tenantId);
			statement.setFloat(2, 0.01f);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			// Need to support primitive
			Double[] doubleArr = new Double[4];
			doubleArr[0] = 25.343;
			doubleArr[1] = 36.763;
		    doubleArr[2] = 37.56;
            doubleArr[3] = 386.63;
			Array array = conn.createArrayOf("DOUBLE",
					doubleArr);
			PhoenixArray resultArray = (PhoenixArray) rs.getArray(1);
			assertEquals(resultArray, array);
			assertEquals(rs.getString("B_string"), B_VALUE);
			assertTrue(Floats.compare(rs.getFloat(3), 0.01f) == 0);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

	@Test
	public void testScanWithArrayInWhereClause() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT a_double_array, /* comment ok? */ b_string, a_float FROM table_with_array WHERE ?=organization_id and ?=a_byte_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			statement.setString(1, tenantId);
			// Need to support primitive
			Byte[] byteArr = new Byte[2];
			byteArr[0] = 25;
			byteArr[1] = 36;
			Array array = conn.createArrayOf("TINYINT", byteArr);
			statement.setArray(2, array);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			// Need to support primitive
			Double[] doubleArr = new Double[4];
			doubleArr[0] = 25.343;
			doubleArr[1] = 36.763;
		    doubleArr[2] = 37.56;
            doubleArr[3] = 386.63;
			array = conn.createArrayOf("DOUBLE", doubleArr);
			Array resultArray = rs.getArray(1);
			assertEquals(resultArray, array);
			assertEquals(rs.getString("B_string"), B_VALUE);
			assertTrue(Floats.compare(rs.getFloat(3), 0.01f) == 0);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

	@Test
	public void testScanWithNonFixedWidthArrayInWhereClause() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT a_double_array, /* comment ok? */ b_string, a_float FROM table_with_array WHERE ?=organization_id and ?=a_string_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			statement.setString(1, tenantId);
			// Need to support primitive
			String[] strArr = new String[4];
			strArr[0] = "ABC";
			strArr[1] = "CEDF";
			strArr[2] = "XYZWER";
			strArr[3] = "AB";
			Array array = conn.createArrayOf("VARCHAR", strArr);
			statement.setArray(2, array);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			// Need to support primitive
			Double[] doubleArr = new Double[4];
			doubleArr[0] = 25.343;
			doubleArr[1] = 36.763;
		    doubleArr[2] = 37.56;
            doubleArr[3] = 386.63;
			array = conn.createArrayOf("DOUBLE", doubleArr);
			Array resultArray = rs.getArray(1);
			assertEquals(resultArray, array);
			assertEquals(rs.getString("B_string"), B_VALUE);
			assertTrue(Floats.compare(rs.getFloat(3), 0.01f) == 0);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

	@Test
	public void testScanWithNonFixedWidthArrayInSelectClause() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT a_string_array FROM table_with_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			String[] strArr = new String[4];
			strArr[0] = "ABC";
			strArr[1] = "CEDF";
			strArr[2] = "XYZWER";
			strArr[3] = "AB";
			Array array = conn.createArrayOf("VARCHAR", strArr);
			PhoenixArray resultArray = (PhoenixArray) rs.getArray(1);
			assertEquals(resultArray, array);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

	@Test
	public void testSelectSpecificIndexOfAnArrayAsArrayFunction()
			throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT ARRAY_ELEM(a_double_array,1) FROM table_with_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			// Need to support primitive
			Double[] doubleArr = new Double[1];
			doubleArr[0] = 36.763;
			conn.createArrayOf("DOUBLE", doubleArr);
			Double result =  rs.getDouble(1);
			assertEquals(result, doubleArr[0]);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

	@Test
	public void testSelectSpecificIndexOfAnArray() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT a_double_array[2] FROM table_with_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			// Need to support primitive
			Double[] doubleArr = new Double[1];
			doubleArr[0] = 37.56;
			Double result =  rs.getDouble(1);
			assertEquals(result, doubleArr[0]);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

    @Test
    public void testCaseWithArray() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(BaseConnectedQueryTest.getUrl(),
                getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts, false);
        String query = "SELECT CASE WHEN A_INTEGER = 1 THEN a_double_array ELSE null END [2] FROM table_with_array";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // Need to support primitive
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 37.56;
            Double result =  rs.getDouble(1);
            assertEquals(result, doubleArr[0]);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
	@Test
	public void testUpsertValuesWithArray() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		String query = "upsert into table_with_array(ORGANIZATION_ID,ENTITY_ID,a_double_array) values('"
				+ tenantId + "','00A123122312312',ARRAY[2.0d,345.8d])";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts)); // Execute
																					// at
																					// timestamp
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			int executeUpdate = statement.executeUpdate();
			assertEquals(1, executeUpdate);
			conn.commit();
			statement.close();
			conn.close();
			// create another connection
			props = new Properties(TEST_PROPERTIES);
			props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
					Long.toString(ts + 2)); // Execute at timestamp 2
			conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
			query = "SELECT ARRAY_ELEM(a_double_array,1) FROM table_with_array";
			statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			// Need to support primitive
			Double[] doubleArr = new Double[1];
			doubleArr[0] = 345.8d;
			conn.createArrayOf("DOUBLE", doubleArr);
			Double result = rs.getDouble(1);
			assertEquals(result, doubleArr[0]);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}
	
	@Test
	public void testUpsertSelectWithSelectAsSubQuery1() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		//initTablesWithArrays(tenantId, null, ts, false);
		try {
			createSimpleTableWithArray(BaseConnectedQueryTest.getUrl(),
					getDefaultSplits(tenantId), null, ts - 2);
			initSimpleArrayTable(tenantId, null, ts, false);
			Properties props = new Properties(TEST_PROPERTIES);
			props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
					Long.toString(ts + 2)); // Execute at timestamp 2
			Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL,
					props);
			String query = "upsert into table_with_array(ORGANIZATION_ID,ENTITY_ID,a_double_array) "
					+ "SELECT organization_id, entity_id, a_double_array  FROM "
					+ SIMPLE_TABLE_WITH_ARRAY
					+ " WHERE a_double_array[1] = 89.96d";
			PreparedStatement statement = conn.prepareStatement(query);
			int executeUpdate = statement.executeUpdate();
			assertEquals(1, executeUpdate);
			conn.commit();
			statement.close();
			conn.close();
			// create another connection
			props = new Properties(TEST_PROPERTIES);
			props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
					Long.toString(ts + 4));
			conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
			query = "SELECT ARRAY_ELEM(a_double_array,1) FROM table_with_array";
			statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			// Need to support primitive
			Double[] doubleArr = new Double[1];
			doubleArr[0] = 89.96d;
			Double result = rs.getDouble(1);
			assertEquals(result, doubleArr[0]);
			assertFalse(rs.next());

		} finally {
		}
	}
	
    @Test
    public void testUpsertSelectWithSelectAsSubQuery2() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(BaseConnectedQueryTest.getUrl(),
                getDefaultSplits(tenantId), null, ts - 2);
        //initTablesWithArrays(tenantId, null, ts, false);
        try {
            createSimpleTableWithArray(BaseConnectedQueryTest.getUrl(),
                    getDefaultSplits(tenantId), null, ts - 2);
            initSimpleArrayTable(tenantId, null, ts, false);
            Properties props = new Properties(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                    Long.toString(ts + 2)); // Execute at timestamp 2
            Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL,
                    props);
            String query = "upsert into table_with_array(ORGANIZATION_ID,ENTITY_ID,a_double_array[3]) values('"
                + tenantId + "','00A123122312312',2.0d)";
            PreparedStatement statement = conn.prepareStatement(query);
            int executeUpdate = statement.executeUpdate();
            assertEquals(1, executeUpdate);
            conn.commit();
            statement.close();
            conn.close();
            // create another connection
            props = new Properties(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                    Long.toString(ts + 4));
            conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
            query = "SELECT ARRAY_ELEM(a_double_array,3) FROM table_with_array";
            statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // Need to support primitive
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 2.0d;
            conn.createArrayOf("DOUBLE", doubleArr);
            Double result = rs.getDouble(1);
            assertEquals(result, doubleArr[0]);

        } finally {
        }
    }

	@Test
	public void testSelectArrayUsingUpsertLikeSyntax() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT a_double_array FROM TABLE_WITH_ARRAY WHERE a_double_array = ARRAY [ 25.343d, 36.763d, 37.56d,386.63d]";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			Double[] doubleArr =  new Double[4];
            doubleArr[0] = 25.343;
            doubleArr[1] = 36.763;
            doubleArr[2] = 37.56;
            doubleArr[3] = 386.63;
			Array array = conn.createArrayOf("DOUBLE", doubleArr);
			PhoenixArray resultArray = (PhoenixArray) rs.getArray(1);
			assertEquals(resultArray, array);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}
	
	@Test
	public void testArrayIndexUsedInWhereClause() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		int a_index = 0;
		String query = "SELECT a_double_array[1] FROM table_with_array where a_double_array["+a_index+"1]<?";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			Double[] doubleArr = new Double[1];
			doubleArr[0] = 40.0;
			conn.createArrayOf("DOUBLE", doubleArr);
			statement.setDouble(1, 40.0d);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			// Need to support primitive
			doubleArr = new Double[1];
			doubleArr[0] = 36.763;
			Double result =  rs.getDouble(1);
			assertEquals(result, doubleArr[0]);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

	@Test
	public void testArrayIndexUsedInGroupByClause() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT a_double_array[1] FROM table_with_array  GROUP BY a_double_array[1]";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			Double[] doubleArr = new Double[1];
			doubleArr[0] = 40.0;
			conn.createArrayOf("DOUBLE", doubleArr);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			doubleArr = new Double[1];
			doubleArr[0] = 36.763;
			Double result =  rs.getDouble(1);
			assertEquals(result, doubleArr[0]);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}
	
	@Test
	public void testVariableLengthArrayWithNullValue() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, true);
		String query = "SELECT a_string_array[1] FROM table_with_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			String[] strArr = new String[1];
			strArr[0] = "XYZWER";
			String result = rs.getString(1);
			assertNull(result);
		} finally {
			conn.close();
		}
	}

	@Test
	public void testSelectSpecificIndexOfAVariableArray() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT a_string_array[2] FROM table_with_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			String[] strArr = new String[1];
			strArr[0] = "XYZWER";
			String result = rs.getString(1);
			assertEquals(result, strArr[0]);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

	@Test
	public void testWithOutOfRangeIndex() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT a_double_array[100] FROM table_with_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			Double[] doubleArr = new Double[1];
			doubleArr[0] = 36.763;
			Array array = conn.createArrayOf("DOUBLE", doubleArr);
			PhoenixArray resultArray = (PhoenixArray) rs.getArray(1);
			assertEquals(resultArray, array);
			Assert.fail("Should have failed");
		} catch (Exception e) {
			System.out.println("");
		} finally {
			conn.close();
		}
	}

	@Test
	public void testArrayLengthFunctionForVariableLength() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT ARRAY_LENGTH(a_string_array) FROM table_with_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());			
			int result = rs.getInt(1);
			assertEquals(result, 4);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}
	

	@Test
	public void testArrayLengthFunctionForFixedLength() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT ARRAY_LENGTH(a_double_array) FROM table_with_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());			
			int result = rs.getInt(1);
			assertEquals(result, 4);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}
	static void createTableWithArray(String url, byte[][] bs, Object object,
			long ts) throws SQLException {
		String ddlStmt = "create table "
				+ TABLE_WITH_ARRAY
				+ "   (organization_id char(15) not null, \n"
				+ "    entity_id char(15) not null,\n"
				+ "    a_string_array varchar(100) array[],\n"
				+ "    b_string varchar(100),\n"
				+ "    a_integer integer,\n"
				+ "    a_date date,\n"
				+ "    a_time time,\n"
				+ "    a_timestamp timestamp,\n"
				+ "    x_decimal decimal(31,10),\n"
				+ "    x_long_array bigint[],\n"
				+ "    x_integer integer,\n"
				+ "    a_byte_array tinyint array,\n"
				+ "    a_short smallint,\n"
				+ "    a_float float,\n"
				+ "    a_double_array double array[],\n"
				+ "    a_unsigned_float unsigned_float,\n"
				+ "    a_unsigned_double unsigned_double \n"
				+ "    CONSTRAINT pk PRIMARY KEY (organization_id, entity_id)\n"
				+ ")";
		BaseTest.createTestTable(url, ddlStmt, bs, ts);
	}
	
	static void createSimpleTableWithArray(String url, byte[][] bs, Object object,
			long ts) throws SQLException {
		String ddlStmt = "create table "
				+ SIMPLE_TABLE_WITH_ARRAY
				+ "   (organization_id char(15) not null, \n"
				+ "    entity_id char(15) not null,\n"
				+ "    a_double_array double array[],\n"
				+ "    CONSTRAINT pk PRIMARY KEY (organization_id, entity_id)\n"
				+ ")";
		BaseTest.createTestTable(url, ddlStmt, bs, ts);
	}
	
	protected static void initSimpleArrayTable(String tenantId, Date date, Long ts, boolean useNull) throws Exception {
   	 Properties props = new Properties();
        if (ts != null) {
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, ts.toString());
        }
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " +SIMPLE_TABLE_WITH_ARRAY+
                    "(" +
                    "    ORGANIZATION_ID, " +
                    "    ENTITY_ID, " +
                    "    a_double_array)" +
                    "VALUES (?, ?, ?)");
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW1);
            // Need to support primitive
            Double[] doubleArr =  new Double[2];
            doubleArr[0] = 64.87;
            doubleArr[1] = 89.96;
            Array array = conn.createArrayOf("DOUBLE", doubleArr);
            stmt.setArray(3, array);
            stmt.execute();
                
            conn.commit();
        } finally {
            conn.close();
        }
   }
}
