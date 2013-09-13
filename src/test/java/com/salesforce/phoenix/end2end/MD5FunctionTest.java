package com.salesforce.phoenix.end2end;

import static org.junit.Assert.*;

import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import org.junit.Test;

public class MD5FunctionTest extends BaseHBaseManagedTimeTest {
  
  @Test
  public void testReverse() throws Exception {
      String testString = "mwalsh";
      
      Connection conn = DriverManager.getConnection(getUrl());
      String ddl = "CREATE TABLE IF NOT EXISTS MD5_TEST (pk VARCHAR NOT NULL PRIMARY KEY)";
      conn.createStatement().execute(ddl);
      String dml = String.format("UPSERT INTO MD5_TEST VALUES('%s')", testString);
      conn.createStatement().execute(dml);
      conn.commit();
      
      ResultSet rs;
      rs = conn.createStatement().executeQuery("SELECT MD5(pk) FROM MD5_TEST");
      assertTrue(rs.next());
      String first = new String(MessageDigest.getInstance("MD5").digest(testString.getBytes()));
      String second = rs.getString(1);
      assertEquals(first, second);
      assertFalse(rs.next());
  }                                                           

}
