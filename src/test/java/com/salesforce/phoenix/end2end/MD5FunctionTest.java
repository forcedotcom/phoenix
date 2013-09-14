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

import static org.junit.Assert.*;

import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import org.junit.Test;

public class MD5FunctionTest extends BaseHBaseManagedTimeTest {
  
  @Test
  public void testRetrieve() throws Exception {
      String testString = "mwalsh";
      
      Connection conn = DriverManager.getConnection(getUrl());
      String ddl = "CREATE TABLE IF NOT EXISTS MD5_RETRIEVE_TEST (pk VARCHAR NOT NULL PRIMARY KEY)";
      conn.createStatement().execute(ddl);
      String dml = String.format("UPSERT INTO MD5_RETRIEVE_TEST VALUES('%s')", testString);
      conn.createStatement().execute(dml);
      conn.commit();
      
      ResultSet rs = conn.createStatement().executeQuery("SELECT MD5(pk) FROM MD5_RETRIEVE_TEST");
      assertTrue(rs.next());
      byte[] first = MessageDigest.getInstance("MD5").digest(testString.getBytes());
      byte[] second = rs.getBytes(1);
      assertArrayEquals(first, second);
      assertFalse(rs.next());
  }      
  
  @Test
  public void testUpsert() throws Exception {
      String testString = "mwalsh";
      
      Connection conn = DriverManager.getConnection(getUrl());
      String ddl = "CREATE TABLE IF NOT EXISTS MD5_UPSERT_TEST (pk binary(16) NOT NULL PRIMARY KEY)";
      conn.createStatement().execute(ddl);
      String dml = String.format("UPSERT INTO MD5_UPSERT_TEST VALUES(md5('%s'))", testString);
      conn.createStatement().execute(dml);
      conn.commit();
      
      ResultSet rs = conn.createStatement().executeQuery("SELECT pk FROM MD5_UPSERT_TEST");
      assertTrue(rs.next());
      byte[] first = MessageDigest.getInstance("MD5").digest(testString.getBytes());
      byte[] second = rs.getBytes(1);
      assertArrayEquals(first, second);
      assertFalse(rs.next());
  }                                                           

}
