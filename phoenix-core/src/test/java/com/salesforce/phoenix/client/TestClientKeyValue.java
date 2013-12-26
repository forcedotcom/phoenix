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
package com.salesforce.phoenix.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * TODO: once the only test is not ignored, make this class concrete again
 */
public abstract class TestClientKeyValue {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte[] ROW = Bytes.toBytes("testRow");
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Simple test that a {@link ClientKeyValue} works as expected on a real table
   * @throws Exception
   */
  @Test
  @Ignore("Only supported with HBase 0.94.14")
  public void testClientKeyValue() throws Exception {
    byte[] TABLE = Bytes.toBytes("testClientKeyValue");
    HTable table = TEST_UTIL.createTable(TABLE, new byte[][] { FAMILY });

    // create several rows
    Put p = new Put(ROW);
    byte[] v = Bytes.toBytes("v1");
    KeyValue kv = new ClientKeyValue(ROW, FAMILY, QUALIFIER, 10, Type.Put, v);
    p.add(kv);
    byte[] v2 = Bytes.toBytes("v2");
    kv = new ClientKeyValue(ROW, FAMILY, QUALIFIER, 11, Type.Put, v2);
    p.add(kv);
    byte[] v3 = Bytes.toBytes("v3");
    kv = new ClientKeyValue(ROW, FAMILY, QUALIFIER, 12, Type.Put, v3);
    p.add(kv);

    table.put(p);
    table.flushCommits();

    byte[][] values = new byte[][] { v, v2, v3 };
    long[] times = new long[] { 10, 11, 12 };
    scanAllVersionsAndVerify(table, ROW, FAMILY, QUALIFIER, times, values, 0, 2);

    // do a delete of the row as well
    Delete d = new Delete(ROW);
    // start with a point delete
    kv = new ClientKeyValue(ROW, FAMILY, QUALIFIER, 10, Type.Delete);
    d.addDeleteMarker(kv);
    table.delete(d);
    scanAllVersionsAndVerify(table, ROW, FAMILY, QUALIFIER, times, values, 1, 2);

    // delete just that column
    d = new Delete(ROW);
    kv = new ClientKeyValue(ROW, FAMILY, QUALIFIER, 11, Type.DeleteColumn);
    d.addDeleteMarker(kv);
    table.delete(d);
    scanAllVersionsAndVerify(table, ROW, FAMILY, QUALIFIER, times, values, 2, 2);

    // delete the whole family
    kv = new ClientKeyValue(ROW, FAMILY, QUALIFIER, 12, Type.DeleteFamily);
    d.addDeleteMarker(kv);
    table.delete(d);
    scanVersionAndVerifyMissing(table, ROW, FAMILY, QUALIFIER, 12);

    // cleanup
    table.close();
  }

  private void scanAllVersionsAndVerify(HTable ht, byte[] row, byte[] family, byte[] qualifier,
      long[] stamps, byte[][] values, int start, int end) throws IOException {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setMaxVersions(Integer.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertNResult(result, row, family, qualifier, stamps, values, start, end);
  }

  private void scanVersionAndVerifyMissing(HTable ht, byte[] row, byte[] family, byte[] qualifier,
      long stamp) throws Exception {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setTimeStamp(stamp);
    scan.setMaxVersions(Integer.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertNullResult(result);
  }

  private void assertNullResult(Result result) throws Exception {
    assertTrue("expected null result but received a non-null result", result == null);
  }

  private Result getSingleScanResult(HTable ht, Scan scan) throws IOException {
    ResultScanner scanner = ht.getScanner(scan);
    Result result = scanner.next();
    scanner.close();
    return result;
  }

  private void assertNResult(Result result, byte[] row, byte[] family, byte[] qualifier,
      long[] stamps, byte[][] values, int start, int end) throws IOException {
    assertTrue(
      "Expected row [" + Bytes.toString(row) + "] " + "Got row [" + Bytes.toString(result.getRow())
          + "]", equals(row, result.getRow()));
    int expectedResults = end - start + 1;
    assertEquals(expectedResults, result.size());

    KeyValue[] keys = result.raw();

    for (int i = 0; i < keys.length; i++) {
      byte[] value = values[end - i];
      long ts = stamps[end - i];
      KeyValue key = keys[i];

      assertTrue("(" + i + ") Expected family [" + Bytes.toString(family) + "] " + "Got family ["
          + Bytes.toString(key.getFamily()) + "]", equals(family, key.getFamily()));
      assertTrue("(" + i + ") Expected qualifier [" + Bytes.toString(qualifier) + "] "
          + "Got qualifier [" + Bytes.toString(key.getQualifier()) + "]",
        equals(qualifier, key.getQualifier()));
      assertTrue("Expected ts [" + ts + "] " + "Got ts [" + key.getTimestamp() + "]",
        ts == key.getTimestamp());
      assertTrue("(" + i + ") Expected value [" + Bytes.toString(value) + "] " + "Got value ["
          + Bytes.toString(key.getValue()) + "]", equals(value, key.getValue()));
    }
  }

  private boolean equals(byte[] left, byte[] right) {
    if (left == null && right == null) return true;
    if (left == null && right.length == 0) return true;
    if (right == null && left.length == 0) return true;
    return Bytes.equals(left, right);
  }
}