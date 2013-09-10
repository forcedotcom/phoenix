/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.hbase.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.salesforce.hbase.index.builder.ColumnFamilyIndexer;

/**
 * Test secondary indexing from an end-to-end perspective (client to server to index table)
 */
public class TestEndtoEndIndexing {

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final byte[] FAM = Bytes.toBytes("FAMILY");
  private static final byte[] FAM2 = Bytes.toBytes("FAMILY2");
  private static final String INDEXED_TABLE = "INDEXED_TABLE";
  private static final String INDEX_TABLE = "INDEX_TABLE";

  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  /**
   * Ensure that even if we don't write to the WAL in the Put we at least <i>attempt</i> to index
   * the values in the Put
   * @throws Exception on failure
   */
  @Test
  public void testPutWithoutWALGetsIndexed() throws Exception {
    byte[] k = new byte[] { 'a', 'a', 'a' };
    Put put = new Put(k);
    put.add(FAM, null, k);
    put.add(FAM2, null, k);
    put.setWriteToWAL(false);
    doPrimaryTablePutWithExpectedIndex(put, 2);
  }

  /**
   * Test that a simple put into the primary table gets a corresponding put in the index table, in
   * non-failure situations.
   * @throws Exception on failure
   */
  @Test
  public void testSimplePrimaryAndIndexTables() throws Exception {
    byte[] k = new byte[] { 'a', 'a', 'a' };
    Put put = new Put(k);
    put.add(FAM, null, k);
    put.add(FAM2, null, k);
    doPrimaryTablePutWithExpectedIndex(put, 2);
  }

  /**
   * Test that we delete change also propagates from the primary table to the index table
   * @throws Exception on failure
   */
  @Test
  public void testPutAndDeleteIsIndexed() throws Exception {
    byte[] k = new byte[] { 'a', 'a', 'a' };
    // start with a put, so we know we have some data
    Put put = new Put(k);
    put.add(FAM, null, k);
    put.add(FAM2, null, k);

    // then do a delete of that same row, ending up with no edits in the index table
    Delete d = new Delete(k);
    // we need to do a full specification here so we in the indexer what to delete on the index
    // table
    d.deleteColumn(FAM, null);
    d.deleteColumn(FAM2, null);
    doPrimaryTableUpdatesWithExpectedIndex(Arrays.asList(put, d), 0);
  }

  private void doPrimaryTablePutWithExpectedIndex(Put m, int indexSize) throws Exception {
    doPrimaryTableUpdatesWithExpectedIndex(Collections.singletonList((Mutation) m), indexSize);
  }

  /**
   * Create a new primary and index table, write the put to the primary table and then scan the
   * index table to ensure that the {@link Put} made it.
   * @param put put to write to the primary table
   * @param indexSize expected size of the index after the operation
   * @throws Exception on failure
   */
  private void doPrimaryTableUpdatesWithExpectedIndex(List<Mutation> mutations, int indexSize)
      throws Exception {
    HTableDescriptor primary = new HTableDescriptor(INDEXED_TABLE);
    primary.addFamily(new HColumnDescriptor(FAM));
    primary.addFamily(new HColumnDescriptor(FAM2));
    // setup indexing on one table and one of its columns
    Map<byte[], String> indexMapping = new HashMap<byte[], String>();
    indexMapping.put(FAM, INDEX_TABLE);
    ColumnFamilyIndexer.enableIndexing(primary, indexMapping);

    // setup the stats table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    // create the primary table
    admin.createTable(primary);

    // create the index table
    ColumnFamilyIndexer.createIndexTable(admin, INDEX_TABLE);

    assertTrue("Target index table (" + INDEX_TABLE + ") didn't get created!",
      admin.tableExists(INDEX_TABLE));
    
    // load some data into our primary table
    HTable primaryTable = new HTable(UTIL.getConfiguration(), INDEXED_TABLE);
    primaryTable.setAutoFlush(false);
    primaryTable.batch(mutations);
    primaryTable.flushCommits();
		primaryTable.close();

    // and now scan the index table
    HTable index = new HTable(UTIL.getConfiguration(), INDEX_TABLE);
    int count = getKeyValueCount(index);

    // we should have 1 index values - one for each key in the FAM column family
    // but none in the FAM2 column family
    assertEquals("Got an unexpected amount of index entries!", indexSize, count);

    // then delete the table and make sure we don't have any more stats in our table
    admin.disableTable(primary.getName());
    admin.deleteTable(primary.getName());
    admin.disableTable(INDEX_TABLE);
    admin.deleteTable(INDEX_TABLE);
  }

  /**
   * Count the number of keyvalue in the table. Scans all possible versions
   * @param table table to scan
   * @return number of keyvalues over all rows in the table
   * @throws IOException
   */
  private int getKeyValueCount(HTable table) throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions(Integer.MAX_VALUE - 1);

    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      count += res.list().size();
      System.out.println(count + ") " + res);
    }
    results.close();

    return count;
  }
}
