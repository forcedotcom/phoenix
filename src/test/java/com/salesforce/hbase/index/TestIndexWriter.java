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
package com.salesforce.hbase.index;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.salesforce.hbase.index.table.HTableFactory;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;

public class TestIndexWriter {
  private static final Log LOG = LogFactory.getLog(TestIndexWriter.class);
  @Rule
  public TableName testName = new TableName();

  /**
   * Simple table factory that just looks up the tables based on name. Useful for mocking up
   * {@link HTableInterface}s without having to mock up the factory too.
   */
  private class FakeTableFactory implements HTableFactory {

    boolean shutdown = false;
    private Map<ImmutableBytesPtr, HTableInterface> tables;

    public FakeTableFactory(Map<ImmutableBytesPtr, HTableInterface> tables) {
      this.tables = tables;
    }

    @Override
    public HTableInterface getTable(ImmutableBytesPtr tablename) throws IOException {
      return this.tables.get(tablename);
    }

    @Override
    public void shutdown() {
      shutdown = true;
    }
  }

  private final byte[] row = Bytes.toBytes("row");

  /**
   * Test that we correctly shutdown/cleanup all the resources the writer creates
   * @throws Exception on failure
   */
  @Test
  public void correctlyCleanupResources() throws Exception {
    Abortable abort = new StubAbortable();
    ExecutorService exec = Executors.newFixedThreadPool(1);
    FakeTableFactory factory = new FakeTableFactory(
        Collections.<ImmutableBytesPtr, HTableInterface> emptyMap());

    // create a simple writer
    IndexWriter writer = new IndexWriter(this.testName.getTableNameString(), abort, exec, factory);
    writer.stop(this.testName.getTableNameString() + " finished");
    assertTrue("Factory didn't get shutdown after writer#stop!", factory.shutdown);
    assertTrue("ExectorService isn't terminated after writer#stop!", exec.isShutdown());
  }

  /**
   * With the move to using a pool of threads to write, we need to ensure that we still block until
   * all index writes for a mutation/batch are completed.
   * @throws Exception on failure
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testSynchronouslyCompletesAllWrites() throws Exception {
    LOG.info("Starting " + testName.getTableNameString());
    LOG.info("Current thread is interrupted: " + Thread.interrupted());
    Abortable abort = new StubAbortable();
    ExecutorService exec = Executors.newFixedThreadPool(1);
    Map<ImmutableBytesPtr, HTableInterface> tables = new HashMap<ImmutableBytesPtr, HTableInterface>();
    FakeTableFactory factory = new FakeTableFactory(tables);

    byte[] tableName = this.testName.getTableName();
    Put m = new Put(row);
    m.add(Bytes.toBytes("family"), Bytes.toBytes("qual"), null);
    Collection<Pair<Mutation, byte[]>> indexUpdates = Arrays.asList(new Pair<Mutation, byte[]>(m,
        tableName));

    HTableInterface table = Mockito.mock(HTableInterface.class);
    final boolean[] completed = new boolean[] { false };
    Mockito.when(table.batch(Mockito.anyList())).thenAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        // just keep track that it was called
        completed[0] = true;
        return null;
      }
    });
    Mockito.when(table.getTableName()).thenReturn(testName.getTableName());
    // add the table to the set of tables, so its returned to the writer
    tables.put(new ImmutableBytesPtr(tableName), table);

    IndexWriter writer = new IndexWriter(this.testName.getTableNameString(), abort, exec, factory);
    writer.write(indexUpdates);
    assertTrue("Writer returned before the table batch completed! Likely a race condition tripped",
      completed[0]);
    writer.stop(this.testName.getTableNameString() + " finished");
    assertTrue("Factory didn't get shutdown after writer#stop!", factory.shutdown);
    assertTrue("ExectorService isn't terminated after writer#stop!", exec.isShutdown());
  }

  /**
   * Index updates can potentially be queued up if there aren't enough writer threads. If a running
   * index write fails, then we should early exit the pending indexupdate, when it comes up (if the
   * pool isn't already shutdown).
   * <p>
   * This test is a little bit racey - we could actually have the failure of the first task before
   * the third task is even submitted. However, we should never see the third task attempt to make
   * the batch write, so we should never see a failure here.
   * @throws Exception on failure
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testFailureOnRunningUpdateAbortsPending() throws Exception {
    Abortable abort = new StubAbortable();
    // single thread factory so the older request gets queued
    ExecutorService exec = Executors.newFixedThreadPool(1);
    Map<ImmutableBytesPtr, HTableInterface> tables = new HashMap<ImmutableBytesPtr, HTableInterface>();
    FakeTableFactory factory = new FakeTableFactory(tables);

    // updates to two different tables
    byte[] tableName = Bytes.add(this.testName.getTableName(), new byte[] { 1, 2, 3, 4 });
    Put m = new Put(row);
    m.add(Bytes.toBytes("family"), Bytes.toBytes("qual"), null);
    byte[] tableName2 = this.testName.getTableName();// this will sort after the first tablename
    List<Pair<Mutation, byte[]>> indexUpdates = new ArrayList<Pair<Mutation, byte[]>>();
    indexUpdates.add(new Pair<Mutation, byte[]>(m, tableName));
    indexUpdates.add(new Pair<Mutation, byte[]>(m, tableName2));
    indexUpdates.add(new Pair<Mutation, byte[]>(m, tableName2));

    // first table will fail
    HTableInterface table = Mockito.mock(HTableInterface.class);
    Mockito.when(table.batch(Mockito.anyList())).thenThrow(
      new IOException("Intentional IOException for failed first write."));
    Mockito.when(table.getTableName()).thenReturn(tableName);

    // second table just blocks to make sure that the abort propagates to the third task
    final CountDownLatch waitOnAbortedLatch = new CountDownLatch(1);
    final boolean[] failed = new boolean[] { false };
    HTableInterface table2 = Mockito.mock(HTableInterface.class);
    Mockito.when(table2.getTableName()).thenReturn(tableName2);
    Mockito.when(table2.batch(Mockito.anyList())).thenAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        waitOnAbortedLatch.await();
        return null;
      }
    }).thenAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        failed[0] = true;
        throw new RuntimeException(
            "Unexpected exception - second index table shouldn't have been written to");
      }
    });

    // add the tables to the set of tables, so its returned to the writer
    tables.put(new ImmutableBytesPtr(tableName), table);
    tables.put(new ImmutableBytesPtr(tableName2), table2);

    IndexWriter writer = new IndexWriter(this.testName.getTableNameString(), abort, exec, factory);
    try {
      writer.write(indexUpdates);
      fail("Should not have successfully completed all index writes");
    } catch (CannotReachIndexException e) {
      LOG.info("Correctly got a failure to reach the index", e);
      // should have correctly gotten the correct abort, so let the next task execute
      waitOnAbortedLatch.countDown();
    }
    assertFalse(
      "Third set of index writes never have been attempted - should have seen the abort before done!",
      failed[0]);
    writer.stop(this.testName.getTableNameString() + " finished");
    assertTrue("Factory didn't get shutdown after writer#stop!", factory.shutdown);
    assertTrue("ExectorService isn't terminated after writer#stop!", exec.isShutdown());
  }

  /**
   * Test that if we get an interruption to to the thread while doing a batch (e.g. via shutdown),
   * that we correctly end the task
   * @throws Exception on failure
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testShutdownInterruptsAsExpected() throws Exception {

    Abortable abort = new StubAbortable();
    // single thread factory so the older request gets queued
    ExecutorService exec = Executors.newFixedThreadPool(1);
    Map<ImmutableBytesPtr, HTableInterface> tables = new HashMap<ImmutableBytesPtr, HTableInterface>();
    FakeTableFactory factory = new FakeTableFactory(tables);

    byte[] tableName = this.testName.getTableName();
    HTableInterface table = Mockito.mock(HTableInterface.class);
    Mockito.when(table.getTableName()).thenReturn(tableName);
    final CountDownLatch writeStartedLatch = new CountDownLatch(1);
    final CountDownLatch waitOnAbortedLatch = new CountDownLatch(1);
    Mockito.when(table.batch(Mockito.anyList())).thenAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        writeStartedLatch.countDown();
        // when we interrupt the thread for shutdown, we should see this throw an interrupt too
        waitOnAbortedLatch.await();
        return null;
      }
    });
    // add the tables to the set of tables, so its returned to the writer
    tables.put(new ImmutableBytesPtr(tableName), table);

    // update a single table
    Put m = new Put(row);
    m.add(Bytes.toBytes("family"), Bytes.toBytes("qual"), null);
    final List<Pair<Mutation, byte[]>> indexUpdates = new ArrayList<Pair<Mutation, byte[]>>();
    indexUpdates.add(new Pair<Mutation, byte[]>(m, tableName));

    // setup the writer
    final IndexWriter writer = new IndexWriter(this.testName.getTableNameString(), abort, exec,
        factory);

    final boolean[] failedWrite = new boolean[] { false };
    Thread primaryWriter = new Thread() {

      @Override
      public void run() {
        try {
          writer.write(indexUpdates);
        } catch (CannotReachIndexException e) {
          failedWrite[0] = true;
        }
      }
    };
    primaryWriter.start();
    // wait for the write to start before intentionally shutdown the pool
    writeStartedLatch.await();
    writer.stop("Shutting down writer for test " + this.testName.getTableNameString());
    primaryWriter.join();
    assertTrue("Writer should have failed because of the stop we issued", failedWrite[0]);
  }
}