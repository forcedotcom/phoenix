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
package com.salesforce.hbase.index.write;

import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.salesforce.hbase.index.StubAbortable;
import com.salesforce.hbase.index.TableName;
import com.salesforce.hbase.index.table.HTableInterfaceReference;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;

public class TestParalleIndexWriter {

  private static final Log LOG = LogFactory.getLog(TestParalleIndexWriter.class);
  @Rule
  public TableName test = new TableName();
  private final byte[] row = Bytes.toBytes("row");

  @Test
  public void testCorrectlyCleansUpResources() throws Exception{
    ExecutorService exec = Executors.newFixedThreadPool(1);
    FakeTableFactory factory = new FakeTableFactory(
        Collections.<ImmutableBytesPtr, HTableInterface> emptyMap());
    ParallelWriterIndexCommitter writer = new ParallelWriterIndexCommitter();
    Abortable mockAbort = Mockito.mock(Abortable.class);
    Stoppable mockStop = Mockito.mock(Stoppable.class);
    // create a simple writer
    writer.setup(factory, exec, mockAbort, mockStop);
    // stop the writer
    writer.stop(this.test.getTableNameString() + " finished");
    assertTrue("Factory didn't get shutdown after writer#stop!", factory.shutdown);
    assertTrue("ExectorService isn't terminated after writer#stop!", exec.isShutdown());
    Mockito.verifyZeroInteractions(mockAbort, mockStop);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSynchronouslyCompletesAllWrites() throws Exception {
    LOG.info("Starting " + test.getTableNameString());
    LOG.info("Current thread is interrupted: " + Thread.interrupted());
    Abortable abort = new StubAbortable();
    Stoppable stop = Mockito.mock(Stoppable.class);
    ExecutorService exec = Executors.newFixedThreadPool(1);
    Map<ImmutableBytesPtr, HTableInterface> tables =
        new HashMap<ImmutableBytesPtr, HTableInterface>();
    FakeTableFactory factory = new FakeTableFactory(tables);

    ImmutableBytesPtr tableName = new ImmutableBytesPtr(this.test.getTableName());
    Put m = new Put(row);
    m.add(Bytes.toBytes("family"), Bytes.toBytes("qual"), null);
    Multimap<HTableInterfaceReference, Mutation> indexUpdates =
        ArrayListMultimap.<HTableInterfaceReference, Mutation> create();
    indexUpdates.put(new HTableInterfaceReference(tableName), m);

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
    Mockito.when(table.getTableName()).thenReturn(test.getTableName());
    // add the table to the set of tables, so its returned to the writer
    tables.put(tableName, table);

    // setup the writer and failure policy
    ParallelWriterIndexCommitter writer = new ParallelWriterIndexCommitter();
    writer.setup(factory, exec, abort, stop);
    writer.write(indexUpdates);
    assertTrue("Writer returned before the table batch completed! Likely a race condition tripped",
      completed[0]);
    writer.stop(this.test.getTableNameString() + " finished");
    assertTrue("Factory didn't get shutdown after writer#stop!", factory.shutdown);
    assertTrue("ExectorService isn't terminated after writer#stop!", exec.isShutdown());
  }
}
