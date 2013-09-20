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
package com.salesforce.hbase.index.table;

import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.hbase.index.util.ImmutableBytesPtr;

/**
 * A simple cache that just uses usual GC mechanisms to cleanup unused {@link HTableInterface}s.
 * When requesting an {@link HTableInterface} via {@link #getTable(byte[])}, you may get the same
 * table as last time, or it may be a new table.
 * <p>
 * You <b>should not call {@link HTableInterface#close()} </b> that is handled when the table goes
 * out of scope. Along the same lines, you must ensure to not keep a reference to the table for
 * longer than necessary - this leak will ensure that the table never gets closed.
 */
public class CachingHTableFactory implements HTableFactory {

  private static final Log LOG = LogFactory.getLog(CachingHTableFactory.class);
  private HTableFactory delegate;

  Map<ImmutableBytesPtr, SoftReference<HTableInterface>> openTables =
      new HashMap<ImmutableBytesPtr, SoftReference<HTableInterface>>();

  private ReferenceQueue<HTableInterface> referenceQueue;

  private Thread cleanupThread;

  public CachingHTableFactory(HTableFactory tableFactory) {
    this.delegate = tableFactory;
    this.referenceQueue = new ReferenceQueue<HTableInterface>();
    this.cleanupThread = new Thread(new TableCleaner(referenceQueue), "cached-table-cleanup");
    cleanupThread.setDaemon(true);
    cleanupThread.start();
  }

  @Override
  public HTableInterface getTable(ImmutableBytesPtr tablename) throws IOException {
    ImmutableBytesPtr tableBytes = new ImmutableBytesPtr(tablename);
    synchronized (openTables) {
      SoftReference<HTableInterface> ref = openTables.get(tableBytes);
      // the reference may be null, in which case this is a new table, or the underlying HTable may
      // have been GC'ed.
      @SuppressWarnings("resource")
      HTableInterface table = ref == null ? null : ref.get();
      if (table == null) {
        table = delegate.getTable(tablename);
        openTables.put(tableBytes, new SoftReference<HTableInterface>(table, referenceQueue));
      }
      return table;
    }
  }

  @Override
  public void shutdown() {
    this.cleanupThread.interrupt();
    this.delegate.shutdown();
  }

  /**
   * Cleaner to ensure that any tables that are GC'ed are also closed.
   */
  private class TableCleaner implements Runnable {

    private ReferenceQueue<HTableInterface> queue;

    public TableCleaner(ReferenceQueue<HTableInterface> referenceQueue) {
      this.queue = referenceQueue;
    }

    @Override
    public void run() {
      try {
        HTableInterface table = this.queue.remove().get();
        if (table != null) {
          try {
            table.close();
          } catch (IOException e) {
            LOG.error(
              "Failed to correctly close htable, ignoring it further since it is being GC'ed", e);
          }
        }
      } catch (InterruptedException e) {
        LOG.info("Recieved an interrupt - assuming system is going down. Closing all remaining HTables and quitting!");
        for (SoftReference<HTableInterface> ref : openTables.values()) {
          HTableInterface table = ref.get();
          if (table != null) {
            try {
              LOG.info("Closing connection to index table: " + Bytes.toString(table.getTableName()));
              table.close();
            } catch (IOException ioe) {
              LOG.error(
                "Failed to correctly close htable on shutdown! Ignoring and closing remaining tables",
                ioe);
            }
          }
        }
      }
    }
  }
}