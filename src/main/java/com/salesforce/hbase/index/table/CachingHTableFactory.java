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
import java.util.Map;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.hbase.index.util.ImmutableBytesPtr;

/**
 * A simple cache that just uses usual GC mechanisms to cleanup unused {@link HTableInterface}s.
 * When requesting an {@link HTableInterface} via {@link #getTable}, you may get the same table as
 * last time, or it may be a new table.
 * <p>
 * You <b>should not call {@link HTableInterface#close()} </b> that is handled when the table goes
 * out of scope. Along the same lines, you must ensure to not keep a reference to the table for
 * longer than necessary - this leak will ensure that the table never gets closed.
 */
public class CachingHTableFactory implements HTableFactory {

  /**
   * LRUMap that closes the {@link HTableInterface} when the table is evicted
   */
  @SuppressWarnings("serial")
  public class HTableInterfaceLRUMap extends LRUMap {

    public HTableInterfaceLRUMap(int cacheSize) {
      super(cacheSize);
    }

    @Override
    protected boolean removeLRU(LinkEntry entry) {
      HTableInterface table = (HTableInterface) entry.getValue();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Closing connection to table: " + Bytes.toString(table.getTableName())
            + " because it was evicted from the cache.");
      }
      try {
        table.close();
      } catch (IOException e) {
        LOG.info("Failed to correctly close HTable: " + Bytes.toString(table.getTableName())
            + " ignoring since being removed from queue.");
      }
      return true;
    }
  }

  public static int getCacheSize(Configuration conf) {
    return conf.getInt(CACHE_SIZE_KEY, DEFAULT_CACHE_SIZE);
  }

  private static final Log LOG = LogFactory.getLog(CachingHTableFactory.class);
  private static final String CACHE_SIZE_KEY = "index.tablefactory.cache.size";
  private static final int DEFAULT_CACHE_SIZE = 10;

  private HTableFactory delegate;

  @SuppressWarnings("rawtypes")
  Map openTables;

  public CachingHTableFactory(HTableFactory tableFactory, Configuration conf) {
    this(tableFactory, getCacheSize(conf));
  }

  public CachingHTableFactory(HTableFactory factory, int cacheSize) {
    this.delegate = factory;
    openTables = new HTableInterfaceLRUMap(cacheSize);
  }

  @Override
  @SuppressWarnings("unchecked")
  public HTableInterface getTable(ImmutableBytesPtr tablename) throws IOException {
    ImmutableBytesPtr tableBytes = new ImmutableBytesPtr(tablename);
    synchronized (openTables) {
      HTableInterface table = (HTableInterface) openTables.get(tableBytes);
      if (table == null) {
        table = delegate.getTable(tablename);
        openTables.put(tableBytes, table);
      }
      return table;
    }
  }

  @Override
  public void shutdown() {
    this.delegate.shutdown();
  }
}