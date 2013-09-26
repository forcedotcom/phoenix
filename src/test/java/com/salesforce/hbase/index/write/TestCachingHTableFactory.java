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

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.mockito.Mockito;

import com.salesforce.hbase.index.table.CachingHTableFactory;
import com.salesforce.hbase.index.table.HTableFactory;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;

public class TestCachingHTableFactory {

  @Test
  public void testCacheCorrectlyExpiresTable() throws Exception {
    // setup the mocks for the tables we will request
    HTableFactory delegate = Mockito.mock(HTableFactory.class);
    ImmutableBytesPtr t1 = new ImmutableBytesPtr(Bytes.toBytes("t1"));
    ImmutableBytesPtr t2 = new ImmutableBytesPtr(Bytes.toBytes("t2"));
    ImmutableBytesPtr t3 = new ImmutableBytesPtr(Bytes.toBytes("t3"));
    HTableInterface table1 = Mockito.mock(HTableInterface.class);
    HTableInterface table2 = Mockito.mock(HTableInterface.class);
    HTableInterface table3 = Mockito.mock(HTableInterface.class);
    Mockito.when(delegate.getTable(t1)).thenReturn(table1);
    Mockito.when(delegate.getTable(t2)).thenReturn(table2);
    Mockito.when(delegate.getTable(t3)).thenReturn(table3);
    
    // setup our factory with a cache size of 2
    CachingHTableFactory factory = new CachingHTableFactory(delegate, 2);
    factory.getTable(t1);
    factory.getTable(t2);
    factory.getTable(t3);
    // get the same table a second time, after it has gone out of cache
    factory.getTable(t1);
    
    Mockito.verify(delegate, Mockito.times(2)).getTable(t1);
    Mockito.verify(delegate, Mockito.times(1)).getTable(t2);
    Mockito.verify(delegate, Mockito.times(1)).getTable(t3);
    Mockito.verify(table1).close();
  }
}