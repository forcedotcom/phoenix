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
package com.salesforce.hbase.index.covered.data;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestIndexMemStore {

  private static final byte[] row = Bytes.toBytes("row");
  private static final byte[] family = Bytes.toBytes("family");
  private static final byte[] qual = Bytes.toBytes("qual");
  private static final byte[] val = Bytes.toBytes("val");
  private static final byte[] val2 = Bytes.toBytes("val2");

  @Test
  public void testCorrectOverwritting() throws Exception {
    IndexMemStore store = new IndexMemStore(IndexMemStore.COMPARATOR);
    long ts = 10;
    KeyValue kv = new KeyValue(row, family, qual, ts, Type.Put, val);
    kv.setMemstoreTS(2);
    KeyValue kv2 = new KeyValue(row, family, qual, ts, Type.Put, val2);
    kv2.setMemstoreTS(0);
    store.add(kv, true);
    // adding the exact same kv shouldn't change anything stored if not overwritting
    store.add(kv2, false);
    KeyValueScanner scanner = store.getScanner();
    KeyValue first = KeyValue.createFirstOnRow(row);
    scanner.seek(first);
    assertTrue("Overwrote kv when specifically not!", kv == scanner.next());
    scanner.close();

    // now when we overwrite, we should get the newer one
    store.add(kv2, true);
    scanner = store.getScanner();
    scanner.seek(first);
    assertTrue("Didn't overwrite kv when specifically requested!", kv2 == scanner.next());
    scanner.close();
  }

  /**
   * We don't expect custom KeyValue creation, so we can't get into weird situations, where a
   * {@link Type#DeleteFamily} has a column qualifier specified.
   * @throws Exception
   */
  @Test
  public void testExpectedOrdering() throws Exception {
    IndexMemStore store = new IndexMemStore();
    KeyValue kv = new KeyValue(row, family, qual, 12, Type.Put, val);
    store.add(kv, true);
    KeyValue kv2 = new KeyValue(row, family, qual, 10, Type.Put, val2);
    store.add(kv2, true);
    KeyValue df = new KeyValue(row, family, null, 11, Type.DeleteFamily, null);
    store.add(df, true);
    KeyValue dc = new KeyValue(row, family, qual, 11, Type.DeleteColumn, null);
    store.add(dc, true);
    KeyValue d = new KeyValue(row, family, qual, 12, Type.Delete, null);
    store.add(d, true);

    // null qualifiers should always sort before the non-null cases
    KeyValueScanner scanner = store.getScanner();
    KeyValue first = KeyValue.createFirstOnRow(row);
    assertTrue("Didn't have any data in the scanner", scanner.seek(first));
    assertTrue("Didn't get delete family first (no qualifier == sort first)", df == scanner.next());
    assertTrue("Didn't get point delete before corresponding put", d == scanner.next());
    assertTrue("Didn't get larger ts Put", kv == scanner.next());
    assertTrue("Didn't get delete column before corresponding put(delete sorts first)",
      dc == scanner.next());
    assertTrue("Didn't get smaller ts Put", kv2 == scanner.next());
    assertNull("Have more data in the scanner", scanner.next());
  }
}