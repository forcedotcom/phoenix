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
package com.salesforce.hbase.index.covered.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * Test filter to ensure that it correctly handles KVs of different types correctly
 */
public class TestApplyAndFilterDeletesFilter {

  private byte[] row = Bytes.toBytes("row");
  private byte[] family = Bytes.toBytes("family");
  private byte[] qualifier = Bytes.toBytes("qualifier");
  private byte[] value = Bytes.toBytes("value");
  private long ts = 10;

  @Test
  public void testDeletesAreNotReturned() {
    KeyValue kv =createKvForType(Type.Delete);
    ApplyAndFilterDeletesFilter filter =
        new ApplyAndFilterDeletesFilter(Collections.<byte[]> emptyList());
    assertEquals("Didn't skip point delete!", ReturnCode.SKIP, filter.filterKeyValue(kv));

    filter.reset();
    kv = createKvForType(Type.DeleteColumn);
    assertEquals("Didn't seek from column delete!", ReturnCode.SEEK_NEXT_USING_HINT,
      filter.filterKeyValue(kv));

    filter.reset();
    kv = createKvForType(Type.DeleteFamily);
    assertEquals("Didn't seek from family delete!", ReturnCode.SEEK_NEXT_USING_HINT,
      filter.filterKeyValue(kv));
  }

  /**
   * Hinting with this filter is a little convoluted as we binary search the list of families to
   * attempt to find the right one to seek.
   */
  @Test
  public void testHintCorrectlyToNextFamily() {
    // start with doing a family delete, so we will seek to the next column
    KeyValue kv = createKvForType(Type.DeleteFamily);
    ApplyAndFilterDeletesFilter filter =
        new ApplyAndFilterDeletesFilter(Collections.<byte[]> emptyList());
    filter.filterKeyValue(kv);
    // make sure the hint is our attempt at the end key, because we have no more families to seek
    assertEquals("Didn't get END_KEY with no families to match", KeyValue.LOWESTKEY,
      filter.getNextKeyHint(kv));

    // check for a family that comes before our family, so we always seek to the end as well
    filter = new ApplyAndFilterDeletesFilter(Collections.singletonList(Bytes.toBytes("afamily")));
    filter.filterKeyValue(kv);
    // make sure the hint is our attempt at the end key, because we have no more families to seek
    assertEquals("Didn't get END_KEY with no families to match", KeyValue.LOWESTKEY,
      filter.getNextKeyHint(kv));

    // check that we seek to the correct family that comes after our family
    byte[] laterFamily = Bytes.toBytes("zfamily");
    filter = new ApplyAndFilterDeletesFilter(Collections.singletonList(laterFamily));
    filter.filterKeyValue(kv);
    KeyValue next = KeyValue.createFirstOnRow(kv.getRow(), laterFamily, new byte[0]);
    assertEquals("Didn't get correct next key with a next family", next, filter.getNextKeyHint(kv));
  }

  /**
   * Point deletes should only cover the exact entry they are tied to. Earlier puts should always
   * show up.
   */
  @Test
  public void testCoveringPointDelete() {
    // start with doing a family delete, so we will seek to the next column
    KeyValue kv = createKvForType(Type.Delete);
    ApplyAndFilterDeletesFilter filter =
        new ApplyAndFilterDeletesFilter(Collections.<byte[]> emptyList());
    filter.filterKeyValue(kv);
    KeyValue put = createKvForType(Type.Put);
    assertEquals("Didn't filter out put with same timestamp!", ReturnCode.SKIP,
      filter.filterKeyValue(put));
    // we should filter out the exact same put again, which could occur with the kvs all kept in the
    // same memstore
    assertEquals("Didn't filter out put with same timestamp on second call!", ReturnCode.SKIP,
      filter.filterKeyValue(put));

    // ensure then that we don't filter out a put with an earlier timestamp (though everything else
    // matches)
    put = createKvForType(Type.Put, ts - 1);
    assertEquals("Didn't accept put that has an earlier ts than the covering delete!",
      ReturnCode.INCLUDE, filter.filterKeyValue(put));
  }

  private KeyValue createKvForType(Type t) {
    return createKvForType(t, this.ts);
  }

  private KeyValue createKvForType(Type t, long timestamp) {
    return new KeyValue(row, family, qualifier, 0, qualifier.length, timestamp, t, value, 0,
        value.length);
  }

  /**
   * Test that when we do a column delete at a given timestamp that we delete the entire column.
   * @throws Exception
   */
  @Test
  public void testCoverForDeleteColumn() throws Exception {
    ApplyAndFilterDeletesFilter filter =
        new ApplyAndFilterDeletesFilter(Collections.<byte[]> emptyList());
    KeyValue dc = createKvForType(Type.DeleteColumn, 11);
    KeyValue put = createKvForType(Type.Put, 10);
    assertEquals("Didn't filter out delete column.", ReturnCode.SEEK_NEXT_USING_HINT,
      filter.filterKeyValue(dc));
    // seek past the given put
    KeyValue seek = filter.getNextKeyHint(dc);
    assertTrue("Seeked key wasn't past the expected put - didn't skip the column",
      KeyValue.COMPARATOR.compare(seek, put) > 0);
  }
}