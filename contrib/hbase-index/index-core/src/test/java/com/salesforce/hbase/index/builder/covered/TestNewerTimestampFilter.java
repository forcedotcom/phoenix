package com.salesforce.hbase.index.builder.covered;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.salesforce.hbase.index.builder.covered.util.NewerTimestampFilter;

public class TestNewerTimestampFilter {
  byte[] row = new byte[] { 'a' };
  byte[] fam = Bytes.toBytes("family");
  byte[] qual = new byte[] { 'b' };
  byte[] val = Bytes.toBytes("val");

  @Test
  public void testOnlyAllowsOlderTimestamps() {
    long ts = 100;
    NewerTimestampFilter filter = new NewerTimestampFilter(ts);

    KeyValue kv = new KeyValue(row, fam, qual, ts, val);
    assertEquals("Didn't accept kv with matching ts", ReturnCode.INCLUDE, filter.filterKeyValue(kv));

    kv = new KeyValue(row, fam, qual, ts + 1, val);
    assertEquals("Didn't skip kv with greater ts", ReturnCode.SKIP, filter.filterKeyValue(kv));

    kv = new KeyValue(row, fam, qual, ts - 1, val);
    assertEquals("Didn't accept kv with lower ts", ReturnCode.INCLUDE, filter.filterKeyValue(kv));
  }
}