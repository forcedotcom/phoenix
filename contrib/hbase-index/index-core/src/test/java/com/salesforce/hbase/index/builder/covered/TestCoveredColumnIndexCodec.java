package com.salesforce.hbase.index.builder.covered;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestCoveredColumnIndexCodec {
  private static final byte[] PK = new byte[] { 'a' };
  private static final String FAMILY_STRING = "family";
  private static final byte[] FAMILY = Bytes.toBytes(FAMILY_STRING);
  private static final byte[] QUAL = Bytes.toBytes("qual");

  @Test
  public void toFromIndexKey() throws Exception {
    // start with empty values
    byte[] indexKey = CoveredColumnIndexCodec
        .composeRowKey(PK, 0, Collections.<byte[]> emptyList());
    List<byte[]> stored = CoveredColumnIndexCodec.getValues(indexKey);
    assertEquals("Found some stored values in an index row key that wasn't created with values!",
      0, stored.size());

    // a single, empty value
    indexKey = CoveredColumnIndexCodec.composeRowKey(PK, 0, Lists.newArrayList(new byte[0]));
    stored = CoveredColumnIndexCodec.getValues(indexKey);
    assertEquals("Found some stored values in an index row key that wasn't created with values!",
      1, stored.size());
    assertEquals("Found a non-zero length value: " + Bytes.toString(stored.get(0)), 0,
      stored.get(0).length);

    // try with a couple values, some different lengthss
    byte[] v1 = new byte[] { 'a' };
    byte[] v2 = new byte[] { 'b' };
    byte[] v3 = Bytes.toBytes("v3");
    int len = v1.length + v2.length + v3.length;
    indexKey = CoveredColumnIndexCodec.composeRowKey(PK, len, Arrays.asList(v1, v2, v3));
    stored = CoveredColumnIndexCodec.getValues(indexKey);
    assertEquals("Didn't find expected number of values in index key!", 3, stored.size());
    assertTrue("First index keys don't match!", Bytes.equals(v1, stored.get(0)));
    assertTrue("Second index keys don't match!", Bytes.equals(v2, stored.get(1)));
    assertTrue("Third index keys don't match!", Bytes.equals(v3, stored.get(2)));
  }

  /**
   * If the Result has a <tt>null</tt> backing {@link KeyValue} list, you could possibly get a
   * {@link NullPointerException}.
   */
  @Test
  public void testWorksWithEmptyResult() {
    ColumnGroup group = new ColumnGroup("group");
    CoveredColumn column = new CoveredColumn(FAMILY_STRING, QUAL);
    group.add(column);
    Result r = new Result();
    new CoveredColumnIndexCodec(r, group);
  }

  @Test
  public void testFullColumnSpecification() {
    ColumnGroup group = new ColumnGroup("testFullColumnSpecification");
    CoveredColumn column = new CoveredColumn(FAMILY_STRING, QUAL);
    group.add(column);

    // setup the kvs to add
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    byte[] v1 = Bytes.toBytes("v1");
    KeyValue kv = new KeyValue(PK, FAMILY, QUAL, 1, v1);
    kvs.add(kv);
    byte[] v2 = Bytes.toBytes("v2");
    kv = new KeyValue(PK, Bytes.toBytes("family2"), QUAL, 1, v2);
    kvs.add(kv);
    Result r = new Result(kvs);

    // make a new codec on those kvs
    CoveredColumnIndexCodec codec = new CoveredColumnIndexCodec(r, group);

    // simple case - no deletes
    byte[] indexValue = getLatestIndexKey(codec);
    byte[] expected = CoveredColumnIndexCodec.composeRowKey(PK, v1.length, Arrays.asList(v1));
    assertArrayEquals("Didn't get expected index value", expected, indexValue);

    // now add a delete that covers that column family
    Delete d = new Delete(PK);
    d.deleteFamily(FAMILY);
    codec.addUpdateForTesting(d);
    indexValue = getLatestIndexKey(codec);
    expected = CoveredColumnIndexCodec.composeRowKey(PK, 0, Lists.newArrayList(new byte[0]));
    assertArrayEquals("Deleting family didn't specify null value as expected", expected, indexValue);

    // reset the codec
    codec = new CoveredColumnIndexCodec(r, group);

    // now try with a full column delete
    d = new Delete(PK);
    d.deleteColumns(FAMILY, QUAL);
    codec.addUpdateForTesting(d);
    indexValue = getLatestIndexKey(codec);
    expected = CoveredColumnIndexCodec.composeRowKey(PK, 0, Lists.newArrayList(new byte[0]));
    assertArrayEquals("Deleting family didn't specify null value as expected", expected, indexValue);

    // reset the codec
    codec = new CoveredColumnIndexCodec(r, group);

    // now try by deleting the single value
    d = new Delete(PK);
    d.deleteColumn(FAMILY, QUAL);
    codec.addUpdateForTesting(d);
    indexValue = getLatestIndexKey(codec);
    expected = CoveredColumnIndexCodec.composeRowKey(PK, 0, Lists.newArrayList(new byte[0]));
    assertArrayEquals("Deleting family didn't specify null value as expected", expected, indexValue);
  }

  /**
   * Test where we match against all the qualifiers in a single column family with a single stored
   * famility:qualifier that matches. This is the simpler case to
   * {@link #testFullyCoveredFamilyWithMultipleStoredColumns()}.
   */
  @Test
  public void testFullyCoveredFamilyWithSingleStoredColumn() {
    ColumnGroup group = new ColumnGroup("testOnlySpecifyFamilyWithSingleStoredColumn");
    CoveredColumn column = new CoveredColumn(FAMILY_STRING, null);
    group.add(column);

    // setup the underlying data
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    byte[] v1 = Bytes.toBytes("v1");
    KeyValue kv = new KeyValue(PK, FAMILY, QUAL, 1, v1);
    kvs.add(kv);
    byte[] v2 = Bytes.toBytes("v2");
    kv = new KeyValue(PK, Bytes.toBytes("family2"), QUAL, 1, v2);
    kvs.add(kv);
    kvs.add(kv);
    Result r = new Result(kvs);

    // setup the codec on those kvs
    CoveredColumnIndexCodec codec = new CoveredColumnIndexCodec(r, group);

    // simple case - no deletes, all columns
    byte[] indexValue = getLatestIndexKey(codec);
    byte[] expected = CoveredColumnIndexCodec.composeRowKey(PK, v1.length, Arrays.asList(v1));
    assertArrayEquals("Didn't get expected index value", expected, indexValue);

    // now add a delete that covers that entire column family
    Delete d = new Delete(PK);
    d.deleteFamily(FAMILY);
    codec.addUpdateForTesting(d);
    indexValue = getLatestIndexKey(codec);
    expected = CoveredColumnIndexCodec.composeRowKey(PK, 0, Lists.newArrayList(new byte[0]));
    assertArrayEquals("Deleting family didn't specify null value as expected", expected, indexValue);

    // reset the codec
    codec = new CoveredColumnIndexCodec(r, group);

    // now try deleting one of the columns
    d = new Delete(PK);
    d.deleteColumns(FAMILY, QUAL);
    codec.addUpdateForTesting(d);
    indexValue = getLatestIndexKey(codec);
    expected = CoveredColumnIndexCodec.composeRowKey(PK, 0, Lists.newArrayList(new byte[0]));
    assertArrayEquals("Deleting family didn't specify null value as expected", expected, indexValue);

    // reset the codec
    codec = new CoveredColumnIndexCodec(r, group);

    // now try by deleting the single value
    d = new Delete(PK);
    d.deleteColumn(FAMILY, QUAL);
    codec.addUpdateForTesting(d);
    indexValue = getLatestIndexKey(codec);
    expected = CoveredColumnIndexCodec.composeRowKey(PK, 0, Lists.newArrayList(new byte[0]));
    assertArrayEquals("Deleting family didn't specify null value as expected", expected, indexValue);
  }

  @Test
  public void testFullyCoveredFamilyWithMultipleStoredColumns() {
    ColumnGroup group = new ColumnGroup("testOnlySpecifyFamilyWithMultipleStoredColumns");
    CoveredColumn column = new CoveredColumn(FAMILY_STRING, null);
    group.add(column);

    List<KeyValue> kvs = new ArrayList<KeyValue>();
    byte[] v1 = Bytes.toBytes("v1");
    KeyValue kv = new KeyValue(PK, FAMILY, QUAL, 1, v1);
    kvs.add(kv);
    byte[] v2 = Bytes.toBytes("v2");
    kv = new KeyValue(PK, Bytes.toBytes("family2"), QUAL, 1, v2);
    kvs.add(kv);
    // add another value for a different column in the indexed family
    byte[] v3 = Bytes.toBytes("v3");
    kv = new KeyValue(PK, FAMILY, Bytes.toBytes("qual2"), 1, v3);
    kvs.add(kv);
    Result r = new Result(kvs);

    // setup the codec on those kvs
    CoveredColumnIndexCodec codec = new CoveredColumnIndexCodec(r, group);

    // simple case - no deletes, all columns
    byte[] indexValue = getLatestIndexKey(codec);
    byte[] expected = CoveredColumnIndexCodec.composeRowKey(PK, v1.length + v3.length,
      Arrays.asList(v1, v3));
    assertArrayEquals("Didn't get expected index value", expected, indexValue);

    // now add a delete that covers that entire column family
    Delete d = new Delete(PK);
    d.deleteFamily(FAMILY);
    codec.addUpdateForTesting(d);
    indexValue = getLatestIndexKey(codec);
    expected = CoveredColumnIndexCodec.composeRowKey(PK, 0, Lists.newArrayList(new byte[0]));
    assertArrayEquals("Deleting family didn't specify null value as expected", expected, indexValue);

    // reset the codec
    codec = new CoveredColumnIndexCodec(r, group);

    // now try deleting one of the columns
    d = new Delete(PK);
    d.deleteColumns(FAMILY, QUAL);
    codec.addUpdateForTesting(d);
    indexValue = getLatestIndexKey(codec);
    expected = CoveredColumnIndexCodec.composeRowKey(PK, v3.length,
      Lists.newArrayList(new byte[0], v3));
    assertArrayEquals("Deleting columns didn't specify null value as expected", expected,
      indexValue);

    // reset the codec
    codec = new CoveredColumnIndexCodec(r, group);

    // now try by deleting the single value
    d = new Delete(PK);
    d.deleteColumn(FAMILY, QUAL);
    codec.addUpdateForTesting(d);
    indexValue = getLatestIndexKey(codec);
    expected = CoveredColumnIndexCodec.composeRowKey(PK, v3.length,
      Lists.newArrayList(new byte[0], v3));
    assertArrayEquals("Deleting col:qual didn't specify null value as expected", expected,
      indexValue);
  }

  @Test
  public void testCheckRowKeyForAllNulls() {
    byte[] pk = new byte[] { 'a', 'b', 'z' };
    // check positive cases first
    byte[] result = CoveredColumnIndexCodec.composeRowKey(pk, 0, Arrays.asList(new byte[0]));
    assertTrue("Didn't correctly read single element as being null in row key",
      CoveredColumnIndexCodec.checkRowKeyForAllNulls(result));
    result = CoveredColumnIndexCodec.composeRowKey(pk, 0, Arrays.asList(new byte[0], new byte[0]));
    assertTrue("Didn't correctly read two elements as being null in row key",
      CoveredColumnIndexCodec.checkRowKeyForAllNulls(result));

    // check cases where it isn't null
    result = CoveredColumnIndexCodec.composeRowKey(pk, 2, Arrays.asList(new byte[] { 1, 2 }));
    assertFalse("Found a null key, when it wasn't!",
      CoveredColumnIndexCodec.checkRowKeyForAllNulls(result));
    result = CoveredColumnIndexCodec.composeRowKey(pk, 2,
      Arrays.asList(new byte[] { 1, 2 }, new byte[0]));
    assertFalse("Found a null key, when it wasn't!",
      CoveredColumnIndexCodec.checkRowKeyForAllNulls(result));
  }

  private byte[] getLatestIndexKey(CoveredColumnIndexCodec codec) {
    return codec.getIndexRowKey(Long.MAX_VALUE).rowKey;
  }
}