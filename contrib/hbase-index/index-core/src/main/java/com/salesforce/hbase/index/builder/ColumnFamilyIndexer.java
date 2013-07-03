package com.salesforce.hbase.index.builder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.salesforce.hbase.index.IndexUtil;

/**
 * Simple indexer that just indexes rows based on their column families
 * <p>
 * Doesn't do any index cleanup; this is just a basic example case.
 */
public class ColumnFamilyIndexer extends BaseIndexBuilder {

  private static final String INDEX_TO_TABLE_CONF_PREFX = "hbase.index.family.";
  private static final String INDEX_TO_TABLE_COUNT_KEY = INDEX_TO_TABLE_CONF_PREFX + "families";
  private static final String SEPARATOR = ",";

  static final byte[] INDEX_ROW_COLUMN_FAMILY = Bytes.toBytes("ROW");
  static final byte[] INDEX_REMAINING_COLUMN_FAMILY = Bytes.toBytes("REMAINING");

  public static void enableIndexing(HTableDescriptor desc, Map<byte[], String> familyMap)
      throws IOException {
    // not indexing any families, so we shouldn't add the indexer
    if (familyMap == null || familyMap.size() == 0) {
      return;
    }
    Map<String, String> opts = new HashMap<String, String>();
    List<String> families = new ArrayList<String>(familyMap.size());

    for (Entry<byte[], String> family : familyMap.entrySet()) {
      String fam = Bytes.toString(family.getKey());
      opts.put(INDEX_TO_TABLE_CONF_PREFX + fam, family.getValue());
      families.add(fam);
    }

    // add the list of families so we can deserialize each
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < families.size(); i++) {
      sb.append(families.get(i));
      if (i < families.size() - 1) {
        sb.append(SEPARATOR);
      }
    }
    opts.put(INDEX_TO_TABLE_COUNT_KEY, sb.toString());
    IndexUtil.enableIndexing(desc, ColumnFamilyIndexer.class, opts);
  }

  private Map<ImmutableBytesWritable, String> columnTargetMap;

  @Override
  public void setup(RegionCoprocessorEnvironment env) {
    Configuration conf = env.getConfiguration();
    String[] families = conf.get(INDEX_TO_TABLE_COUNT_KEY).split(SEPARATOR);

    // build up our mapping of column - > index table
    columnTargetMap = new HashMap<ImmutableBytesWritable, String>(families.length);
    for (int i = 0; i < families.length; i++) {
      byte[] fam = Bytes.toBytes(families[i]);
      String indexTable = conf.get(INDEX_TO_TABLE_CONF_PREFX + families[i]);
      columnTargetMap.put(new ImmutableBytesWritable(fam), indexTable);
    }
  }

  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdate(Put p) {
    // if not columns to index, we are done and don't do anything special
    if (columnTargetMap == null || columnTargetMap.size() == 0) {
      return null;
    }

    Collection<Pair<Mutation, String>> updateMap = new ArrayList<Pair<Mutation, String>>();
    Set<byte[]> keys = p.getFamilyMap().keySet();
    for (Entry<byte[], List<KeyValue>> entry : p.getFamilyMap().entrySet()) {
      String ref = columnTargetMap
          .get(new ImmutableBytesWritable(entry.getKey()));
      // no reference for that column, skip it
      if (ref == null) {
        continue;
      }

      // get the keys for this family
      List<KeyValue> kvs = entry.getValue();
      if (kvs == null || kvs.isEmpty()) {
        // should never be the case, but just to be careful
        continue;
      }

      // swap the row key and the column family
      Put put = new Put(kvs.get(0).getFamily());
      // got through each of the family's key-values and add it to the put
      for (KeyValue kv : entry.getValue()) {
        put.add(ColumnFamilyIndexer.INDEX_ROW_COLUMN_FAMILY,
          ArrayUtils.addAll(kv.getRow(), kv.getQualifier()), kv.getValue());
      }

      // go through the rest of the families and add them to the put, under the special columnfamily
      for (byte[] key : keys) {
        if (!Bytes.equals(key, entry.getKey())) {
          List<KeyValue> otherFamilyKeys = p.getFamilyMap().get(key);
          if (otherFamilyKeys == null || otherFamilyKeys.isEmpty()) {
            continue;
          }
          for (KeyValue kv : otherFamilyKeys) {
            put.add(ColumnFamilyIndexer.INDEX_REMAINING_COLUMN_FAMILY,
              ArrayUtils.addAll(kv.getFamily(), kv.getQualifier()), kv.getValue());
          }
        }
      }

      // add the mapping
      updateMap.add(new Pair<Mutation, String>(put, ref));
    }
    return updateMap;
  }

  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdate(Delete d) {
    // if no columns to index, we are done and don't do anything special
    if (columnTargetMap == null || columnTargetMap.size() == 0) {
      return null;
    }

    Collection<Pair<Mutation, String>> updateMap = new ArrayList<Pair<Mutation, String>>();
    for (Entry<byte[], List<KeyValue>> entry : d.getFamilyMap().entrySet()) {
      String ref = columnTargetMap
          .get(new ImmutableBytesWritable(entry.getKey()));
      // no reference for that column, skip it
      if (ref == null) {
        continue;
      }
      List<KeyValue> kvs = entry.getValue();
      if (kvs == null || kvs.isEmpty()) {
        continue;
      }

      // swap the row key and the column family - we only need the row key since we index on the
      // column family from the original update
      Delete delete = new Delete(kvs.get(0).getFamily());
      // add the mapping
      updateMap.add(new Pair<Mutation, String>(delete, ref));
    }
    return updateMap;
  }

  /**
   * Create the specified index table with the necessary columns
   * @param admin {@link HBaseAdmin} to use when creating the table
   * @param indexTable name of the index table. Should be specified in
   *          {@link setupColumnFamilyIndex} as an index target
   */
  public static void createIndexTable(HBaseAdmin admin, String indexTable) throws IOException {
    HTableDescriptor index = new HTableDescriptor(indexTable);
    index.addFamily(new HColumnDescriptor(INDEX_REMAINING_COLUMN_FAMILY));
    index.addFamily(new HColumnDescriptor(INDEX_ROW_COLUMN_FAMILY));

    admin.createTable(index);
  }

  /**
   * Doesn't do any index cleanup. This is just a basic example case.
   */
  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdateForFilteredRows(
      Collection<KeyValue> filtered)
      throws IOException {
    return null;
  }
}