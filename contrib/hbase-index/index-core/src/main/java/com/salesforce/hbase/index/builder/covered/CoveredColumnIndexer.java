package com.salesforce.hbase.index.builder.covered;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.salesforce.hbase.index.builder.BaseIndexBuilder;

/**
 * Index maintainer that maintains multiple indexes based on '{@link ColumnGroup}s'. Each group is a
 * fully covered within itself and stores the fully 'pre-joined' version of that values for that
 * group of columns.
 * <p>
 * <h2>Index Layout</h2> The row key for a given index entry is the current state of the all the
 * values of the columns in a column group, followed by the primary key (row key) of the original
 * row, and then the length of each value and then finally the total number of values. This is then
 * enough information to completely rebuild the latest value of row for each column in the group.
 * <p>
 * The family is always {@link CoveredColumnIndexCodec#INDEX_ROW_COLUMN_FAMILY}
 * <p>
 * The qualifier is prepended with the integer index (serialized with {@link Bytes#toBytes(int)}) of
 * the column in the group. This index corresponds the index of the value for the group in the row
 * key.
 * 
 * <pre>
 *         ROW                            ||   FAMILY     ||    QUALIFIER     ||   VALUE
 * (v1)(v2)...(vN)(pk)(L1)(L2)...(Ln)(#V) || INDEX_FAMILY ||     1Cf1:Cq1     ||  null
 * (v1)(v2)...(vN)(pk)(L1)(L2)...(Ln)(#V) || INDEX_FAMILY ||     2Cf2:Cq2     ||  null
 * ...
 * (v1)(v2)...(vN)(pk)(L1)(L2)...(Ln)(#V) || INDEX_FAMILY ||     NCfN:CqN     ||  null
 * </pre>
 * 
 * <h2>Index Maintenance</h2>
 * <p>
 * When making an insertion into the table, we also attempt to cleanup the index. This means that we
 * need to remove the previous entry from the index. Generally, this is completed by inserting a
 * delete at the previous value of the previous row.
 * <p>
 * The main caveat here is when dealing with custom timestamps. If there is no special timestamp
 * specified, we can just insert the proper {@link Delete} at the current timestamp and move on.
 * However, when the client specifies a timestamp, we could see updates out of order. In that case,
 * we can do an insert using the specified timestamp, but a delete is different...
 * <p>
 * Taking the simple case, assume we do a single column in a group. Then if we get an out of order
 * update, we need to check the current state of that column in the current row. If the current row
 * is older, we can issue a delete as normal. If the current row is newer, however, we then have to
 * issue a delete for the index update at the time of the current row. This ensures that the index
 * update made for the 'future' time still covers the existing row.
 * <p>
 * <b>ASSUMPTION:</b> all key-values in a single {@link Delete}/{@link Put} have the same timestamp.
 * This dramatically simplifies the logic needed to manage updating the index for out-of-order
 * {@link Put}s as we don't need to manage multiple levels of timestamps across multiple columns.
 * <p>
 * We can extend this to multiple columns by picking the latest update of any column in group as the
 * delete point.
 * <p>
 * <b>NOTE:</b> this means that we need to do a lookup (point {@link Get}) of the entire row
 * <i>every time there is a write to the table</i>.
 */
public class CoveredColumnIndexer extends BaseIndexBuilder {

  private static final Log LOG = LogFactory.getLog(CoveredColumnIndexer.class);

  /** Empty put that has no information - used to build index delete markers */

  /**
   * Create the specified index table with the necessary columns
   * @param admin {@link HBaseAdmin} to use when creating the table
   * @param indexTable name of the index table.
   * @throws IOException
   */
  public static void createIndexTable(HBaseAdmin admin, String indexTable) throws IOException {
    HTableDescriptor index = new HTableDescriptor(indexTable);
    HColumnDescriptor col = new HColumnDescriptor(CoveredColumnIndexCodec.INDEX_ROW_COLUMN_FAMILY);
    // ensure that we can 'see past' delete markers when doing scans
    col.setKeepDeletedCells(true);
    index.addFamily(col);
    admin.createTable(index);
  }

  private volatile HTableInterface localTable;
  private List<ColumnGroup> groups;
  private RegionCoprocessorEnvironment env;

  @Override
  public void setup(RegionCoprocessorEnvironment env) throws IOException {
    groups = CoveredColumnIndexSpecifierBuilder.getColumns(env.getConfiguration());
    this.env = env;
  }

  // TODO we loop through all the keyvalues for the row a few times - we should be able to do better

  /**
   * Ensure we have a connection to the local table. We need to do this after
   * {@link #setup(RegionCoprocessorEnvironment)} because we are created on region startup and the
   * table isn't actually accessible until later.
   * @throws IOException if we can't reach the table
   */
  private void ensureLocalTable() throws IOException {
    if (this.localTable == null) {
      synchronized (this) {
        if (this.localTable == null) {
          localTable = env.getTable(env.getRegion().getTableDesc().getName());
        }
      }
    }
  }

  @Override
  public Map<Mutation, String> getIndexUpdate(Put p) throws IOException {
    // if not columns to index, we are done
    if (groups == null || groups.size() == 0) {
      return Collections.emptyMap();
    }

    ensureLocalTable();

    // get the current state of the row in our table. We will always need to do this to cleanup the
    // index, so we might as well do this up front
    final byte[] sourceRow = p.getRow();
    Result r = localTable.get(new Get(sourceRow));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Updating index for row: " + Bytes.toString(sourceRow));
    }

    // we need to check each key-value in the update to see if it matches the others. Generally,
    // this will be the case, but you can add kvs to a mutation that don't all have the timestamp,
    // so we need to manage everything in batches based on timestamp.
    TreeMultimap<Long, KeyValue> timestampMap = createTimestampBatchesFromFamilyMap(p);

    // build the index updates for each group
    Map<Mutation, String> updateMap = new HashMap<Mutation, String>();
    Collection<ColumnGroup> matches = findMatchingGroups(p);

    // we can use a single codec for everything, as long as we apply the updates in timestamp order
    CoveredColumnIndexCodec codec = new CoveredColumnIndexCodec(sourceRow, r);

    // go through each batch of keyvalues and build separate index entries for each
    for (Entry<Long, Collection<KeyValue>> batch : timestampMap.asMap().entrySet()) {
      /*
       * We have to split the work between the cleanup and the update for each group because when we
       * update the current state of the row for the current batch (appending the mutations for the
       * current batch) the next group will see that as the current state, which will can cause the
       * a delete and a put to be created for the next group.
       */
      addMutationsForBatch(updateMap, batch, matches, codec);
    }

    return updateMap;
  }

  @Override
  public Map<Mutation, String> getIndexUpdate(Delete d) throws IOException {
    // if not columns to index, we are done
    if (groups == null || groups.size() == 0) {
      return Collections.emptyMap();
    }

    ensureLocalTable();

    // stores all the return values
    Map<Mutation, String> updateMap = new HashMap<Mutation, String>();

    // get the current state of the row in our table. We will always need to do this to cleanup the
    // index, so we might as well do this up front
    final byte[] sourceRow = d.getRow();
    Result r = localTable.get(new Get(sourceRow));

    // We have to figure out which kind of delete it is, since we need to do different things if its
    // a general (row) delete, versus a delete of just a single column or family
    Map<byte[], List<KeyValue>> families = d.getFamilyMap();

    // Option 1: its a row delete marker, so we just need to delete the most recent state for each
    // group, as of the specified timestamp in the delete
    if (families.size() == 0) {
      // get a consistent view of name
      long now = d.getTimeStamp();
      if (now == HConstants.LATEST_TIMESTAMP) {
        now = EnvironmentEdgeManager.currentTimeMillis();
        // update the delete's idea of 'now' to be consistent with the index
        d.setTimestamp(now);
      }

      // insert a delete for each group since the delete will cover all columns
      for (ColumnGroup group : groups) {
        CoveredColumnIndexCodec codec = new CoveredColumnIndexCodec(sourceRow, r, group);
        byte[] row = codec.getIndexRowKey(now).getFirst();
        Delete indexUpdate = new Delete(row);
        indexUpdate.setTimestamp(now);
        updateMap.put(indexUpdate, group.getTable());
      }

      return updateMap;
    }

    // Option 2: Its actually a bunch single updaets, which can have different timestamps.
    // Therefore, we need to do something similar to the put case and batch by timestamp
    TreeMultimap<Long, KeyValue> batches = createTimestampBatchesFromFamilyMap(d);

    // check the map to see if we are affecting any of the groups
    Collection<ColumnGroup> matches = findMatchingGroups(d);

    // build up the index entries for each group
    CoveredColumnIndexCodec codec = new CoveredColumnIndexCodec(sourceRow, r);
    for (Entry<Long, Collection<KeyValue>> batch : batches.asMap().entrySet()) {
      addMutationsForBatch(updateMap, batch, matches, codec);
    }
    return updateMap;
  }

  /**
   * Batch all the {@link KeyValue}s in a {@link Mutation} by timestamp. Updates any
   * {@link KeyValue} with a timestamp == {@link HConstants#LATEST_TIMESTAMP} to a single value
   * obtained when the method is called.
   * @param m {@link Mutation} from which to extract the {@link KeyValue}s
   * @return map of timestamp to all the keyvalues with the same timestamp. the implict tree sorting
   *         in the returned ensures that batches (when iterating through the keys) will iterate the
   *         kvs in timestamp order
   */
  private TreeMultimap<Long, KeyValue> createTimestampBatchesFromFamilyMap(Mutation m) {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    byte[] nowBytes = Bytes.toBytes(now);
    TreeMultimap<Long, KeyValue> batches =
        TreeMultimap.create(Ordering.natural(), KeyValue.COMPARATOR);
    for (List<KeyValue> kvs : m.getFamilyMap().values()) {
      for (KeyValue kv : kvs) {
        long ts = kv.getTimestamp();
        // override the timestamp to the current time, so the index and primary tables match
        // all the keys with LATEST_TIMESTAMP will then be put into the same batch
        if (ts == HConstants.LATEST_TIMESTAMP) {
          kv.updateLatestStamp(nowBytes);
        }
        batches.put(kv.getTimestamp(), kv);
      }
    }
    return batches;
  }

  /**
   * Find all the {@link ColumnGroup}s that match this {@link Mutation} to the primary table.
   * @param m mutation to match against
   * @return the {@link ColumnGroup}s that should be updated with this {@link Mutation}.
   */
  private Collection<ColumnGroup> findMatchingGroups(Mutation m) {
    // just using pointer hashes here - we don't need to calculate actual equality, just that we
    // don't get dupes
    Set<ColumnGroup> matches = new HashSet<ColumnGroup>();
    for (Entry<byte[], List<KeyValue>> entry : m.getFamilyMap().entrySet()) {
      // early exit if we already know we need to match all the groups
      if (matches.size() == this.groups.size()) {
        return matches;
      }

      // get the keys for this family that we are indexing
      List<KeyValue> kvs = entry.getValue();
      if (kvs == null || kvs.isEmpty()) {
        // should never be the case, but just to be careful
        continue;
      }

      // figure out the groups we need to index
      String family = Bytes.toString(entry.getKey());
      for (ColumnGroup column : groups) {
        if (column.matches(family)) {
          matches.add(column);
        }
      }
    }
    return matches;
  }

  /**
   * @param updateMap
   * @param batch
   */
  private void addMutationsForBatch(Map<Mutation, String> updateMap,
      Entry<Long, Collection<KeyValue>> batch, Collection<ColumnGroup> matches,
      CoveredColumnIndexCodec codec) {
    /*
     * Generally, the current Put will be the most recent thing to be added. In that case, all we
     * need to is issue a delete for the previous index row (the state of the row, without the put
     * applied) at the Put's current timestamp. This gets rid of anything currently in the index for
     * the current state of the row (at the timestamp). If things arrive out of order (we are using
     * custom timestamps) we should always still only see the most recent update in the index, even
     * if we are making a put back in time (out of order). Therefore, we need to issue a delete for
     * the index update but at the next most recent timestam; using batches helps, but we still need
     * to do this iteratively since we need to cleanup the current state first as it could be
     * overwritten by a key-value at the same timestamp
     */

    long ts = batch.getKey();
    // start by getting the cleanup for the current state of the
    for (ColumnGroup group : matches) {
      codec.setGroup(group);
      Delete cleanup = getIndexCleanupForCurrentRow(codec, ts);
      String table = group.getTable();
      if (cleanup != null) {
        updateMap.put(cleanup, table);
      }

    }

    // add the current batch to the map
    codec.addAll(batch.getValue());

    // get the updates to the current index
    for (ColumnGroup group : matches) {
      codec.setGroup(group);
      String table = group.getTable();
      Put indexInsert = codec.getPutToIndex(ts);
      if (indexInsert != null) {
        updateMap.put(indexInsert, table);
      }
    }
  }

  /**
   * Make a delete for the state of the current row, at the given timestamp.
   * @param codec to form the row key
   * @param timestamp
   * @return the delete to apply or <tt>null</tt>, if no {@link Delete} is necessary
   */
  private Delete getIndexCleanupForCurrentRow(CoveredColumnIndexCodec codec, long timestamp) {
    byte[] currentRowkey = codec.getIndexRowKey(timestamp).getFirst();
    // no previous state for the current group, so don't create a delete
    if (CoveredColumnIndexCodec.checkRowKeyForAllNulls(currentRowkey)) {
      return null;
    }

    Delete cleanup = new Delete(currentRowkey);
    cleanup.setTimestamp(timestamp);

    return cleanup;
  }

  /**
   * Exposed for testing! Set the local table that should be used to lookup the state of the current
   * row.
   * @param table
   */
  public void setTableForTesting(HTableInterface table) {
    this.localTable = table;
  }
}