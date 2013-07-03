package com.salesforce.hbase.index.builder.covered;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

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
  public Collection<Pair<Mutation, String>> getIndexUpdate(Put p) throws IOException {
    // if not columns to index, we are done
    if (groups == null || groups.size() == 0) {
      return Collections.emptyList();
    }

    ensureLocalTable();

    // get the current state of the row in our table. We will always need to do this to cleanup the
    // index, so we might as well do this up front
    final byte[] sourceRow = p.getRow();
    Result r = getCurrentRow(sourceRow);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Updating index for row: " + Bytes.toString(sourceRow));
    }

    // build the index updates for each group
    List<Pair<Mutation, String>> updateMap = new ArrayList<Pair<Mutation, String>>();

    // we need to check each key-value in the update to see if it matches the others. Generally,
    // this will be the case, but you can add kvs to a mutation that don't all have the timestamp,
    // so we need to manage everything in batches based on timestamp.
    TreeMultimap<Long, KeyValue> batches = createTimestampBatchesFromFamilyMap(p);

    // we can use a single codec for everything, as long as we apply the updates in timestamp order
    CoveredColumnIndexCodec codec = new CoveredColumnIndexCodec(sourceRow, r);

    // go through each batch of keyvalues and build separate index entries for each
    Set<ColumnGroup> matches = new HashSet<ColumnGroup>();
    for (Entry<Long, Collection<KeyValue>> batch : batches.asMap().entrySet()) {
      // check to see if this batch is affecting any of the groups
      matches.clear();
      getAllMatchingGroups(matches, batch.getValue());
      /*
       * We have to split the work between the cleanup and the update for each group because when we
       * update the current state of the row for the current batch (appending the mutations for the
       * current batch) the next group will see that as the current state, which will can cause the
       * a delete and a put to be created for the next group.
       */
      addMutationsForBatch(updateMap, batch, matches, codec);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Found index updates for Put: " + updateMap);
    }
    return updateMap;
  }

  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdate(Delete d) throws IOException {
    // if not columns to index, we are done
    if (groups == null || groups.size() == 0) {
      return Collections.emptyList();
    }

    ensureLocalTable();

    // get the current state of the row in our table. We will always need to do this to cleanup the
    // index, so we might as well do this up front
    final byte[] sourceRow = d.getRow();
    Result r = getCurrentRow(sourceRow);

    // stores all the return values
    List<Pair<Mutation, String>> updateMap = new ArrayList<Pair<Mutation, String>>();

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
        byte[] row = codec.getIndexRowKey(now).rowKey;
        Delete indexUpdate = new Delete(row);
        indexUpdate.setTimestamp(now);
        updateMap.add(new Pair<Mutation, String>(indexUpdate, group.getTable()));
      }

      return updateMap;
    }

    // Option 2: Its actually a bunch single updates, which can have different timestamps.
    // Therefore, we need to do something similar to the put case and batch by timestamp
    TreeMultimap<Long, KeyValue> batches = createTimestampBatchesFromFamilyMap(d);

    // build up the index entries for each group
    CoveredColumnIndexCodec codec = new CoveredColumnIndexCodec(sourceRow, r);
    Set<ColumnGroup> matches = new HashSet<ColumnGroup>();
    for (Entry<Long, Collection<KeyValue>> batch : batches.asMap().entrySet()) {
      // check to see if this batch is affecting any of the groups
      matches.clear();
      getAllMatchingGroups(matches, batch.getValue());

      // then find the mutations that match for this batch
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
    Set<ColumnGroup> matches = new HashSet<ColumnGroup>();
    for (Entry<byte[], List<KeyValue>> family : m.getFamilyMap().entrySet()) {
      // no kvs being added, means we can ignore this family
      if (family.getValue() == null || family.getValue().size() == 0) {
        continue;
      }

      getAllMatchingGroups(matches, family.getValue());

    }
    return matches;
  }

  /**
   * @param matches
   * @param value
   */
  private void getAllMatchingGroups(Set<ColumnGroup> matches, Iterable<KeyValue> values) {
    for (KeyValue kv : values) {
      // find any groups that we need to index
      findMatchingGroups(matches, kv);
      // done if we are matching all groups
      if (matches.size() == this.groups.size()) {
        return;
      }
    }

  }

  /**
   * Add any matching {@link ColumnGroup} that index the passed family to the set of matches.
   * @param matches to add new {@link ColumnGroup}s
   * @param kvs
   */
  private void findMatchingGroups(Set<ColumnGroup> matches, KeyValue kv) {
    for (ColumnGroup column : groups) {
      if (column.matches(kv.getFamily(), kv.getQualifier())) matches.add(column);
    }
  }

  /**
   * @param updateMap
   * @param batch
   */
  private void addMutationsForBatch(Collection<Pair<Mutation, String>> updateMap,
      Entry<Long, Collection<KeyValue>> batch, Collection<ColumnGroup> matches,
      CoveredColumnIndexCodec codec) {
    /*
     * Generally, the current update will be the most recent thing to be added. In that case, all we
     * need to is issue a delete for the previous index row (the state of the row, without the
     * update applied) at the current timestamp. This gets rid of anything currently in the index
     * for the current state of the row (at the timestamp).
     *
     * If things arrive out of order (we are using custom timestamps) we should still see the index
     * in the correct order (assuming we scan after the out-of-order update in finished). Therefore,
     * we when we aren't the most recent update to the index, we need to delete the state at the
     * current timestamp (similar to above), but also issue a delete for the added for at the next
     * newest timestamp of any of the columns in the update; we need to cleanup the insert so it
     * looks like it was also deleted at that newer timestamp. see the most recent update in the
     * index, even if we are making a put back in time (out of order).
     */

    // start by getting the cleanup for the current state of the
    long ts = batch.getKey();
    for (ColumnGroup group : matches) {
      codec.setGroup(group);
      Delete cleanup = getIndexCleanupForCurrentRow(codec, ts);
      if (cleanup != null) {
        String table = group.getTable();
        updateMap.add(new Pair<Mutation, String>(cleanup, table));
      }

    }

    // add the current batch to the map
    codec.addAll(batch.getValue());

    // get the updates to the current index
    for (ColumnGroup group : matches) {
      codec.setGroup(group);
      Collection<Mutation> indexInsert = codec.getIndexUpdate(ts);
      if (!indexInsert.isEmpty()) {
        String table = group.getTable();
        for (Mutation m : indexInsert) {
          updateMap.add(new Pair<Mutation, String>(m, table));
        }
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
    byte[] currentRowkey = codec.getIndexRowKey(timestamp).rowKey;
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

  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdateForFilteredRows(Collection<KeyValue> filtered)
      throws IOException {
    ensureLocalTable();

    // stores all the return values
    List<Pair<Mutation, String>> updateMap = new ArrayList<Pair<Mutation, String>>(filtered.size());
    // batch the updates by row to make life easier and ordered
    TreeMultimap<byte[], KeyValue> batches = batchByRow(filtered);

    for (Entry<byte[], Collection<KeyValue>> batch : batches.asMap().entrySet()) {
      // get the current state of the row in our table
      final byte[] sourceRow = batch.getKey();
      // have to do a raw scan so we see everything, which includes things that haven't been
      // filtered out yet, but aren't generally client visible. We don't need to do this in the
      // other update cases because they are only concerned wi
      Result r = getCurrentRow(sourceRow);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Updating index for row: " + Bytes.toString(sourceRow));
      }

      // build up the index entries for each kv
      CoveredColumnIndexCodec codec = new CoveredColumnIndexCodec(sourceRow, r);
      // find all the matching groups that we need to update
      Set<ColumnGroup> matches = new HashSet<ColumnGroup>();
      for (KeyValue kv : batch.getValue()) {
        findMatchingGroups(matches, kv);
        // didn't find a match, so go to the next kv
        if (matches.size() == 0) {
          continue;
        }

        // for each matching group, we need to get the delete that index entry
        for (ColumnGroup group : matches) {
          // the kv here will definitely have a valid timestamp, since it came from the memstore, so
          // we don't need to do any of the timestamp adjustment we do above.
          codec.setGroup(group);
          Delete cleanup = getIndexCleanupForCurrentRow(codec, kv.getTimestamp());
          if (cleanup != null) {
            updateMap.add(new Pair<Mutation, String>(cleanup, group.getTable()));
          }
        }

        matches.clear();
      }
    }
    return updateMap;
  }

  /**
   * @param sourceRow row key to extract
   * @return the full state of the given row. Includes all current versions (even if they are not
   *         usually visible to the client (unless they are also doing a raw scan)).
   */
  private Result getCurrentRow(byte[] sourceRow) throws IOException {
    Scan s = new Scan(sourceRow, sourceRow);
    s.setRaw(true);
    s.setMaxVersions();
    ResultScanner results = localTable.getScanner(s);
    Result r = results.next();
    assert results.next() == null : "Got more than one result when scanning"
        + " a single row in the primary table!";
    results.close();
    return r;
  }

  /**
   * @param filtered
   * @return
   */
  private TreeMultimap<byte[], KeyValue> batchByRow(Collection<KeyValue> filtered) {
    TreeMultimap<byte[], KeyValue> batches =
        TreeMultimap.create(Bytes.BYTES_COMPARATOR, KeyValue.COMPARATOR);

    for (KeyValue kv : filtered) {
      batches.put(kv.getRow(), kv);
    }

    return batches;
  }
}