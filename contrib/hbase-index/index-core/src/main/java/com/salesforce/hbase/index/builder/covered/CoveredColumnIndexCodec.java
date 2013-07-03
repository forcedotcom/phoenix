package com.salesforce.hbase.index.builder.covered;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.regionserver.ExposedMemStore;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.salesforce.hbase.index.builder.covered.util.FamilyOnlyFilter;
import com.salesforce.hbase.index.builder.covered.util.FilteredKeyValueScanner;

/**
 * Handle serialization to/from a column-covered index.
 * @see CoveredColumnIndexer
 */
public class CoveredColumnIndexCodec {
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  private static final long NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP = Long.MAX_VALUE;
  public static final byte[] INDEX_ROW_COLUMN_FAMILY = Bytes.toBytes("INDEXED_COLUMNS");
  private static final Configuration conf = HBaseConfiguration.create();
  {
    // keep it all on the heap - hopefully this should be a bit faster and shouldn't need to grow
    // very large as we are just handling a single row.
    conf.setBoolean("hbase.hregion.memstore.mslab.enabled", false);
  }

  private ColumnGroup group;
  private ExposedMemStore memstore;
  private byte[] pk;

  public CoveredColumnIndexCodec(byte[] primaryKey, Result currentRow, ColumnGroup group) {
    this.pk =primaryKey;
    this.group = group;
    this.memstore = new ExposedMemStore(conf, KeyValue.COMPARATOR);
    if (currentRow != null && !currentRow.isEmpty()) {
      addAll(currentRow.list());
    }
  }
  
  /**
   * Setup the codec on the specified row for the current group of columns
   * @param currentRow must not return <tt>null</tt> for {@link Result#getRow()} - its expected to
   *          be the {@link Result} from reading an existing row. If you are not sure, you should
   *          call the constructor which specifies the primary key.
   * @param group columns for which we want to build an index codec
   */
  public CoveredColumnIndexCodec(Result currentRow, ColumnGroup group) {
    this(currentRow.getRow(), currentRow, group);
  }

  /**
   * Create a codec on the given primary row with the given backing current state. If you use this
   * constructor, you must specify a group via {@link #setGroup(ColumnGroup)} before calling any
   * other instance methods.
   * @param sourceRow primary key of the row in questions
   * @param r current state of the current row (can be empty)
   */
  public CoveredColumnIndexCodec(byte[] sourceRow, Result r) {
    this(sourceRow, r, null);
  }

  public void setGroup(ColumnGroup group) {
    this.group = group;
  }

  /**
   * Add all the {@link KeyValue}s in the list to the memstore. This is just a small utility method
   * around {@link ExposedMemStore#add(KeyValue)} to make it easier to deal with batches of
   * {@link KeyValue}s.
   * @param list keyvalues to add
   */
  public void addAll(Iterable<KeyValue> list) {
    for (KeyValue kv : list) {
      this.memstore.add(kv);
    }
  }

  /**
   * Add a {@link Mutation} to the values stored for the current row
   * @param pendingUpdate update to apply
   */
  public void addUpdateForTesting(Mutation pendingUpdate) {
    for (Map.Entry<byte[], List<KeyValue>> e : pendingUpdate.getFamilyMap().entrySet()) {
      List<KeyValue> edits = e.getValue();
      addAll(edits);
    }
  }

  /**
   * Get the most recent value for each column group, less than the specified timestamps, in the
   * order of the columns stored in the group and then build them into a single byte array to use as
   * the row key for an index update for the column group.
   * @param timestamp timestamp at which to extract the state of the current row. No
   *          {@link KeyValue} for the row newer than the timestamp is included (so inclusive up to
   *          and including this time).
   * @return the row key and the corresponding list of {@link CoveredColumn}s to the position of
   *         their value in the row key
   */
  public IndexUpdateEntry getIndexRowKey(long timestamp) {
    int length = 0;
    List<byte[]> topValues = new ArrayList<byte[]>();
    // columns that match against values, as we find them
    List<CoveredColumn> columns = new ArrayList<CoveredColumn>();
    long nextNewestTs = NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP;
    // go through each group,in order, to find the matching value (or none)
    for (CoveredColumn column : group) {
      final byte[] family = Bytes.toBytes(column.family);
      // filter families that aren't what we are looking for
      FamilyOnlyFilter familyFilter = new FamilyOnlyFilter(new BinaryComparator(family));
      KeyValueScanner scanner = new FilteredKeyValueScanner(familyFilter, this.memstore);

      /*
       * now we have two possibilities. (1) the CoveredColumn has a specific column - this is the
       * easier one, we can just seek down to that keyvalue and then pull the next one out. If there
       * aren't any keys, we just inject a null value and point at the coveredcolumn, or (2) it
       * includes all qualifiers - we need to match all column families, but only inject the null
       * mapping if its the first key
       */

      Collection<ColumnEntry> entries = getNextEntries(column, scanner, timestamp);
      for (ColumnEntry entry : entries) {
        topValues.add(entry.value);
        columns.add(entry.column);
        if (entry.prevTs < nextNewestTs) {
          assert entry.prevTs > timestamp : "Got a previous timestamp that is <= target timestamp!";
          nextNewestTs = entry.prevTs;
        }
        length += entry.value.length;
      }

    }

    byte[] key = CoveredColumnIndexCodec.composeRowKey(pk, length, topValues);
    return new IndexUpdateEntry(key, nextNewestTs, columns);
  }

  private class ColumnEntry {
    byte[] value;
    long prevTs = Long.MAX_VALUE;
    final CoveredColumn column;

    /**
     * @param column
     */
    public ColumnEntry(CoveredColumn column) {
      this.column = column;
    }
  }

  /**
   * Find the next batch of entries from the scanner for the given column. The scanner is expected
   * to to filter out rows that do not match the passed column.
   * @param column
   * @param scanner
   * @param timestamp
   * @return
   */
  private Collection<ColumnEntry> getNextEntries(CoveredColumn column, KeyValueScanner scanner,
      long timestamp) {
    Collection<ColumnEntry> entries = new ArrayList<ColumnEntry>();
    // key to seek. We can only seek to the family because we may have a family delete on top that
    // covers everything below it, which we would miss if we seek right to the family:qualifier
    KeyValue first = KeyValue.createFirstOnRow(pk, Bytes.toBytes(column.family), null);
    ColumnEntry nextEntry = new ColumnEntry(column);
    try {
      // seek to right before the key in the scanner
      byte[] value = EMPTY_BYTE_ARRAY;
      // no values, so add a null against the entire CoveredColumn
      if (!scanner.seek(first)) {
        nextEntry.value = value;
        return Collections.singleton(nextEntry);
      }

      byte[] prevCol = null;
      // not null because seek() returned true
      KeyValue next = scanner.next();
      KeyValue coveringDelete = null;
      boolean done = false;
      do {
        byte[] qual = next.getQualifier();
        boolean columnMatches = column.matchesQualifier(qual);

        // first check the timestamp to figure out if it even matches the given row
        if (next.getTimestamp() > timestamp) {
          // newer timestamp, so we need to update the entry's seen ts
          nextEntry.prevTs = next.getTimestamp();
          // we skip this entry because its newer than the index update we want to make
          continue;
        }

        // at this point, we are sure the entry has our target timestamp or is older

        /*
         * check for a delete to see if we can just replace this with a single delete; if its a
         * family delete, then we have deleted all columns and are definitely done with this
         * coveredcolumn. This works because deletes will always sort first, so we can be sure that
         * if we see a delete, we can skip everything else.
         */
        if (next.isDeleteFamily()) {
          // count it as a non-match for all rows, so we add a single null for the entire column
          value = EMPTY_BYTE_ARRAY;
          break;
        }
        // its not a family delete and it matches the target column and is <= the target timestamp
        else if (columnMatches) {
          switch (KeyValue.Type.codeToType(next.getType())) {
          case DeleteColumn:
            // if its the delete of the entire column, then there are no more possible columns it
            // could be and we are done.
            value = EMPTY_BYTE_ARRAY;
            break;
          case Delete:
            // we are just deleting the single column value at this point.
            // therefore we just skip this entry and go onto the next one. The only caveat is that
            // we should still cover the next entry if this delete applies to the next entry, so we
            // have to keep around a reference to the KV to compare against the next valid entry
            coveringDelete = next;
            value = EMPTY_BYTE_ARRAY;
            continue;
          default:
            // its definitely not a DeleteFamily, since we checked that already, so its a valid
            // entry and we can just add it, as long as it isn't directly covered by the previous
            // delete
            if (coveringDelete != null
                && coveringDelete.matchingColumn(next.getFamily(), next.getQualifier())) {
              // check to see if the match applies directly to this version
              if (coveringDelete.getTimestamp() == next.getTimestamp()) {
                // this covers this exact key. Therefore, we can skip this key AND discard the
                // covering delete because it must only match this single version
                coveringDelete = null;
                value = EMPTY_BYTE_ARRAY;
                continue;
              }
            } else {
              // delete no longer applies, we are onto a new cf/cq
              coveringDelete = null;
              value = next.getValue();
            }

          }

          done = true;
          // we are covering a single column, then we are done.
          if (column.allColumns()) {
            /*
             * we are matching all columns, so we need to make sure that this is a new qualifier. If
             * its a new qualifier, then we want to add that value, but otherwise we can skip ahead
             * to the next key.
             */
            if (prevCol == null || !Bytes.equals(prevCol, qual)) {
              prevCol = qual;
            } else {
              continue;
            }
          }
        }

        // add the array to the list
        nextEntry.value = value;
        entries.add(nextEntry);
        //create a new entry, for the next iteration
        nextEntry = new ColumnEntry(column);
        // only go around again if there is more data and we are matching against all column
      } while ((!done || column.allColumns()) && (next = scanner.next()) != null);

      // we never found a match, so we need to add an empty entry
      if (!done) {
        nextEntry.value = value;
        entries.add(nextEntry);
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return entries;
  }

  /**
   * Get the index updates that should be made to match the current update. If the current update is
   * the newest element in the index (no newer timestamps), then we just need to return a
   * {@link Put}. However, if the updates were 'out of order' (there are newer timestamps for any
   * involved column) we also return a delete to match our index update, but at the next newest
   * timestamp (this update would have been deleted by the next entry).
   * <p>
   * There is a bit of conflation in the use of this method (creates the {@link Put} <i>and</i> the
   * {@link Delete} to bring the index up-to-date, rather than just one or the other), but this is
   * to minimize the number of times we need to loop through the state of the current row. Moving
   * the {@link Delete} generation out of this method requires at least one more pass (the simple
   * implementation being <i>per index column</i>) to verify the timestamps - work we can save by
   * combining the two here. Unfortunately, leaking the abstraction here is kind of the nature of
   * optimizations like this.
   * <p>
   * The created {@link Put} to update the index is based on the state of the row at the given
   * timestamp. Only the most recent version (up to the timestamp) is included for each column in
   * the current {@link ColumnGroup}.
   * <p>
   * Each {@link CoveredColumn} is added to a {@link Put} under a single column family. Each value
   * stored in the key is matched to a column group - value 1 matches family:qualfier 1. This holds
   * true even if the {@link CoveredColumn} matches all columns in the family.
   * <p>
   * Columns are added as:
   * 
   * <pre>
   * &lt{@value CoveredColumnIndexCodec#INDEX_ROW_COLUMN_FAMILY}&gt | &lti&gt[covered column family]:[covered column qualifier] | &lttimestamp&gt | <tt>null</tt>
   * </pre>
   * 
   * where "i" is the integer index matching the index of the value in the row key.
   * <p>
   * The cleaning delete (if its necessary) is designed exactly the same way, except it is an
   * individual delete marker for each column in the corresponding put at the next most recent
   * timestamp (the next newest/largest timestamp on any of the involved columns, when compared to
   * the passed timestamp).
   * @param timestamp time (up to and including) of the current row to extract.
   * @return the put to add to the index table or <tt>null</tt> if no update is necessary
   */
  public Collection<Mutation> getIndexUpdate(long timestamp) {
    IndexUpdateEntry indexRow = this.getIndexRowKey(timestamp);
    byte[] rowKey = indexRow.rowKey;

    // no update to the table, we are done
    if (CoveredColumnIndexCodec.checkRowKeyForAllNulls(rowKey)) {
      return Collections.emptySet();
    }
    
    Put indexInsert = new Put(rowKey);
    addColumnsToPut(indexInsert, indexRow, timestamp);
    
    // if the next newest timestamp is newer than our timestamp there are entries in the table for
    // the index rows that affect this index entry, so we need to delete the Put, but at the newer
    // timestamp
    Delete d = null;
    if (indexRow.nextNewestTs > timestamp
        && indexRow.nextNewestTs != CoveredColumnIndexCodec.NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP) {
      d = new Delete(rowKey);
      d.setTimestamp(indexRow.nextNewestTs);
    }

    return d == null? Collections.singleton((Mutation)indexInsert) : Arrays.asList((Mutation)indexInsert, (Mutation)d);
  }

  private static void addColumnsToPut(Put indexInsert, IndexUpdateEntry columns,
      long timestamp) {
    // add each of the corresponding families to the put
    int count = 0;
    for (CoveredColumn column : columns.columns) {
      indexInsert.add(INDEX_ROW_COLUMN_FAMILY,
        ArrayUtils.addAll(Bytes.toBytes(count++), toIndexQualifier(column)), timestamp, null);
    }
  }

  private static byte[] toIndexQualifier(CoveredColumn column) {
    return ArrayUtils.addAll(Bytes.toBytes(column.family + CoveredColumn.SEPARATOR),
      column.qualifier);
  }

  /**
   * Essentially a short-cut from building a {@link Put}.
   * @param pk row key
   * @param timestamp timestamp of all the keyvalues
   * @param values expected value--column pair
   * @return a keyvalues that the index contains for a given row at a timestamp with the given value
   *         -- column pairs.
   */
  public static List<KeyValue> getIndexKeyValueForTesting(byte[] pk, long timestamp,
      List<Pair<byte[], CoveredColumn>> values) {

    int length = 0;
    List<byte[]> firsts = new ArrayList<byte[]>(values.size());
    List<CoveredColumn> columns = new ArrayList<CoveredColumn>(values.size());
    for (Pair<byte[], CoveredColumn> value : values) {
      firsts.add(value.getFirst());
      length += value.getFirst().length;
      columns.add(value.getSecond());
    }

    byte[] rowKey = composeRowKey(pk, length, firsts);
    Put p = new Put(rowKey);
    addColumnsToPut(p, new IndexUpdateEntry(null, Long.MAX_VALUE, columns), timestamp);
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    for (Entry<byte[], List<KeyValue>> entry : p.getFamilyMap().entrySet()) {
      kvs.addAll(entry.getValue());
    }

    return kvs;
  }

  /**
   * Compose the final index row key.
   * <p>
   * This is faster than adding each value independently as we can just build a single a array and
   * copy everything over once.
   * @param pk primary key of the original row
   * @param length total number of bytes of all the values that should be added
   * @param values to use when building the key
   * @return
   */
  static byte[] composeRowKey(byte[] pk, int length, List<byte[]> values) {
    // now build up expected row key, each of the values, in order, followed by the PK and then some
    // info about lengths so we can deserialize each value
    byte[] output = new byte[length + pk.length];
    int pos = 0;
    int[] lengths = new int[values.size()];
    int i = 0;
    for (byte[] v : values) {
      System.arraycopy(v, 0, output, pos, v.length);
      lengths[i++] = v.length;
      pos += v.length;
    }
  
    // add the primary key to the end of the row key
    System.arraycopy(pk, 0, output, pos, pk.length);
  
    // add the lengths as suffixes so we can deserialize the elements again
    for (int l : lengths) {
      output = ArrayUtils.addAll(output, Bytes.toBytes(l));
    }
  
    // and the last integer is the number of values
    return ArrayUtils.addAll(output, Bytes.toBytes(values.size()));
  }

  /**
   * Get the values for each the columns that were stored in the row key from calls to
   * {@link #getIndexUpdate(long)}in the order they were stored.
   * @param bytes bytes that were written by this codec
   * @return the list of values for the columns
   */
  public static List<byte[]> getValues(byte[] bytes) {
    // get the total number of keys in the bytes
    int keyCount = CoveredColumnIndexCodec.getPreviousInteger(bytes, bytes.length);
    List<byte[]> keys = new ArrayList<byte[]>(keyCount);
    int[] lengths = new int[keyCount];
    int lengthPos = keyCount - 1;
    int pos = bytes.length - Bytes.SIZEOF_INT;
    // figure out the length of each key
    for (int i = 0; i < keyCount; i++) {
      lengths[lengthPos--] = CoveredColumnIndexCodec.getPreviousInteger(bytes, pos);
      pos -= Bytes.SIZEOF_INT;
    }

    int current = 0;
    for (int length : lengths) {
      byte[] key = Arrays.copyOfRange(bytes, current, current + length);
      keys.add(key);
      current += length;
    }
    
    return keys;
  }

  /**
   * Check to see if an row key just contains a list of null values.
   * @param bytes row key to examine
   * @return <tt>true</tt> if all the values are zero-length, <tt>false</tt> otherwise
   */
  public static boolean checkRowKeyForAllNulls(byte[] bytes) {
    int keyCount = CoveredColumnIndexCodec.getPreviousInteger(bytes, bytes.length);
    int pos = bytes.length - Bytes.SIZEOF_INT;
    for (int i = 0; i < keyCount; i++) {
      int next = CoveredColumnIndexCodec.getPreviousInteger(bytes, pos);
      if (next > 0) {
        return false;
      }
      pos -= Bytes.SIZEOF_INT;
    }
  
    return true;
  }

  /**
   * Read an integer from the preceding {@value Bytes#SIZEOF_INT} bytes
   * @param bytes array to read from
   * @param start start point, backwards from which to read. For example, if specifying "25", we
   *          would try to read an integer from 21 -> 25
   * @return an integer from the proceeding {@value Bytes#SIZEOF_INT} bytes, if it exists.
   */
  private static int getPreviousInteger(byte[] bytes, int start) {
    return Bytes.toInt(bytes, start - Bytes.SIZEOF_INT);
  }
}