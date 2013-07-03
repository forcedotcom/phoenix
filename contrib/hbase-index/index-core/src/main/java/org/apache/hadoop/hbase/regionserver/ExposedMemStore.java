package org.apache.hadoop.hbase.regionserver;

import java.rmi.UnexpectedException;
import java.util.List;
import java.util.SortedSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;

/**
 * A {@link MemStore} that exposes all the package-protected methods.
 * <p>
 * If used improperly, this class can completely hose the memstore - its only for expert usage.
 * <p>
 * @see MemStore
 */
public class ExposedMemStore extends MemStore {

  public ExposedMemStore(Configuration conf, KVComparator comparator) {
    super(conf, comparator);
  }

  @Override
  public void dump() {
    super.dump();
  }

  @Override
  void snapshot() {
    super.snapshot();
  }

  @Override
  public KeyValueSkipListSet getSnapshot() {
    return super.getSnapshot();
  }

  @Override
  public void clearSnapshot(SortedSet<KeyValue> ss) throws UnexpectedException {
    super.clearSnapshot(ss);
  }


  @Override
  public long add(KeyValue kv) {
    return super.add(kv);
  }

  @Override
  public void rollback(KeyValue kv) {
    super.rollback(kv);
  }

  /**
   * Use {@link #add(KeyValue)} instead! This method is not used in the usual HBase codepaths for
   * adding deletes
   */
  @Override
  public long delete(KeyValue delete) {
    return super.delete(delete);
  }

  @Override
  public KeyValue getNextRow(KeyValue kv) {
    return getNextRow(kv);
  }

  @Override
  void getRowKeyAtOrBefore(GetClosestRowBeforeTracker state) {
    super.getRowKeyAtOrBefore(state);
  }

  @Override
  public List<KeyValueScanner> getScanners() {
    return super.getScanners();
  }

  @Override
  public long heapSizeChange(KeyValue kv, boolean notpresent) {
    return super.heapSizeChange(kv, notpresent);
  }

}
