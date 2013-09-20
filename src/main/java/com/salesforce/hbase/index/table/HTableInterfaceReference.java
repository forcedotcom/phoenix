package com.salesforce.hbase.index.table;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.hbase.index.util.ImmutableBytesPtr;

/**
 * Reference to an HTableInterface that only gets the underlying {@link HTableInterface} from the
 * {@link CoprocessorEnvironment} on calls to {@link #getTable()}. Until calling {@link #getTable()}
 * , this class just contains the name of the table and an optional {@link HTableFactory}. After
 * calling {@link #getTable()}, <tt>this</tt> holds a reference to that {@link HTableInterface},
 * even if that table is closed.
 * <p>
 * Whenever calling {@link #getTable()}, an internal reference counter is incremented. Similarly,
 * the reference count is decremented by calling {@link #close()}. The underlying table, if
 * previously resolved, will be closed on calls to {@link #close()} only if the underlying reference
 * count is zero.
 * <p>
 * This class is not thread-safe when resolving the reference to the {@link HTableInterface} -
 * multi-threaded usage must employ external locking to ensure that multiple {@link HTableInterface}
 * s are not resolved.
 */
public class HTableInterfaceReference {

  private ImmutableBytesPtr tableName;


  public HTableInterfaceReference(ImmutableBytesPtr tableName) {
    this.tableName = tableName;
  }

  public ImmutableBytesPtr get() {
    return this.tableName;
  }

  public String getTableName() {
    return this.tableName.toString();
  }

  @Override
  public int hashCode() {
      return tableName.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      HTableInterfaceReference other = (HTableInterfaceReference)obj;
      return tableName.equals(other.tableName);
  }

  @Override
  public String toString() {
    return Bytes.toString(this.tableName.get());
  }
}