package com.salesforce.hbase.index.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.io.Writable;

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
public class HTableInterfaceReference implements Writable {

  private ImmutableBytesPtr tableName;
  private HTableInterface table;
  private HTableFactory factory;

  /**
   * For use with {@link #readFields(DataInput)}. A {@link HTableFactory} must be passed either to
   * {@link #setFactory(HTableFactory)} before resolving an HTableInterface or
   * {@link #getTable(HTableFactory)} when resolving an {@link HTableInterface}
   */
  public HTableInterfaceReference() {
  }

  public HTableInterfaceReference(ImmutableBytesPtr tableName) {
    this.tableName = tableName;
  }

  public HTableInterfaceReference(ImmutableBytesPtr tableName, HTableFactory factory) {
    this(tableName);
    this.factory = factory;
  }

  public void setFactory(HTableFactory e) {
    this.factory = e;
  }

  public HTableInterface getTable(HTableFactory e) throws IOException {
    if (this.table == null) {
      this.table = e.getTable(this.tableName.copyBytes());
    }
    return this.table;
  }

  /**
   * @return get the referenced table, if one has been stored
   * @throws IOException if we are creating a new table (first instance of request) and it cannot be
   *           reached
   */
  public HTableInterface getTable() throws IOException {
    return this.getTable(this.factory);
  }

  public String getTableName() {
    return this.tableName.toString();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.tableName = new ImmutableBytesPtr();
    this.tableName.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    this.tableName.write(out);
  }

  @Override
  public int hashCode() {
    return this.tableName.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    return o == null ? false : this.hashCode() == o.hashCode();
  }
}