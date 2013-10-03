package com.salesforce.hbase.index.scanner;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;

/**
 * Scan the primary table. This is similar to HBase's scanner, but ensures that you will never see
 * deleted columns/rows
 */
public interface Scanner extends Closeable {

  /**
   * @return the next keyvalue in the scanner or <tt>null</tt> if there is no next {@link KeyValue}
   * @throws IOException if there is an underlying error reading the data
   */
  public KeyValue next() throws IOException;

  /**
   * Seek to immediately before the given {@link KeyValue}. If that exact {@link KeyValue} is
   * present in <tt>this</tt>, it will be returned by the next call to {@link #next()}. Otherwise,
   * returns the next {@link KeyValue} after the seeked {@link KeyValue}.
   * @param next {@link KeyValue} to seek to. Doesn't need to already be present in <tt>this</tt>
   * @return <tt>true</tt> if there are values left in <tt>this</tt>, <tt>false</tt> otherwise
   * @throws IOException if there is an error reading the underlying data.
   */
  public boolean seek(KeyValue next) throws IOException;

  /**
   * Read the {@link KeyValue} at the top of <tt>this</tt> without 'popping' it off the top of the
   * scanner.
   * @return the next {@link KeyValue} or <tt>null</tt> if there are no more values in <tt>this</tt>
   * @throws IOException if there is an error reading the underlying data.
   */
  public KeyValue peek() throws IOException;
}