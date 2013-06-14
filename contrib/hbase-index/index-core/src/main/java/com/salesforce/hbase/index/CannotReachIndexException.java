package com.salesforce.hbase.index;

import org.apache.hadoop.hbase.client.Mutation;

/**
 * Exception thrown if we cannot successfully write to an index table.
 */
@SuppressWarnings("serial")
public class CannotReachIndexException extends Exception {

  public CannotReachIndexException(String targetTableName, Mutation m, Exception cause) {
    super("Cannot reach index table " + targetTableName + " to update index for edit: " + m, cause);
  }
}
