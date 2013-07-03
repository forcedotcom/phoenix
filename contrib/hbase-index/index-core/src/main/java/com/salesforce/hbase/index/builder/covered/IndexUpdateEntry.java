package com.salesforce.hbase.index.builder.covered;

import java.util.List;

/**
 * Simple tuple class for storing the results of building the update to an index from the state of
 * primary table.
 */
class IndexUpdateEntry {
  byte[] rowKey;
  long nextNewestTs;
  List<CoveredColumn> columns;

  public IndexUpdateEntry(byte[] value, long nextNewestTs, List<CoveredColumn> columns) {
    this.rowKey = value;
    this.nextNewestTs = nextNewestTs;
    this.columns = columns;
  }
}