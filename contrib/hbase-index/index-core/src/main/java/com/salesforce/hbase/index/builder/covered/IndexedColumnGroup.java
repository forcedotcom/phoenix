package com.salesforce.hbase.index.builder.covered;

import java.util.List;

/**
 * Group of columns that were requested to build an index
 */
public interface IndexedColumnGroup {

  public List<ColumnReference> getColumns();
}
