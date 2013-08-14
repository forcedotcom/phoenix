/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *     Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *     Neither the name of Salesforce.com nor the names of its contributors may 
 *     be used to endorse or promote products derived from this software without 
 *     specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.hbase.index.builder.covered;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


/**
 * Simple POJO for tracking a bunch of column references and the next-newest timestamp for those
 * columns
 * <p>
 * Two {@link ColumnTracker}s are considered equal if they track the same columns, even if their
 * timestamps are different.
 */
public class ColumnTracker implements IndexedColumnGroup {

  public static final long NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP = Long.MAX_VALUE;
  private final List<ColumnReference> columns;
  private long ts = NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP;

  public ColumnTracker(Collection<? extends ColumnReference> columns) {
    this.columns = new ArrayList<ColumnReference>(columns);
    // sort the columns
    Collections.sort(this.columns);
  }

  /**
   * Set the current timestamp, only if the passed timestamp is strictly less than the currently
   * stored timestamp
   * @param ts the timestmap to potentially store.
   * @return the currently stored timestamp.
   */
  public long setTs(long ts) {
    this.ts = this.ts > ts ? ts : this.ts;
    return this.ts;
  }

  public long getTS() {
    return this.ts;
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (ColumnReference ref : columns) {
      hash += ref.hashCode();
    }
    return hash;
  }

  @Override
  public boolean equals(Object o){
    if(!(o instanceof ColumnTracker)){
      return false;
    }
    ColumnTracker other = (ColumnTracker)o;
    if (other.columns.size() != columns.size()) {
      return false;
    }

    // check each column to see if they match
    for (int i = 0; i < columns.size(); i++) {
      if (!columns.get(i).equals(other.columns.get(i))) {
        return false;
      }
    }

    return true;
  }

  @Override
  public List<ColumnReference> getColumns() {
    return this.columns;
  }

  /**
   * @return if this set of columns has seen a column with a timestamp newer than the requested
   *         timestamp
   */
  public boolean hasNewerTimestamps() {
    return this.ts < NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP;
  }
}