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
package com.salesforce.hbase.index.covered;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.hbase.index.covered.update.ColumnTracker;

/**
 * Update to make to the index table.
 */
public class IndexUpdate {
  Mutation update;
  byte[] tableName;
  ColumnTracker columns;

  IndexUpdate(ColumnTracker tracker) {
    this.columns = tracker;
  }

  public void setUpdate(Mutation p) {
    this.update = p;
  }

  public void setTable(byte[] tableName) {
    this.tableName = tableName;
  }

  public Mutation getUpdate() {
    return update;
  }

  public byte[] getTableName() {
    return tableName;
  }

  public ColumnTracker getIndexedColumns() {
    return columns;
  }

  @Override
  public String toString() {
    return "IndexUpdate: \n\ttable - " + Bytes.toString(tableName) + "\n\tupdate: " + update
        + "\n\tcolumns: " + columns;
  }

  public static IndexUpdate createIndexUpdateForTesting(ColumnTracker tracker, byte[] table, Put p) {
    IndexUpdate update = new IndexUpdate(tracker);
    update.setTable(table);
    update.setUpdate(p);
    return update;
  }

  /**
   * @return <tt>true</tt> if the necessary state for a valid index update has been set.
   */
  public boolean isValid() {
    return this.tableName != null && this.update != null;
  }
}