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
package com.salesforce.hbase.index.builder.covered.util;

import java.util.List;

import org.apache.hadoop.hbase.KeyValue;

import com.salesforce.hbase.index.builder.covered.ColumnReference;

/**
 * Utility class to help manage indexes
 */
public class IndexUtil {

  private IndexUtil() {
    // private ctor for util classes
  }

  /** check to see if the kvs in the update  match any of the passed columns. Generally, this is useful to for an index codec to determine if a given update should even be indexed.
  * This assumes that for any index, there are going to small number of columns, versus the number of
  * kvs in any one batch.
  */
  public static boolean updateMatchesColumns(List<KeyValue> update,
      List<ColumnReference> columns) {
    // check to see if the kvs in the new update even match any of the columns requested
    // assuming that for any index, there are going to small number of columns, versus the number of
    // kvs in any one batch.
    boolean matches = false;
    outer: for (KeyValue kv : update) {
      for (ColumnReference ref : columns) {
        if (ref.matchesFamily(kv.getFamily()) && ref.matchesQualifier(kv.getQualifier())) {
          matches = true;
          // if a single column matches a single kv, we need to build a whole scanner
          break outer;
        }
      }
    }
    return matches;
  }
  
  /**
   * check to see if the kvs in the update match any of the passed columns. Generally, this is
   * useful to for an index codec to determine if a given update should even be indexed. This
   * assumes that for any index, there are going to small number of kvs, versus the number of
   * columns in any one batch.
   * <p>
   * This employs the same logic as {@link #updateMatchesColumns(List, List)}, but is flips the
   * iteration logic to search columns before kvs.
   */
  public static boolean columnMatchUpdate(
      List<ColumnReference> columns, List<KeyValue> update) {
    boolean matches = false;
    outer: for (ColumnReference ref : columns) {
      for (KeyValue kv : update) {
        if (ref.matchesFamily(kv.getFamily()) && ref.matchesQualifier(kv.getQualifier())) {
          matches = true;
          // if a single column matches a single kv, we need to build a whole scanner
          break outer;
        }
      }
    }
    return matches;
  }
}
