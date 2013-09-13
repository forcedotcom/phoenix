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
package com.salesforce.hbase.index.covered.data;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;

import com.salesforce.hbase.index.ValueGetter;
import com.salesforce.hbase.index.covered.update.ColumnReference;
import com.salesforce.hbase.index.scanner.Scanner;

/**
 * {@link ValueGetter} that uses lazy initialization to get the value for the given
 * {@link ColumnReference}. Once stored, the mapping for that reference is retained.
 */
public class LazyValueGetter implements ValueGetter {

  private Scanner scan;
  private volatile Map<ColumnReference, byte[]> values;
  private byte[] row;
  
  /**
   * Back the getter with a {@link Scanner} to actually access the local data.
   * @param scan backing scanner
   * @param currentRow row key for the row to seek in the scanner
   */
  public LazyValueGetter(Scanner scan, byte[] currentRow) {
    this.scan = scan;
    this.row = currentRow;
  }

  @Override
  public byte[] getLatestValue(ColumnReference ref) throws IOException {
    // ensure we have a backing map
    if (values == null) {
      synchronized (this) {
        values = Collections.synchronizedMap(new HashMap<ColumnReference, byte[]>());
      }
    }

    // check the value in the map
    byte[] value = values.get(ref);
    if (value == null) {
      value = get(ref);
      values.put(ref, value);
    }

    return value;
  }

  /**
   * @param ref
   * @return the first value on the scanner for the given column
   */
  private byte[] get(ColumnReference ref) throws IOException {
    KeyValue first = ref.getFirstKeyValueForRow(row);
    if (!scan.seek(first)) {
      return null;
    }
    // there is a next value - we only care about the current value, so we can just snag that
    KeyValue next = scan.next();
    if (ref.matches(next)) {
      return next.getValue();
    }
    return null;
  }
}