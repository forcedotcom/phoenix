/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.hbase.index.builder.covered;

import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 */
public class ColumnReference implements Comparable<ColumnReference> {
  public static byte[] ALL_QUALIFIERS = new byte[0];

  byte[] family;
  byte[] qualifier;

  public ColumnReference(byte[] family, byte[] qualifier) {
    this.family = family;
    this.qualifier = qualifier;
  }

  public byte[] getFamily() {
    return this.family;
  }

  public byte[] getQualifier() {
    return this.qualifier;
  }

  /**
   * @param qual to check against
   * @return <tt>true</tt> if this column covers the given qualifier.
   */
  public boolean matchesQualifier(byte[] qual) {
    return allColumns() ? true : match(qual, qualifier);
  }

  /**
   * @param family to check against
   * @return <tt>true</tt> if this column covers the given family.
   */
  public boolean matchesFamily(byte[] family) {
    return match(family, this.family);
  }

  /**
   * @return <tt>true</tt> if this should include all column qualifiers, <tt>false</tt> otherwise
   */
  public boolean allColumns() {
    return this.qualifier == ALL_QUALIFIERS;
  }

  /**
   * Check to see if the passed bytes match the stored bytes
   * @param first
   * @param storedKey the stored byte[], should never be <tt>null</tt>
   * @return <tt>true</tt> if they are byte-equal
   */
  private boolean match(byte[] first, byte[] storedKey) {
    return first == null ? false : Arrays.equals(first, storedKey);
  }

  public KeyValue getFirstKeyValueForRow(byte[] row) {
    return KeyValue.createFirstOnRow(row, family, qualifier == ALL_QUALIFIERS ? null : qualifier);
  }

  @Override
  public int compareTo(ColumnReference o) {
    int c = Bytes.compareTo(family, o.family);
    if (c == 0) {
      // matching families, compare qualifiers
      c = Bytes.compareTo(qualifier, o.qualifier);
    }
    return c;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ColumnReference) {
      ColumnReference other = (ColumnReference) o;
      if (Bytes.equals(family, other.family)) {
        return Bytes.equals(qualifier, other.qualifier);
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(family) + Bytes.hashCode(qualifier);
  }

  @Override
  public String toString() {
    return "ColumnReference - " + Bytes.toString(family) + ":" + Bytes.toString(qualifier);
  }
}