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
package com.salesforce.hbase.index.covered.update;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 */
public class ColumnReference implements Comparable<ColumnReference> {
  public static byte[] ALL_QUALIFIERS = new byte[0];

  protected byte[] family;
  protected byte[] qualifier;

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

  public boolean matches(KeyValue kv) {
    if (matchesFamily(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength())) {
      return matchesQualifier(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
    }
    return false;
  }

  /**
   * @param qual to check against
   * @return <tt>true</tt> if this column covers the given qualifier.
   */
  public boolean matchesQualifier(byte[] qual) {
    return matchesQualifier(qual, 0, qual.length);
  }

  public boolean matchesQualifier(byte[] bytes, int offset, int length) {
    return allColumns() ? true : match(bytes, offset, length, qualifier);
  }

  /**
   * @param family to check against
   * @return <tt>true</tt> if this column covers the given family.
   */
  public boolean matchesFamily(byte[] family) {
    return matchesFamily(family, 0, family.length);
  }

  public boolean matchesFamily(byte[] bytes, int offset, int length) {
    return match(bytes, offset, length, family);
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
  private boolean match(byte[] first, int offset, int length, byte[] storedKey) {
    return first == null ? false : Bytes.equals(first, offset, length, storedKey, 0,
      storedKey.length);
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