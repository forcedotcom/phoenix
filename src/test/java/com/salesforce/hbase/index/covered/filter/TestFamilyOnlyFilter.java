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
package com.salesforce.hbase.index.covered.filter;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * Test that the family only filter only allows a single family through
 */
public class TestFamilyOnlyFilter {

  byte[] row = new byte[] { 'a' };
  byte[] qual = new byte[] { 'b' };
  byte[] val = Bytes.toBytes("val");

  @Test
  public void testPassesFirstFamily() {
    byte[] fam = Bytes.toBytes("fam");
    byte[] fam2 = Bytes.toBytes("fam2");

    FamilyOnlyFilter filter = new FamilyOnlyFilter(fam);

    KeyValue kv = new KeyValue(row, fam, qual, 10, val);
    ReturnCode code = filter.filterKeyValue(kv);
    assertEquals("Didn't pass matching family!", ReturnCode.INCLUDE, code);

    kv = new KeyValue(row, fam2, qual, 10, val);
    code = filter.filterKeyValue(kv);
    assertEquals("Didn't filter out non-matching family!", ReturnCode.SKIP, code);
  }

  @Test
  public void testPassesTargetFamilyAsNonFirstFamily() {
    byte[] fam = Bytes.toBytes("fam");
    byte[] fam2 = Bytes.toBytes("fam2");
    byte[] fam3 = Bytes.toBytes("way_after_family");

    FamilyOnlyFilter filter = new FamilyOnlyFilter(fam2);

    KeyValue kv = new KeyValue(row, fam, qual, 10, val);

    ReturnCode code = filter.filterKeyValue(kv);
    assertEquals("Didn't filter out non-matching family!", ReturnCode.SKIP, code);

    kv = new KeyValue(row, fam2, qual, 10, val);
    code = filter.filterKeyValue(kv);
    assertEquals("Didn't pass matching family", ReturnCode.INCLUDE, code);

    kv = new KeyValue(row, fam3, qual, 10, val);
    code = filter.filterKeyValue(kv);
    assertEquals("Didn't filter out non-matching family!", ReturnCode.SKIP, code);
  }

  @Test
  public void testResetFilter() {
    byte[] fam = Bytes.toBytes("fam");
    byte[] fam2 = Bytes.toBytes("fam2");
    byte[] fam3 = Bytes.toBytes("way_after_family");

    FamilyOnlyFilter filter = new FamilyOnlyFilter(fam2);

    KeyValue kv = new KeyValue(row, fam, qual, 10, val);

    ReturnCode code = filter.filterKeyValue(kv);
    assertEquals("Didn't filter out non-matching family!", ReturnCode.SKIP, code);

    KeyValue accept = new KeyValue(row, fam2, qual, 10, val);
    code = filter.filterKeyValue(accept);
    assertEquals("Didn't pass matching family", ReturnCode.INCLUDE, code);

    kv = new KeyValue(row, fam3, qual, 10, val);
    code = filter.filterKeyValue(kv);
    assertEquals("Didn't filter out non-matching family!", ReturnCode.SKIP, code);

    // we shouldn't match the family again - everything after a switched family should be ignored
    code = filter.filterKeyValue(accept);
    assertEquals("Should have skipped a 'matching' family if it arrives out of order",
      ReturnCode.SKIP, code);

    // reset the filter and we should accept it again
    filter.reset();
    code = filter.filterKeyValue(accept);
    assertEquals("Didn't pass matching family after reset", ReturnCode.INCLUDE, code);
  }
}
