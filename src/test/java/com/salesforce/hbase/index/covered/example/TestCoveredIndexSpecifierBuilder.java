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
package com.salesforce.hbase.index.covered.example;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestCoveredIndexSpecifierBuilder {
  private static final String FAMILY = "FAMILY";
  private static final String FAMILY2 = "FAMILY2";
  private static final String INDEX_TABLE = "INDEX_TABLE";
  private static final String INDEX_TABLE2 = "INDEX_TABLE2";


  @Test
  public void testSimpleSerialziationDeserialization() throws Exception {
    byte[] indexed_qualifer = Bytes.toBytes("indexed_qual");

    //setup the index 
    CoveredColumnIndexSpecifierBuilder builder = new CoveredColumnIndexSpecifierBuilder();
    ColumnGroup fam1 = new ColumnGroup(INDEX_TABLE);
    // match a single family:qualifier pair
    CoveredColumn col1 = new CoveredColumn(FAMILY, indexed_qualifer);
    fam1.add(col1);
    // matches the family2:* columns
    CoveredColumn col2 = new CoveredColumn(FAMILY2, null);
    fam1.add(col2);
    builder.addIndexGroup(fam1);
    ColumnGroup fam2 = new ColumnGroup(INDEX_TABLE2);
    // match a single family2:qualifier pair
    CoveredColumn col3 = new CoveredColumn(FAMILY2, indexed_qualifer);
    fam2.add(col3);
    builder.addIndexGroup(fam2);
    
    Configuration conf = new Configuration(false);
    //convert the map that HTableDescriptor gets into the conf the coprocessor receives
    Map<String, String> map = builder.convertToMap();
    for(Entry<String, String> entry: map.entrySet()){
      conf.set(entry.getKey(), entry.getValue());
    }

    List<ColumnGroup> columns = CoveredColumnIndexSpecifierBuilder.getColumns(conf);
    assertEquals("Didn't deserialize the expected number of column groups", 2, columns.size());
    ColumnGroup group = columns.get(0);
    assertEquals("Didn't deserialize expected column in first group", col1, group.getColumnForTesting(0));
    assertEquals("Didn't deserialize expected column in first group", col2, group.getColumnForTesting(1));
    group = columns.get(1);
    assertEquals("Didn't deserialize expected column in second group", col3, group.getColumnForTesting(0));
  }
}