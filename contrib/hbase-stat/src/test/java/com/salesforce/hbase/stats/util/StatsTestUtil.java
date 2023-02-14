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
package com.salesforce.hbase.stats.util;

import java.io.IOException;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.salesforce.hbase.stats.TestTrackerImpl;

/**
 * Helper utility for testing
 */
public class StatsTestUtil {

  /**
   * @return a valid {@link HTableDescriptor} for the primary table on which we want to collect
   *         statistics
   */
  public static HTableDescriptor getValidPrimaryTableDescriptor() {
    HTableDescriptor table = new HTableDescriptor("primary_table_for_test");
    return table;
  }

  /**
   * Count the total number of rows in the table
   * @param table the table to count
   * @return the number of {@link KeyValue}s in the table
   * @throws IOException if the table has an error while reading
   */
  public static int getKeyValueCount(HTable table) throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions(Integer.MAX_VALUE - 1);
  
    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      count += res.list().size();
      TestTrackerImpl.LOG.info(count + ") " + res);
    }
    results.close();
  
    return count;
  }

}
