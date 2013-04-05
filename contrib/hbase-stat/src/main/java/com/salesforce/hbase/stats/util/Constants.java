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

import org.apache.hadoop.hbase.util.Bytes;

/**
 * General constants for hbase-stat
 */
public class Constants {
  

  private Constants() {
    // private ctor for utility class
  }

  
  /** Name of the column family to store all the statistics data */
  public static final byte[] STATS_DATA_COLUMN_FAMILY = Bytes.toBytes("STAT");
  
  /** Name of the statistics table */
  public static final String STATS_TABLE_NAME = "_stats_";
  
  public static final byte[] STATS_TABLE_NAME_BYTES = Bytes.toBytes(STATS_TABLE_NAME);
}
