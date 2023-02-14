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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.salesforce.hbase.stats.cleanup.CleanupStatistics;

import static com.salesforce.hbase.stats.util.Constants.STATS_TABLE_NAME;

/**
 * Utility helper class to ensure that your primary and statistics table is setup correctly
 */
public class SetupTableUtil {

  private static final String TABLE_STATS_ENABLED_DESC_KEY = "com.salesforce.hbase.stats.cleanup";

  private SetupTableUtil() {
    // private ctor for util classes
  }


  /**
   * Ensure all the necessary coprocessors are added to a cluster's configuration
   * @param conf {@link Configuration} to update
   */
  public static void setupCluster(Configuration conf){
    CleanupStatistics.setupClusterConfiguration(conf);
  }
  
  public static void setupTable(HBaseAdmin admin, HTableDescriptor primaryTable,
      boolean ensureStatTable, boolean createStatTable)
      throws IOException {
    // add the right keys to the primary table
    primaryTable.setValue(TABLE_STATS_ENABLED_DESC_KEY, "true");
    CleanupStatistics.setupTable(primaryTable);

    if (!ensureStatTable) {
      return;
    }

    // ensure that the stats table is setup correctly
    boolean exists = admin.tableExists(STATS_TABLE_NAME);
    HTableDescriptor statDesc = null;
    if (exists) {
      if (createStatTable) {
      throw new IllegalStateException("Statistics table '" + STATS_TABLE_NAME
          + " was requested to be created, but already exists!");
      } else {
        // get the descriptor so we can verify it has the right properties
        statDesc = admin.getTableDescriptor(Bytes.toBytes(STATS_TABLE_NAME));
    }
    } else {
      if (createStatTable) {
        statDesc = createStatsTable(admin);
      }
    }
    verifyStatsTable(statDesc);
  }

  public static HTableDescriptor createStatsTable(HBaseAdmin admin) throws IOException {
    HTableDescriptor statDesc = new HTableDescriptor(STATS_TABLE_NAME);
    HColumnDescriptor col = new HColumnDescriptor(Constants.STATS_DATA_COLUMN_FAMILY);
    col.setMaxVersions(1);
    statDesc.addFamily(col);
    admin.createTable(statDesc);
    return statDesc;
  }
  
  /**
   * @param desc {@link HTableDescriptor} of the statistics table to verify
   */
  public static void verifyStatsTable(HTableDescriptor desc) {
    if (!desc.hasFamily(Constants.STATS_DATA_COLUMN_FAMILY)) {
      throw new IllegalStateException("Statistics table '" + desc
          + "' doesn't have expected column family: " + Bytes.toString(Constants.STATS_DATA_COLUMN_FAMILY));
    }
    // only keep around a single version
    int versions = desc.getFamily(Constants.STATS_DATA_COLUMN_FAMILY).getMaxVersions();
    Preconditions.checkState(versions == 1,
      "Stats rows should only have a single version, but set to: " + versions);
  }


  /**
   * @param desc {@link HTableDescriptor} to check
   * @return <tt>true</tt> if statistics have been turned on for the table
   */
  public static boolean getStatsEnabled(HTableDescriptor desc) {
    String hasStats = desc.getValue(TABLE_STATS_ENABLED_DESC_KEY);
    if (hasStats != null && hasStats.equals("true")) {
      return true;
    }
    return false;
  }
}