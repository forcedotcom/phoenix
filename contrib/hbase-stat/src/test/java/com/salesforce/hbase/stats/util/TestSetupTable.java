/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.salesforce.hbase.stats.util;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Ensure that we verify the tables are setup correctly
 */
public class TestSetupTable {

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCreatesStatTable() throws Exception {
    HTableDescriptor primary = StatsTestUtil.getValidPrimaryTableDescriptor();
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    SetupTableUtil.setupTable(admin, primary, true, true);

    assertTrue("Statistics table didn't get created!", admin.tableExists(Constants.STATS_TABLE_NAME_BYTES));
    // make sure it it is a valid table
    HTableDescriptor statDesc = admin.getTableDescriptor(Constants.STATS_TABLE_NAME_BYTES);
    try {
      SetupTableUtil.verifyStatsTable(statDesc);
    } catch (Exception e) {
      fail("Created statistics table isn't considered valid! Maybe missing a check in the creation?");
    }

    // cleanup after ourselves
    admin.disableTable(Constants.STATS_TABLE_NAME_BYTES);
    admin.deleteTable(Constants.STATS_TABLE_NAME_BYTES);
  }
}