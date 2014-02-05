package com.salesforce.hbase.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.salesforce.hbase.stats.util.Constants;
import com.salesforce.hbase.stats.util.SetupTableUtil;
import com.salesforce.hbase.stats.util.StatsTestUtil;

/**
 * Helper test for testing an implementation of a statistic.
 * <p>
 * Uses the {@link HBaseTestingUtility#loadTable(HTable, byte[])} to load the {@link #FAM} column
 * family with data. The table is then flushed and compacted, ensuring statistics are gathered
 * through the normal mechanisms.
 * <p>
 * Use {@link #preparePrimaryTableDescriptor(HTableDescriptor)} to add your custom
 * {@link StatisticTracker} to the table.
 * <p>
 * Use {@link #verifyStatistics(HTableDescriptor)} to verify that all the correct statistics have
 * been collected on the table, after it has been loaded, flushed and compacted.
 */
@SuppressWarnings("javadoc")
public abstract class TestTrackerImpl {
  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  public static final byte[] FAM = Bytes.toBytes("FAMILY");
  public static final Log LOG = LogFactory.getLog(TestTrackerImpl.class);

  @BeforeClass
  public static void setupCluster() throws Exception {
    SetupTableUtil.setupCluster(UTIL.getConfiguration());
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setupTables() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    // setup the stats table
    SetupTableUtil.createStatsTable(admin);
    // make sure the stats table got created
    assertTrue("Stats table didn't get created!", admin.tableExists(Constants.STATS_TABLE_NAME));
  }

  @After
  public void cleanupTables() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.disableTable(Constants.STATS_TABLE_NAME);
    admin.deleteTable(Constants.STATS_TABLE_NAME);
    admin.close();
  }

  /**
   * Goes through a full end-to-end test of gathering statistics on a table.
   * <p>
   * First, we create and verify the statistics table. Then we write some data to the primary table.
   * Finally, we check that the given statistic is enabled and working correctly by reading the
   * stats table.
   * @throws Exception on failure
   */
  @Test
  public void testSimplePrimaryAndStatsTables() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();

    // setup our primary table
    HTableDescriptor primary = new HTableDescriptor("testSimplePrimaryAndStatsTables");
    primary.addFamily(new HColumnDescriptor(FAM));

    // make sure stats are enabled on the table
    SetupTableUtil.setupTable(UTIL.getHBaseAdmin(), primary, false, false);

    // do any further setup on the table
    preparePrimaryTableDescriptor(primary);

    // create the primary table
    admin.createTable(primary);

    // load some data into our primary table
    HTable primaryTable = new HTable(UTIL.getConfiguration(), primary.getName());
    UTIL.loadTable(primaryTable, FAM);

    // now flush and compact our table
    HRegionServer server = UTIL.getRSForFirstRegionInTable(primary.getName());
    List<HRegion> regions = server.getOnlineRegions(primary.getName());
    assertTrue("Didn't find any regions for primary table!", regions.size() > 0);
    // flush and compact all the regions of the primary table
    for (HRegion region : regions) {
      region.flushcache();
      region.compactStores(true);
    }

    // make sure all the stats that we expect got written
    verifyStatistics(primary);

    // then delete the table and make sure we don't have any more stats in our table
    admin.disableTable(primary.getName());
    admin.deleteTable(primary.getName());

    // make sure that we cleanup the stats on table delete
    HTable stats = new HTable(UTIL.getConfiguration(), Constants.STATS_TABLE_NAME);
    assertEquals("Stats table still has values after primary table delete", 0,
      StatsTestUtil.getKeyValueCount(stats));

    // and cleanup after ourselves
    stats.close();
  }

  /**
   * Prepare the primary table descriptor for the test. For instance, add the
   * {@link StatisticTracker} to the table. This is called before the primary table is created
   * @throws Exception on failure
   */
  protected abstract void preparePrimaryTableDescriptor(HTableDescriptor primary) throws Exception;

  /**
   * Verify the statistics on the given primary table after the table has been loaded, flushed, and
   * compacted.
   * @param primary {@link HTableDescriptor} for the primary table for which we were collecting
   *          statistics
   * @throws Exception on failure
   */
  protected abstract void verifyStatistics(HTableDescriptor primary) throws Exception;
}
