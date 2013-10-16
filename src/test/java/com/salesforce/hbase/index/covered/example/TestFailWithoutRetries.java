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

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.MultiResponse;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.salesforce.hbase.index.IndexTestingUtils;
import com.salesforce.hbase.index.Indexer;
import com.salesforce.hbase.index.TableName;
import com.salesforce.hbase.index.covered.IndexUpdate;
import com.salesforce.hbase.index.covered.TableState;
import com.salesforce.hbase.index.util.IndexManagementUtil;
import com.salesforce.phoenix.index.BaseIndexCodec;

/**
 * If {@link DoNotRetryIOException} is not subclassed correctly (with the {@link String}
 * constructor), {@link MultiResponse#readFields(java.io.DataInput)} will not correctly deserialize
 * the exception, and just return <tt>null</tt> to the client, which then just goes and retries.
 */
public class TestFailWithoutRetries {

  private static final Log LOG = LogFactory.getLog(TestFailWithoutRetries.class);
  @Rule
  public TableName table = new TableName();

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private String getIndexTableName() {
    return Bytes.toString(table.getTableName()) + "_index";
  }

  public static class FailingTestCodec extends BaseIndexCodec {

    @Override
    public Iterable<IndexUpdate> getIndexDeletes(TableState state) throws IOException {
      throw new RuntimeException("Intentionally failing deletes for "
          + TestFailWithoutRetries.class.getName());
    }

    @Override
    public Iterable<IndexUpdate> getIndexUpserts(TableState state) throws IOException {
      throw new RuntimeException("Intentionally failing upserts for "
          + TestFailWithoutRetries.class.getName());
    }

  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    // setup and verify the config
    Configuration conf = UTIL.getConfiguration();
    IndexTestingUtils.setupConfig(conf);
    IndexManagementUtil.ensureMutableIndexingCorrectlyConfigured(conf);
    // start the cluster
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  /**
   * If this test times out, then we didn't fail quickly enough. {@link Indexer} maybe isn't
   * rethrowing the exception correctly?
   * <p>
   * We use a custom codec to enforce the thrown exception.
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testQuickFailure() throws Exception {
    // incorrectly setup indexing for the primary table - target index table doesn't exist, which
    // should quickly return to the client
    byte[] family = Bytes.toBytes("family");
    ColumnGroup fam1 = new ColumnGroup(getIndexTableName());
    // values are [col1]
    fam1.add(new CoveredColumn(family, CoveredColumn.ALL_QUALIFIERS));
    CoveredColumnIndexSpecifierBuilder builder = new CoveredColumnIndexSpecifierBuilder();
    // add the index family
    builder.addIndexGroup(fam1);
    // usually, we would create the index table here, but we don't for the sake of the test.

    // setup the primary table
    String primaryTable = Bytes.toString(table.getTableName());
    HTableDescriptor pTable = new HTableDescriptor(primaryTable);
    pTable.addFamily(new HColumnDescriptor(family));
    // override the codec so we can use our test one
    builder.build(pTable, FailingTestCodec.class);

    // create the primary table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.createTable(pTable);
    Configuration conf = new Configuration(UTIL.getConfiguration());
    // up the number of retries/wait time to make it obvious that we are failing with retries here
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 20);
    conf.setLong(HConstants.HBASE_CLIENT_PAUSE, 1000);
    HTable primary = new HTable(conf, primaryTable);
    primary.setAutoFlush(false, true);

    // do a simple put that should be indexed
    Put p = new Put(Bytes.toBytes("row"));
    p.add(family, null, Bytes.toBytes("value"));
    primary.put(p);
    try {
      primary.flushCommits();
      fail("Shouldn't have gotten a successful write to the primary table");
    } catch (RetriesExhaustedWithDetailsException e) {
      LOG.info("Correclty got a failure of the put!");
    }
    primary.close();
  }
}