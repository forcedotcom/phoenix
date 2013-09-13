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
package com.salesforce.hbase.index;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.junit.Test;

import com.salesforce.hbase.index.builder.example.ColumnFamilyIndexer;

/**
 * Test that we correctly fail for versions of HBase that don't support current properties
 */
public class TestFailForUnsupportedHBaseVersions {
  private static final Log LOG = LogFactory.getLog(TestFailForUnsupportedHBaseVersions.class);

  /**
   * We don't support WAL Compression for HBase &lt; 0.94.9, so we shouldn't even allow the server
   * to start if both indexing and WAL Compression are enabled for the wrong versions.
   */
  @Test
  public void testDoesNotSupportCompressedWAL() {
    Configuration conf = HBaseConfiguration.create();
    IndexTestingUtils.setupConfig(conf);
    // get the current version
    String version = VersionInfo.getVersion();
    
    // ensure WAL Compression not enabled
    conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, false);
    
    //we support all versions without WAL Compression
    String supported = Indexer.validateVersion(version, conf);
    assertNull(
      "WAL Compression wasn't enabled, but version "+version+" of HBase wasn't supported! All versions should"
          + " support writing without a compressed WAL. Message: "+supported, supported);

    // enable WAL Compression
    conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);

    // set the version to something we know isn't supported
    version = "0.94.4";
    supported = Indexer.validateVersion(version, conf);
    assertNotNull("WAL Compression was enabled, but incorrectly marked version as supported",
      supported);
    
    //make sure the first version of 0.94 that supports Indexing + WAL Compression works
    version = "0.94.9";
    supported = Indexer.validateVersion(version, conf);
    assertNull(
      "WAL Compression wasn't enabled, but version "+version+" of HBase wasn't supported! Message: "+supported, supported);
    
    //make sure we support snapshot builds too
    version = "0.94.9-SNAPSHOT";
    supported = Indexer.validateVersion(version, conf);
    assertNull(
      "WAL Compression wasn't enabled, but version "+version+" of HBase wasn't supported! Message: "+supported, supported);
  }

  /**
   * Test that we correctly abort a RegionServer when we run tests with an unsupported HBase
   * version. The 'completeness' of this test requires that we run the test with both a version of
   * HBase that wouldn't be supported with WAL Compression. Currently, this is the default version
   * (0.94.4) so just running 'mvn test' will run the full test. However, this test will not fail
   * when running against a version of HBase with WALCompression enabled. Therefore, to fully test
   * this functionality, we need to run the test against both a supported and an unsupported version
   * of HBase (as long as we want to support an version of HBase that doesn't support custom WAL
   * Codecs).
   * @throws Exception on failure
   */
  @Test(timeout = 300000 /* 5 mins */)
  public void testDoesNotStartRegionServerForUnsupportedCompressionAndVersion() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IndexTestingUtils.setupConfig(conf);
    // enable WAL Compression
    conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);

    // check the version to see if it isn't supported
    String version = VersionInfo.getVersion();
    boolean supported = false;
    if (Indexer.validateVersion(version, conf) == null) {
      supported = true;
    }

    // start the minicluster
    HBaseTestingUtility util = new HBaseTestingUtility(conf);
    util.startMiniCluster();

    // setup the primary table
    HTableDescriptor desc = new HTableDescriptor(
        "testDoesNotStartRegionServerForUnsupportedCompressionAndVersion");
    String family = "f";
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes(family)));

    // enable indexing to a non-existant index table
    Map<byte[], String> familyMap = new HashMap<byte[], String>();
    familyMap.put(Bytes.toBytes(family), "INDEX_TABLE");
    ColumnFamilyIndexer.enableIndexing(desc, familyMap);

    // get a reference to the regionserver, so we can ensure it aborts
    HRegionServer server = util.getMiniHBaseCluster().getRegionServer(0);

    // create the primary table
    HBaseAdmin admin = util.getHBaseAdmin();
    if (supported) {
      admin.createTable(desc);
      assertFalse("Hosting regeion server failed, even the HBase version (" + version
          + ") supports WAL Compression.", server.isAborted());
    } else {
      admin.createTableAsync(desc, null);

      // wait for the regionserver to abort - if this doesn't occur in the timeout, assume its
      // broken.
      while (!server.isAborted()) {
        LOG.debug("Waiting on regionserver to abort..");
      }
    }

    // cleanup
    util.shutdownMiniCluster();
  }
}