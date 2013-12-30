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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec;
import org.apache.hadoop.hbase.regionserver.wal.WALEditCodec;
import org.apache.hadoop.hbase.util.Bytes;



/**
 * Utility class for testing indexing
 */
public class IndexTestingUtils {

  private static final Log LOG = LogFactory.getLog(IndexTestingUtils.class);
  private static final String MASTER_INFO_PORT_KEY = "hbase.master.info.port";
  private static final String RS_INFO_PORT_KEY = "hbase.regionserver.info.port";
  
  private IndexTestingUtils() {
    // private ctor for util class
  }

  public static void setupConfig(Configuration conf) {
      conf.setInt(MASTER_INFO_PORT_KEY, -1);
      conf.setInt(RS_INFO_PORT_KEY, -1);
    // setup our codec, so we get proper replay/write
      conf.set(WALEditCodec.WAL_EDIT_CODEC_CLASS_KEY, IndexedWALEditCodec.class.getName());
  }
  /**
   * Verify the state of the index table between the given key and time ranges against the list of
   * expected keyvalues.
   * @throws IOException
   */
  @SuppressWarnings("javadoc")
  public static void verifyIndexTableAtTimestamp(HTable index1, List<KeyValue> expected,
      long start, long end, byte[] startKey, byte[] endKey) throws IOException {
    LOG.debug("Scanning " + Bytes.toString(index1.getTableName()) + " between times (" + start
        + ", " + end + "] and keys: [" + Bytes.toString(startKey) + ", " + Bytes.toString(endKey)
        + "].");
    Scan s = new Scan(startKey, endKey);
    // s.setRaw(true);
    s.setMaxVersions();
    s.setTimeRange(start, end);
    List<KeyValue> received = new ArrayList<KeyValue>();
    ResultScanner scanner = index1.getScanner(s);
    for (Result r : scanner) {
      received.addAll(r.list());
      LOG.debug("Received: " + r.list());
    }
    scanner.close();
    assertEquals("Didn't get the expected kvs from the index table!", expected, received);
  }

  public static void verifyIndexTableAtTimestamp(HTable index1, List<KeyValue> expected, long ts,
      byte[] startKey) throws IOException {
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts, startKey, HConstants.EMPTY_END_ROW);
  }

  public static void verifyIndexTableAtTimestamp(HTable index1, List<KeyValue> expected, long start,
      byte[] startKey, byte[] endKey) throws IOException {
    verifyIndexTableAtTimestamp(index1, expected, start, start + 1, startKey, endKey);
  }
}
