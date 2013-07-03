package com.salesforce.hbase.index;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Utility class for testing indexing
 */
public class IndexTestingUtils {

  private static final Log LOG = LogFactory.getLog(IndexTestingUtils.class);
  private IndexTestingUtils() {
    // private ctor for util class
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
