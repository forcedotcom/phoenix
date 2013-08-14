package com.salesforce.hbase.index;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;

import com.salesforce.hbase.index.builder.IndexBuilder;

public final class IndexUtil {

  static final String INDEX_BUILDER_CONF_KEY = "index.builder";

  private IndexUtil(){
    //private ctor for util classes
  }


  /**
   * Enable indexing on the given table
   * @param desc {@link HTableDescriptor} for the table on which indexing should be enabled
   * @param builder class to use when building the index for this table
   * @param properties map of custom configuration options to make available to your
   *          {@link IndexBuilder} on the server-side
   * @throws IOException the Indexer coprocessor cannot be added
   */
  public static void enableIndexing(HTableDescriptor desc, Class<? extends IndexBuilder> builder,
      Map<String, String> properties) throws IOException {
    if (properties == null) {
      properties = new HashMap<String, String>();
    }
    properties.put(INDEX_BUILDER_CONF_KEY, builder.getName());
    desc.addCoprocessor(Indexer.class.getName(), null, Coprocessor.PRIORITY_USER, properties);
  }

  /**
   * Validate that the version and configuration parameters are supported
   * @param hBaseVersion current version of HBase on which <tt>this</tt> coprocessor is installed
   * @param conf configuration to check for allowed parameters (e.g. WAL Compression only if >=
   *          0.94.9)
   */
  static String validateVersion(String hBaseVersion, Configuration conf) {
    String[] versions = hBaseVersion.split("[.]");
    if (versions.length < 3) {
      return "HBase version could not be read, expected three parts, but found: "
          + Arrays.toString(versions);
    }
  
    if (versions[1].equals("94")) {
      String pointVersion = versions[2];
      //remove -SNAPSHOT if applicable
      int snapshot = pointVersion.indexOf("-SNAPSHOT");
      if(snapshot > 0){
        pointVersion = pointVersion.substring(0, snapshot);
      }
      // less than 0.94.9, so we need to check if WAL Compression is enabled
      if (Integer.parseInt(pointVersion) < 9) {
        if (conf.getBoolean(HConstants.ENABLE_WAL_COMPRESSION, false)) {
          return
                "Indexing not supported with WAL Compression for versions of HBase older than 0.94.9 - found version:"
              + Arrays.toString(versions);
        }
      }
    }
    return null;
  }
}