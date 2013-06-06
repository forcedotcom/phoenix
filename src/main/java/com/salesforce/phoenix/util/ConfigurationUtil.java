package com.salesforce.phoenix.util;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.query.QueryServicesOptions;

/**
 * 
 * Static utils for working with HBase Configuration
 *
 * @author jtaylor
 * @since 0.1
 */
public class ConfigurationUtil {

    private ConfigurationUtil() {
    }

    public static void adjust(Configuration config) {
        // Ensure that HBase RPC time out value is at least as large as our thread time out for query. 
        int threadTimeOutMS = config.getInt(QueryServices.THREAD_TIMEOUT_MS_ATTRIB, QueryServicesOptions.DEFAULT_THREAD_TIMEOUT_MS);
        int hbaseRPCTimeOut = config.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
        if (threadTimeOutMS > hbaseRPCTimeOut) {
            config.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, threadTimeOutMS);
        }
    }
    
    public static Configuration newConfiguration(ReadOnlyProps props) {
        Configuration config = HBaseConfiguration.create();
        for (Entry<String,String> entry : props.getMap().entrySet()) {
            config.set(entry.getKey(), entry.getValue());
        }
        adjust(config);
        return config;
    }
}
