package com.salesforce.phoenix.index;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Lists;
import com.salesforce.phoenix.cache.ServerCacheClient;
import com.salesforce.phoenix.cache.ServerCacheClient.ServerCache;
import com.salesforce.phoenix.compile.ScanRanges;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.join.MaxServerCacheSizeExceededException;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.SaltingUtil;
import com.salesforce.phoenix.schema.TableRef;

public class IndexMetaDataCacheClient {
    private static final int USE_CACHE_THRESHOLD = 10;
    private static final int UUID_BYTE_LENGTH = 16; // 128 bits as bytes should hopefully be 16 bytes

    private final ServerCacheClient serverCache;
    
    /**
     * Construct client used to send index metadata to each region server
     * for caching during batched put for secondary index maintenance.
     * @param connection the client connection
     * @param cacheUsingTableRef table ref to table that will use the cache during its scan
     * @param List<Mutation> the list of mutations that will be sent in the batched put
     */
    public IndexMetaDataCacheClient(PhoenixConnection connection, TableRef cacheUsingTableRef) {
        serverCache = new ServerCacheClient(connection, cacheUsingTableRef);
    }

    /**
     * Determines whether or not to use the IndexMetaDataCache to send the index metadata
     * to the region servers. The alternative is to just set the index metadata as an attribute on
     * the mutations.
     * @param mutations the list of mutations that will be sent in a batch to server
     * @param indexMetaDataByteLength length in bytes of the index metadata cache
     * @return
     */
    public static boolean useIndexMetadataCache(List<Mutation> mutations, int indexMetaDataByteLength) {
        return (indexMetaDataByteLength > UUID_BYTE_LENGTH && mutations.size() > USE_CACHE_THRESHOLD);
    }
    
    /**
     * Send the index metadata cahce to all region servers for regions that will handle the mutations.
     * @return client-side {@link ServerCache} representing the added index metadata cache
     * @throws SQLException 
     * @throws MaxServerCacheSizeExceededException if size of hash cache exceeds max allowed
     * size
     */
    public ServerCache addIndexMetadataCache(List<Mutation> mutations, ImmutableBytesWritable ptr) throws SQLException {
        List<KeyRange> keys = Lists.newArrayListWithExpectedSize(mutations.size());
        for (Mutation m : mutations) {
            keys.add(PDataType.VARBINARY.getKeyRange(m.getRow()));
        }
        ScanRanges keyRanges = ScanRanges.create(Collections.singletonList(keys), SaltingUtil.VAR_BINARY_SCHEMA);
        /**
         * Serialize and compress hashCacheTable
         */
        return serverCache.addServerCache(keyRanges, ptr, new IndexMetaDataCacheFactory());
    }
    
}
