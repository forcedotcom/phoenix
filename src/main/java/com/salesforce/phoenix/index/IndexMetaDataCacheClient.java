package com.salesforce.phoenix.index;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

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

    private final ServerCacheClient serverCache;
    
    /**
     * Construct client used to send index metadata to each region server
     * for caching during batched put for secondary index maintenance.
     * @param connection the client connection
     * @param cacheUsingTableRef table ref to table that will use the cache during its scan
     * @param List<Mutation> the list of mutations that will be sent in the batched put
     */
    public IndexMetaDataCacheClient(PhoenixConnection connection, TableRef cacheUsingTableRef, List<Mutation> mutations) {
        List<KeyRange> keys = Lists.newArrayListWithExpectedSize(mutations.size());
        for (Mutation m : mutations) {
            keys.add(PDataType.VARBINARY.getKeyRange(m.getRow()));
        }
        ScanRanges keyRanges = ScanRanges.create(Collections.singletonList(keys), SaltingUtil.VAR_BINARY_SCHEMA);
        serverCache = new ServerCacheClient(connection,cacheUsingTableRef,keyRanges);
    }

    /**
     * Send the index metadata cahce to all region servers for regions that will handle the mutations.
     * @return client-side {@link ServerCache} representing the added index metadata cache
     * @throws SQLException 
     * @throws MaxServerCacheSizeExceededException if size of hash cache exceeds max allowed
     * size
     */
    public ServerCache addIndexMetadataCache() throws SQLException {
        /**
         * Serialize and compress hashCacheTable
         */
        final ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        serverCache.getTableRef().getTable().getIndexMaintainers(Bytes.toBytes(serverCache.getTableRef().getSchema().getName()), ptr);
        return serverCache.addServerCache(ptr, new IndexMetaDataCacheFactory());
    }
    
}
