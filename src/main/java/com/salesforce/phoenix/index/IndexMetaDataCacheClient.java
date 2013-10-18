/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc. All rights reserved. Redistribution and use in source and binary forms, with
 * or without modification, are permitted provided that the following conditions are met: Redistributions of source code
 * must retain the above copyright notice, this list of conditions and the following disclaimer. Redistributions in
 * binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution. Neither the name of Salesforce.com nor the names
 * of its contributors may be used to endorse or promote products derived from this software without specific prior
 * written permission. THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.phoenix.index;

import static com.salesforce.phoenix.query.QueryServices.INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.cache.ServerCacheClient;
import com.salesforce.phoenix.cache.ServerCacheClient.ServerCache;
import com.salesforce.phoenix.compile.ScanRanges;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.join.MaxServerCacheSizeExceededException;
import com.salesforce.phoenix.query.QueryServicesOptions;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.ReadOnlyProps;
import com.salesforce.phoenix.util.ScanUtil;

public class IndexMetaDataCacheClient {

    private final ServerCacheClient serverCache;
    
    /**
     * Construct client used to send index metadata to each region server
     * for caching during batched put for secondary index maintenance.
     * @param connection the client connection
     * @param cacheUsingTableRef table ref to table that will use the cache during its scan
     */
    public IndexMetaDataCacheClient(PhoenixConnection connection, TableRef cacheUsingTableRef) {
        serverCache = new ServerCacheClient(connection, cacheUsingTableRef);
    }

    /**
     * Determines whether or not to use the IndexMetaDataCache to send the index metadata
     * to the region servers. The alternative is to just set the index metadata as an attribute on
     * the mutations.
     * @param connection 
     * @param mutations the list of mutations that will be sent in a batch to server
     * @param indexMetaDataByteLength length in bytes of the index metadata cache
     */
    public static boolean useIndexMetadataCache(PhoenixConnection connection, List<Mutation> mutations, int indexMetaDataByteLength) {
        ReadOnlyProps props = connection.getQueryServices().getProps();
        int threshold = props.getInt(INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB, QueryServicesOptions.DEFAULT_INDEX_MUTATE_BATCH_SIZE_THRESHOLD);
        return (indexMetaDataByteLength > ServerCacheClient.UUID_LENGTH && mutations.size() > threshold);
    }
    
    /**
     * Send the index metadata cahce to all region servers for regions that will handle the mutations.
     * @return client-side {@link ServerCache} representing the added index metadata cache
     * @throws SQLException 
     * @throws MaxServerCacheSizeExceededException if size of hash cache exceeds max allowed
     * size
     */
    public ServerCache addIndexMetadataCache(List<Mutation> mutations, ImmutableBytesWritable ptr) throws SQLException {
        /**
         * Serialize and compress hashCacheTable
         */
        return serverCache.addServerCache(ScanUtil.newScanRanges(mutations), ptr, new IndexMetaDataCacheFactory());
    }
    
    
    /**
     * Send the index metadata cahce to all region servers for regions that will handle the mutations.
     * @return client-side {@link ServerCache} representing the added index metadata cache
     * @throws SQLException 
     * @throws MaxServerCacheSizeExceededException if size of hash cache exceeds max allowed
     * size
     */
    public ServerCache addIndexMetadataCache(ScanRanges ranges, ImmutableBytesWritable ptr) throws SQLException {
        /**
         * Serialize and compress hashCacheTable
         */
        return serverCache.addServerCache(ranges, ptr, new IndexMetaDataCacheFactory());
    }
}
