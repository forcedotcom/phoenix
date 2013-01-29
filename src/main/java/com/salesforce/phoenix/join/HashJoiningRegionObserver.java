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
package com.salesforce.phoenix.join;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;


import com.google.common.collect.Lists;
import com.salesforce.phoenix.cache.*;
import com.salesforce.phoenix.coprocessor.BaseRegionScanner;
import com.salesforce.phoenix.coprocessor.BaseScannerRegionObserver;
import com.salesforce.phoenix.query.QueryConstants.JoinType;
import com.salesforce.phoenix.util.*;


/**
 * 
 * Prototype for region observer that performs a hash join between two tables.
 * The client sends over the rows in a serialized format and the coprocessor
 * deserialized into a Map and caches it on the region server.  The map is then
 * used to resolve the foreign key reference and the rows are then joined together.
 *
 * TODO: Scan rows locally on region server instead of returning to client
 * if we can know that all both tables rows are on the same region server.
 * 
 * @author jtaylor
 * @since 0.1
 */
public class HashJoiningRegionObserver extends BaseScannerRegionObserver  {
    public static final String JOIN_COUNT = "JoinCount";
    public static final String JOIN_IDS = "JoinIds";
    public static final String JOIN_THROUGH_FAMILIES = "JoinFamilies";
    public static final String JOIN_THROUGH_QUALIFIERS = "JoinQualifiers";
    public static final String JOIN_THROUGH_POSITIONS = "JoinPositions";
    public static final String JOIN_TYPES = "JoinTypes";
        
    private HashCache getHashCache(Configuration config, ImmutableBytesWritable tenantId, ImmutableBytesWritable joinId) throws IOException {
        TenantCache tenantCache = GlobalCache.getTenantCache(config, tenantId);
        HashCache hashCache = tenantCache.getHashCache(joinId);
        if (hashCache == null) {
            throw new DoNotRetryIOException("Unable to find hash cache for tenantId=" + tenantId + ",joinId=" + joinId);
        }
        return hashCache;
    }
    
    @Override
    protected RegionScanner doPostScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan,
            final RegionScanner s) throws IOException {
        /*
         * Support snowflake join.  The JOIN_COUNT is the number of dimension
         * tables being joined against.  Each one of them is cached inside of
         * the OrgCache.  The join column families, join column names, and join
         * column IDs are passed through in parallel arrays through attributes
         * on the Scan object.
         */
        byte[] joinCountBytes = scan.getAttribute(JOIN_COUNT);
        if (joinCountBytes == null) return s;
        final int joinCount = (int)Bytes.bytesToVint(joinCountBytes);

        byte[] joinThroughFamsBytes = scan.getAttribute(JOIN_THROUGH_FAMILIES);
        if (joinThroughFamsBytes == null) return s;
        final byte[][] joinThroughFams = ByteUtil.toByteArrays(joinThroughFamsBytes, joinCount);

        byte[] joinThroughColsBytes = scan.getAttribute(JOIN_THROUGH_QUALIFIERS);
        if (joinThroughColsBytes == null) return s;
        final byte[][] joinThroughCols = ByteUtil.toByteArrays(joinThroughColsBytes, joinCount);

        byte[] joinThroughPossBytes = scan.getAttribute(JOIN_THROUGH_POSITIONS);
        if (joinThroughPossBytes == null) return s;
        final int[] joinThroughPoss = ByteUtil.deserializeIntArray(joinThroughPossBytes, joinCount);

        byte[] joinTypesBytes = scan.getAttribute(JOIN_TYPES);
        if (joinTypesBytes == null) return s;
        final int[] joinTypes = ByteUtil.deserializeIntArray(joinTypesBytes, joinCount);

        byte[] joinIdsBytes = scan.getAttribute(JOIN_IDS);
        if (joinIdsBytes == null) return s;
        final byte[][] joinIds = ByteUtil.toByteArrays(joinIdsBytes, joinCount);

        final ImmutableBytesWritable fkValue = new ImmutableBytesWritable();
        
        if (joinThroughFams.length != joinCount
            || joinThroughCols.length != joinCount 
            || joinIds.length != joinCount) {
            throw new IllegalStateException("Expected join column family, join column name, and join id arrays to be of the same length");
        }
        ImmutableBytesWritable tenantId = ScanUtil.getTenantId(scan);
        Configuration config = c.getEnvironment().getConfiguration();
        final HashCache[] hashCaches = new HashCache[joinCount];
        for (int i = 0; i < joinCount; i++) {
            hashCaches[i] = getHashCache(config, tenantId, new ImmutableBytesWritable(joinIds[i]));
        }
        
        return new BaseRegionScanner() {
            @Override
            public HRegionInfo getRegionInfo() {
                return s.getRegionInfo();
            }

            @Override
            public boolean isFilterDone() {
                return s.isFilterDone();
            }

            @Override
            public void close() throws IOException {
                s.close();
            }

            @Override
            public boolean next(List<KeyValue> joinedResults) throws IOException {
                try {
                    boolean hasMore;
                    do {
                        // Results are potentially returned even when the return value of s.next is false
                        // since this is an indication of whether or not there are more values after the
                        // ones returned
                        List<KeyValue> results = Lists.newArrayList();
                        hasMore = s.next(results) && !s.isFilterDone();
                        if (!results.isEmpty()) {
                            Result result = new Result(results);
                            ImmutableBytesWritable key = ResultUtil.getKey(result);
                            joinedResults.addAll(results);
                            for (int i = 0; i < joinCount; i++) {
                                Result hashCacheRow = null;
                                byte[] joinThroughCol = joinThroughCols[i];
                                byte[] joinThroughFam = joinThroughFams[i];
                                int joinThroughPos = joinThroughPoss[i];
                                HashCache hashCache = hashCaches[i];
                                if (joinThroughCol == null) { // Join using key of lhs table
                                    hashCacheRow = hashCache.get(new ImmutableBytesWritable(key.get(),key.getOffset(), key.getLength()));
                                } else { // else join through fk
                                    if (SchemaUtil.getValue(result, joinThroughFam, joinThroughCol, joinThroughPos, fkValue)) {
                                        hashCacheRow = hashCache.get(fkValue);
                                    }
                                }
                                if (hashCacheRow != null) {
                                    SchemaUtil.joinColumns(hashCacheRow.list(), joinedResults);
                                } else if (JoinType.values()[joinTypes[i]] == JoinType.LEFT_OUTER) {
                                    // If left outer join nothing to do
                                } else { // If not left outer join and no match found then filter this row
                                    joinedResults.clear();
                                    break;
                                }
                            }
                            if (joinedResults.isEmpty()) {
                                continue; // Unable to find joined row for one or more tables, so just continue
                            } else {
                                break; // Found row for all joined tables, so return result back to client
                            }
                        }
                        
                    } while (hasMore);
                    return hasMore;
                } catch (IOException e) {
                    throw e;
                } catch (Throwable t) {
                    throw new DoNotRetryIOException(t.getMessage(),t);
                }
            }

            @Override
            public boolean next(List<KeyValue> result, int limit) throws IOException {
                // TODO: not sure how this limit comes into play.  This would limit
                // the number of columns being returned, since we're always returning
                // a single row here.
                return next(result);
            }

			@Override
			public boolean reseek(byte[] row) throws IOException {
                throw new DoNotRetryIOException("Unsupported");
			}

        };
    }

}
