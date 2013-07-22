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
package com.salesforce.phoenix.coprocessor;

import java.io.*;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.Lists;
import com.salesforce.phoenix.cache.GlobalCache;
import com.salesforce.phoenix.cache.TenantCache;
import com.salesforce.phoenix.expression.OrderByExpression;
import com.salesforce.phoenix.iterate.*;
import com.salesforce.phoenix.join.HashJoinInfo;
import com.salesforce.phoenix.memory.MemoryManager.MemoryChunk;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ScanUtil;
import com.salesforce.phoenix.util.ServerUtil;


/**
 * 
 * Wraps the scan performing a non aggregate query to prevent needless retries
 * if a Phoenix bug is encountered from our custom filter expression evaluation.
 * Unfortunately, until HBASE-7481 gets fixed, there's no way to do this from our
 * custom filters.
 *
 * @author jtaylor
 * @since 0.1
 */
public class ScanRegionObserver extends BaseScannerRegionObserver {
    public static final String NON_AGGREGATE_QUERY = "NonAggregateQuery";
    private static final String TOPN = "TopN";

    public static void serializeIntoScan(Scan scan, int thresholdBytes, int limit, List<OrderByExpression> orderByExpressions, int estimatedRowSize) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream(); // TODO: size?
        try {
            DataOutputStream output = new DataOutputStream(stream);
            WritableUtils.writeVInt(output, thresholdBytes);
            WritableUtils.writeVInt(output, limit);
            WritableUtils.writeVInt(output, estimatedRowSize);
            WritableUtils.writeVInt(output, orderByExpressions.size());
            for (OrderByExpression orderingCol : orderByExpressions) {
                orderingCol.write(output);
            }
            scan.setAttribute(TOPN, stream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    public static OrderedResultIterator deserializeFromScan(Scan scan, RegionScanner s) {
        byte[] topN = scan.getAttribute(TOPN);
        if (topN == null) {
            return null;
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(topN); // TODO: size?
        try {
            DataInputStream input = new DataInputStream(stream);
            int thresholdBytes = WritableUtils.readVInt(input);
            int limit = WritableUtils.readVInt(input);
            int estimatedRowSize = WritableUtils.readVInt(input);
            int size = WritableUtils.readVInt(input);
            List<OrderByExpression> orderByExpressions = Lists.newArrayListWithExpectedSize(size);           
            for (int i = 0; i < size; i++) {
                OrderByExpression orderByExpression = new OrderByExpression();
                orderByExpression.readFields(input);
                orderByExpressions.add(orderByExpression);
            }
            ResultIterator inner = new RegionScannerResultIterator(s);
            return new OrderedResultIterator(inner, orderByExpressions, thresholdBytes, limit >= 0 ? limit : null, estimatedRowSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected RegionScanner doPostScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan, final RegionScanner s) throws Throwable {
        byte[] isScanQuery = scan.getAttribute(NON_AGGREGATE_QUERY);

        if (isScanQuery == null || Bytes.compareTo(PDataType.FALSE_BYTES, isScanQuery) == 0) {
            return s;
        }
        
        final ScanProjector p = ScanProjector.deserializeProjectorFromScan(scan);
        final HashJoinInfo j = HashJoinInfo.deserializeHashJoinFromScan(scan);
        final OrderedResultIterator iterator = deserializeFromScan(scan,s);
        final ImmutableBytesWritable tenantId = ScanUtil.getTenantId(scan);
        
        RegionScanner innerScanner = s;
        if (p != null || j != null) {
            innerScanner = new HashJoinRegionScanner(s, p, j, tenantId, c.getEnvironment().getConfiguration());
        }
        
        if (iterator == null) {
            return getWrappedScanner(c, innerScanner);
        }
        
        return getTopNScanner(c, innerScanner, iterator, tenantId);
    }
    
    /**
     *  Return region scanner that does TopN.
     *  We only need to call startRegionOperation and closeRegionOperation when
     *  getting the first Tuple (which forces running through the entire region)
     *  since after this everything is held in memory
     */
    private RegionScanner getTopNScanner(final ObserverContext<RegionCoprocessorEnvironment> c, final RegionScanner s, final OrderedResultIterator iterator, ImmutableBytesWritable tenantId) throws Throwable {
        final Tuple firstTuple;
        TenantCache tenantCache = GlobalCache.getTenantCache(c.getEnvironment().getConfiguration(), tenantId);
        long estSize = iterator.getEstimatedByteSize();
        final MemoryChunk chunk = tenantCache.getMemoryManager().allocate(estSize);
        final HRegion region = c.getEnvironment().getRegion();
        region.startRegionOperation();
        try {
            // Once we return from the first call to next, we've run through and cached
            // the topN rows, so we no longer need to start/stop a region operation.
            firstTuple = iterator.next();
            // Now that the topN are cached, we can resize based on the real size
            long actualSize = iterator.getByteSize();
            chunk.resize(actualSize);
        } catch (Throwable t) {
            ServerUtil.throwIOException(region.getRegionNameAsString(), t);
            return null;
        } finally {
            region.closeRegionOperation();
        }
        return new BaseRegionScanner() {
            private Tuple tuple = firstTuple;
            
            @Override
            public boolean isFilterDone() {
                return tuple == null; 
            }

            @Override
            public HRegionInfo getRegionInfo() {
                return s.getRegionInfo();
            }

            @Override
            public boolean next(List<KeyValue> results) throws IOException {
                try {
                    if (isFilterDone()) {
                        return false;
                    }
                    
                    for (int i = 0; i < tuple.size(); i++) {
                        results.add(tuple.getValue(i));
                    }
                    
                    tuple = iterator.next();
                    return !isFilterDone();
                } catch (Throwable t) {
                    ServerUtil.throwIOException(region.getRegionNameAsString(), t);
                    return false;
                }
            }

            @Override
            public void close() throws IOException {
                try {
                    s.close();
                } finally {
                    chunk.close();                }
            }
        };
    }
        
    /**
     * Return wrapped scanner that catches unexpected exceptions (i.e. Phoenix bugs) and
     * re-throws as DoNotRetryIOException to prevent needless retrying hanging the query
     * for 30 seconds. Unfortunately, until HBASE-7481 gets fixed, there's no way to do
     * the same from a custom filter.
     */
    private RegionScanner getWrappedScanner(final ObserverContext<RegionCoprocessorEnvironment> c, final RegionScanner s) {
        return new RegionScanner() {

            @Override
            public boolean next(List<KeyValue> results) throws IOException {
                try {
                    return s.next(results);
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionNameAsString(), t);
                    return false; // impossible
                }
            }

            @Override
            public boolean next(List<KeyValue> results, String metric) throws IOException {
                try {
                    return s.next(results, metric);
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionNameAsString(), t);
                    return false; // impossible
                }
            }

            @Override
            public boolean next(List<KeyValue> result, int limit) throws IOException {
                try {
                    return s.next(result, limit);
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionNameAsString(), t);
                    return false; // impossible
                }
            }

            @Override
            public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
                try {
                    return s.next(result, limit, metric);
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionNameAsString(), t);
                    return false; // impossible
                }
            }

            @Override
            public void close() throws IOException {
                s.close();
            }

            @Override
            public HRegionInfo getRegionInfo() {
                return s.getRegionInfo();
            }

            @Override
            public boolean isFilterDone() {
                return s.isFilterDone();
            }

            @Override
            public boolean reseek(byte[] row) throws IOException {
                return s.reseek(row);
            }
            
            @Override
            public long getMvccReadPoint() {
                return s.getMvccReadPoint();
            }

            @Override
            public boolean nextRaw(List<KeyValue> result, String metric) throws IOException {
                try {
                    return s.nextRaw(result, metric);
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionNameAsString(), t);
                    return false; // impossible
                }
            }

            @Override
            public boolean nextRaw(List<KeyValue> result, int limit, String metric) throws IOException {
                try {
                    return s.nextRaw(result, limit, metric);
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionNameAsString(), t);
                    return false; // impossible
                }
            }
        };
    }

}
