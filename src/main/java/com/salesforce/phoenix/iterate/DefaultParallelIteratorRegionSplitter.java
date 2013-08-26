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
package com.salesforce.phoenix.iterate;

import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.salesforce.phoenix.compile.StatementContext;
import com.salesforce.phoenix.parse.HintNode;
import com.salesforce.phoenix.parse.HintNode.Hint;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.ReadOnlyProps;


/**
 * Default strategy for splitting regions in ParallelIterator. Refactored from the
 * original version.
 * 
 * @author jtaylor
 * @author zhuang
 */
public class DefaultParallelIteratorRegionSplitter implements ParallelIteratorRegionSplitter {

    protected final int targetConcurrency;
    protected final int maxConcurrency;
    protected final int maxIntraRegionParallelization;
    protected final StatementContext context;
    protected final TableRef table;

    public static DefaultParallelIteratorRegionSplitter getInstance(StatementContext context, TableRef table, HintNode hintNode) {
        return new DefaultParallelIteratorRegionSplitter(context, table, hintNode);
    }

    protected DefaultParallelIteratorRegionSplitter(StatementContext context, TableRef table, HintNode hintNode) {
        this.context = context;
        this.table = table;
        ReadOnlyProps props = context.getConnection().getQueryServices().getProps();
        this.targetConcurrency = props.getInt(QueryServices.TARGET_QUERY_CONCURRENCY_ATTRIB,
                QueryServicesOptions.DEFAULT_TARGET_QUERY_CONCURRENCY);
        this.maxConcurrency = props.getInt(QueryServices.MAX_QUERY_CONCURRENCY_ATTRIB,
                QueryServicesOptions.DEFAULT_MAX_QUERY_CONCURRENCY);
        Preconditions.checkArgument(targetConcurrency >= 1, "Invalid target concurrency: " + targetConcurrency);
        Preconditions.checkArgument(maxConcurrency >= targetConcurrency , "Invalid max concurrency: " + maxConcurrency);
        this.maxIntraRegionParallelization = hintNode.hasHint(Hint.NO_INTRA_REGION_PARALLELIZATION) ? 1 : props.getInt(QueryServices.MAX_INTRA_REGION_PARALLELIZATION_ATTRIB,
                QueryServicesOptions.DEFAULT_MAX_INTRA_REGION_PARALLELIZATION);
        Preconditions.checkArgument(maxIntraRegionParallelization >= 1 , "Invalid max intra region parallelization: " + maxIntraRegionParallelization);
    }

    // Get the mapping between key range and the regions that contains them.
    protected List<Entry<HRegionInfo, ServerName>> getAllRegions() throws SQLException {
        Scan scan = context.getScan();
        NavigableMap<HRegionInfo, ServerName> allTableRegions = context.getConnection().getQueryServices().getAllTableRegions(table);
        return filterRegions(allTableRegions, scan.getStartRow(), scan.getStopRow());
    }

    /**
     * Filters out regions that intersect with key range specified by the startKey and stopKey
     * @param allTableRegions all region infos for a given table
     * @param startKey the lower bound of key range, inclusive
     * @param stopKey the upper bound of key range, inclusive
     * @return regions that intersect with the key range given by the startKey and stopKey
     */
    // exposed for tests
    public static List<Map.Entry<HRegionInfo, ServerName>> filterRegions(NavigableMap<HRegionInfo, ServerName> allTableRegions, byte[] startKey, byte[] stopKey) {
        Iterable<Map.Entry<HRegionInfo, ServerName>> regions;
        final KeyRange keyRange = KeyRange.getKeyRange(startKey, true, stopKey, false);
        if (keyRange == KeyRange.EVERYTHING_RANGE) {
            regions = allTableRegions.entrySet();
        } else {
            regions = Iterables.filter(allTableRegions.entrySet(), new Predicate<Map.Entry<HRegionInfo, ServerName>>() {
                @Override
                public boolean apply(Map.Entry<HRegionInfo, ServerName> region) {
                    KeyRange regionKeyRange = KeyRange.getKeyRange(region.getKey().getStartKey(), region.getKey().getEndKey());
                    return keyRange.intersect(regionKeyRange) != KeyRange.EMPTY_RANGE;
                }
            });
        }
        return Lists.newArrayList(regions);
    }

    protected List<KeyRange> genKeyRanges(List<Map.Entry<HRegionInfo, ServerName>> regions) {
        if (regions.isEmpty()) {
            return Collections.emptyList();
        }
        
        StatsManager statsManager = context.getConnection().getQueryServices().getStatsManager();
        // the splits are computed as follows:
        //
        // let's suppose:
        // t = target concurrency
        // m = max concurrency
        // r = the number of regions we need to scan
        //
        // if r >= t:
        //    scan using regional boundaries
        // elif r > t/2:
        //    split each region in s splits such that:
        //    s = max(x) where s * x < m
        // else:
        //    split each region in s splits such that:
        //    s = max(x) where s * x < t
        //
        // The idea is to align splits with region boundaries. If rows are not evenly
        // distributed across regions, using this scheme compensates for regions that
        // have more rows than others, by applying tighter splits and therefore spawning
        // off more scans over the overloaded regions.
        int splitsPerRegion = regions.size() >= targetConcurrency ? 1 : (regions.size() > targetConcurrency / 2 ? maxConcurrency : targetConcurrency) / regions.size();
        splitsPerRegion = Math.min(splitsPerRegion, maxIntraRegionParallelization);
        // Create a multi-map of ServerName to List<KeyRange> which we'll use to round robin from to ensure
        // that we keep each region server busy for each query.
        ListMultimap<ServerName,KeyRange> keyRangesPerRegion = ArrayListMultimap.create(regions.size(),regions.size() * splitsPerRegion);;
        if (splitsPerRegion == 1) {
            for (Map.Entry<HRegionInfo, ServerName> region : regions) {
                keyRangesPerRegion.put(region.getValue(), ParallelIterators.TO_KEY_RANGE.apply(region));
            }
        } else {
            // Maintain bucket for each server and then returns KeyRanges in round-robin
            // order to ensure all servers are utilized.
            for (Map.Entry<HRegionInfo, ServerName> region : regions) {
                byte[] startKey = region.getKey().getStartKey();
                byte[] stopKey = region.getKey().getEndKey();
                boolean lowerUnbound = Bytes.compareTo(startKey, HConstants.EMPTY_START_ROW) == 0;
                boolean upperUnbound = Bytes.compareTo(stopKey, HConstants.EMPTY_END_ROW) == 0;
                /*
                 * If lower/upper unbound, get the min/max key from the stats manager.
                 * We use this as the boundary to split on, but we still use the empty
                 * byte as the boundary in the actual scan (in case our stats are out
                 * of date).
                 */
                if (lowerUnbound) {
                    startKey = statsManager.getMinKey(table);
                    if (startKey == null) {
                        keyRangesPerRegion.put(region.getValue(),ParallelIterators.TO_KEY_RANGE.apply(region));
                        continue;
                    }
                }
                if (upperUnbound) {
                    stopKey = statsManager.getMaxKey(table);
                    if (stopKey == null) {
                        keyRangesPerRegion.put(region.getValue(),ParallelIterators.TO_KEY_RANGE.apply(region));
                        continue;
                    }
                }
                
                byte[][] boundaries = null;
                // Both startKey and stopKey will be empty the first time
                if (Bytes.compareTo(startKey, stopKey) >= 0 || (boundaries = Bytes.split(startKey, stopKey, splitsPerRegion - 1)) == null) {
                    // Bytes.split may return null if the key space
                    // between start and end key is too small
                    keyRangesPerRegion.put(region.getValue(),ParallelIterators.TO_KEY_RANGE.apply(region));
                } else {
                    keyRangesPerRegion.put(region.getValue(),KeyRange.getKeyRange(lowerUnbound ? KeyRange.UNBOUND : boundaries[0], boundaries[1]));
                    if (boundaries.length > 1) {
                        for (int i = 1; i < boundaries.length-2; i++) {
                            keyRangesPerRegion.put(region.getValue(),KeyRange.getKeyRange(boundaries[i], true, boundaries[i+1], false));
                        }
                        keyRangesPerRegion.put(region.getValue(),KeyRange.getKeyRange(boundaries[boundaries.length-2], true, upperUnbound ? KeyRange.UNBOUND : boundaries[boundaries.length-1], false));
                    }
                }
            }
        }
        List<KeyRange> splits = Lists.newArrayListWithCapacity(regions.size() * splitsPerRegion);
        // as documented for ListMultimap
        Collection<Collection<KeyRange>> values = keyRangesPerRegion.asMap().values();
        List<Collection<KeyRange>> keyRangesList = Lists.newArrayList(values);
        // Randomize range order to ensure that we don't hit the region servers in the same order every time
        // thus helping to distribute the load more evenly
        Collections.shuffle(keyRangesList);
        // Transpose values in map to get regions in round-robin server order. This ensures that
        // all servers will be used to process the set of parallel threads available in our executor.
        int i = 0;
        boolean done;
        do {
            done = true;
            for (int j = 0; j < keyRangesList.size(); j++) {
                List<KeyRange> keyRanges = (List<KeyRange>)keyRangesList.get(j);
                if (i < keyRanges.size()) {
                    splits.add(keyRanges.get(i));
                    done = false;
                }
            }
            i++;
        } while (!done);
        return splits;
    }

    @Override
    public List<KeyRange> getSplits() throws SQLException {
        return genKeyRanges(getAllRegions());
    }
}
