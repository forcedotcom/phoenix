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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.query.QueryServicesOptions;
import com.salesforce.phoenix.schema.TableRef;


/**
 * Default strategy for splitting regions in ParallelIterator. Refactored from the
 * original version.
 * 
 * @author jtaylor
 * @author zhuang
 */
public class DefaultParallelIteratorRegionSplitter implements ParallelIteratorRegionSplitter {

    private final ConnectionQueryServices services;
    private final TableRef table;
    private final Scan scan;
    private final SortedSet<HRegionInfo> allTableRegions;

    public static DefaultParallelIteratorRegionSplitter getInstance(ConnectionQueryServices services, 
            TableRef table, Scan scan, SortedSet<HRegionInfo> allTableRegions) {
        return new DefaultParallelIteratorRegionSplitter(services, table, scan, allTableRegions);
    }

    private DefaultParallelIteratorRegionSplitter(ConnectionQueryServices services, 
            TableRef table, Scan scan, SortedSet<HRegionInfo> allTableRegions) {
        this.services = services;
        this.table = table;
        this.scan = scan;
        this.allTableRegions = allTableRegions;
    }

    @Override
    public List<KeyRange> getSplits() {
        Configuration config = services.getConfig();
        final int targetConcurrency = config.getInt(QueryServices.TARGET_QUERY_CONCURRENCY_ATTRIB,
                QueryServicesOptions.DEFAULT_TARGET_QUERY_CONCURRENCY);
        final int maxConcurrency = config.getInt(QueryServices.MAX_QUERY_CONCURRENCY_ATTRIB,
                QueryServicesOptions.DEFAULT_MAX_QUERY_CONCURRENCY);
        
        Preconditions.checkArgument(targetConcurrency >= 1, "Invalid target concurrency: " + targetConcurrency);
        Preconditions.checkArgument(maxConcurrency >= targetConcurrency , "Invalid max concurrency: " + maxConcurrency);
        
        // the splits are computed as follows:
        //
        // let's suppose:
        // t = target concurrency
        // m = max concurrency
        // r = the number of regions we need to scan
        //
        // if r >= t:
        //    scan using regional boundaries
        // elif r/2 > t:
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
        
        List<HRegionInfo> regions = ParallelIterators.filterRegions(allTableRegions, scan.getStartRow(), scan.getStopRow());
        if (regions.isEmpty()) {
            return Collections.emptyList();
        }
        
        int splitsPerRegion = regions.size() >= targetConcurrency ? 1 : (regions.size() > targetConcurrency / 2 ? maxConcurrency : targetConcurrency) / regions.size();
        ListMultimap<Long,KeyRange> keyRangesPerRegion = ArrayListMultimap.create(regions.size(),regions.size() * splitsPerRegion);;
        if (regions.size() >= targetConcurrency) {
            for (HRegionInfo region : regions) {
                keyRangesPerRegion.put(region.getRegionId(), ParallelIterators.TO_KEY_RANGE.apply(region));
            }
        } else {
            assert splitsPerRegion >= 2 : "Splits per region has to be greater than 2";
            
            // Maintain bucket for each server and then returns KeyRanges in round-robin
            // order to ensure all servers are utilized.
            for (HRegionInfo region : regions) {
                byte[] startKey = region.getStartKey();
                byte[] stopKey = region.getEndKey();
                boolean lowerUnbound = Bytes.compareTo(startKey, HConstants.EMPTY_START_ROW) == 0;
                boolean upperUnbound = Bytes.compareTo(stopKey, HConstants.EMPTY_END_ROW) == 0;
                /*
                 * If lower/upper unbound, get the min/max key from the stats manager.
                 * We use this as the boundary to split on, but we still use the empty
                 * byte as the boundary in the actual scan (in case our stats are out
                 * of date).
                 */
                if (lowerUnbound) {
                    startKey = services.getStatsManager().getMinKey(table);
                    if (startKey == null) {
                        keyRangesPerRegion.put(region.getRegionId(),ParallelIterators.TO_KEY_RANGE.apply(region));
                        continue;
                    }
                }
                if (upperUnbound) {
                    stopKey = services.getStatsManager().getMaxKey(table);
                    if (stopKey == null) {
                        keyRangesPerRegion.put(region.getRegionId(),ParallelIterators.TO_KEY_RANGE.apply(region));
                        continue;
                    }
                }
                
                byte[][] boundaries = null;
                // Both startKey and stopKey will be empty the first time
                if (Bytes.compareTo(startKey, stopKey) >= 0 || (boundaries = Bytes.split(startKey, stopKey, splitsPerRegion - 1)) == null) {
                    // Bytes.split may return null if the key space
                    // between start and end key is too small
                    keyRangesPerRegion.put(region.getRegionId(),ParallelIterators.TO_KEY_RANGE.apply(region));
                } else {
                    keyRangesPerRegion.put(region.getRegionId(),KeyRange.getKeyRange(lowerUnbound ? HConstants.EMPTY_START_ROW : boundaries[0], true, boundaries[1], false));
                    if (boundaries.length > 1) {
                        for (int i = 1; i < boundaries.length-2; i++) {
                            keyRangesPerRegion.put(region.getRegionId(),KeyRange.getKeyRange(boundaries[i], true, boundaries[i+1], false));
                        }
                        keyRangesPerRegion.put(region.getRegionId(),KeyRange.getKeyRange(boundaries[boundaries.length-2], true, upperUnbound ? HConstants.EMPTY_END_ROW : boundaries[boundaries.length-1], false));
                    }
                }
            }
        }
        List<KeyRange> splits = Lists.newArrayListWithCapacity(regions.size() * splitsPerRegion);
        // as documented for ListMultimap
        Collection<Collection<KeyRange>> values = keyRangesPerRegion.asMap().values();
        List<Collection<KeyRange>> keyRangesList = Lists.newArrayList(values);
        // Randomize range order to help with even distribution
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
}
