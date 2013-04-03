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

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.salesforce.phoenix.filter.SkipScanFilter;
import com.salesforce.phoenix.query.*;


/**
 * Split the region according to the information contained in the scan's SkipScanFilter.
 */
public class SkipRangeParallelIteratorRegionSplitter implements ParallelIteratorRegionSplitter {

    private final int targetConcurrency;
    private final int maxConcurrency;
    private final Scan scan;

    public static SkipRangeParallelIteratorRegionSplitter getInstance(ConnectionQueryServices services, Scan scan) {
        return new SkipRangeParallelIteratorRegionSplitter(services, scan);
    }

    private SkipRangeParallelIteratorRegionSplitter(ConnectionQueryServices services, Scan scan) {
        Configuration config = services.getConfig();
        targetConcurrency = config.getInt(QueryServices.TARGET_QUERY_CONCURRENCY_ATTRIB,
                QueryServicesOptions.DEFAULT_TARGET_QUERY_CONCURRENCY);
        maxConcurrency = config.getInt(QueryServices.MAX_QUERY_CONCURRENCY_ATTRIB,
                QueryServicesOptions.DEFAULT_MAX_QUERY_CONCURRENCY);
        Preconditions.checkArgument(targetConcurrency >= 1, "Invalid target concurrency: " + targetConcurrency);
        Preconditions.checkArgument(maxConcurrency >= targetConcurrency , "Invalid max concurrency: " + maxConcurrency);
        this.scan = scan;
    }

    @Override
    public List<KeyRange> getSplits() {
        // The split strategies are split as follows:
        //
        // let's suppose:
        // t = target concurrency
        // m = max concurrency
        // r = estimate key range from SkipScanFilters
        //
        // if r >= m:
        //   s = ceil(m / r)
        //   combine s keyRange into one scan range
        // elif r/2 > t:
        //   split each key range into s splits such that:
        //   s = max(x) where s * x < m
        // else:
        //   split each key range into s splits such that:
        //   s = max(x) where s * x < t
        
        List<KeyRange> keyRanges = ((SkipScanFilter) scan.getFilter()).generateSplitRanges(maxConcurrency);
        if (keyRanges.size() >= targetConcurrency) {
            // The splits as returned from SkipScanFilter already defined the split regions. We do
            // not need to further split each keyRange into sub ranges.
            Collections.shuffle(keyRanges);
            return keyRanges;
        }
        int splitsPerRange = (keyRanges.size() > targetConcurrency / 2 ? maxConcurrency : targetConcurrency) / keyRanges.size();
        List<KeyRange> splits = Lists.newArrayListWithCapacity(keyRanges.size() * splitsPerRange);
        for (KeyRange range: keyRanges) {
            byte[] startKey = range.getLowerRange();
            byte[] stopKey = range.getUpperRange();
            boolean lowerUnbound = Bytes.compareTo(startKey, HConstants.EMPTY_START_ROW) == 0;
            boolean upperUnbound = Bytes.compareTo(stopKey, HConstants.EMPTY_END_ROW) == 0;
            
            byte[][] boundaries = null;
            if (Bytes.compareTo(startKey, stopKey) >= 0 || 
                    (boundaries = Bytes.split(startKey, stopKey, splitsPerRange - 1)) == null) {
                // Bytes.split may return null if the key space between start and end key is too small.
                splits.add(range);
            } else {
                KeyRange subRange = KeyRange.getKeyRange(lowerUnbound ? HConstants.EMPTY_START_ROW :
                    boundaries[0], true, boundaries[1], false);
                splits.add(subRange);
                if (boundaries.length > 1) {
                    for (int i = 1; i < boundaries.length-2; i++) {
                        subRange = KeyRange.getKeyRange(boundaries[i], true, boundaries[i+1], false);
                        splits.add(subRange);
                    }
                    subRange = KeyRange.getKeyRange(boundaries[boundaries.length-2], true, 
                            upperUnbound ? HConstants.EMPTY_END_ROW : boundaries[boundaries.length-1], false);
                    splits.add(subRange);
                }
            }
        }
        // Shuffle it. Despite the fact that the regions are not being tagged with region ID, we would
        // still gain some performance gain from this random shuffle or key ranges.
        Collections.shuffle(splits);
        return splits;
    }
}