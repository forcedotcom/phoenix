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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;

import com.google.common.collect.ImmutableList;
import com.salesforce.phoenix.filter.SkipScanFilter;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.schema.TableRef;


/**
 * Split the region according to the information contained in the scan's SkipScanFilter.
 */
public class SkipRangeParallelIteratorRegionSplitter extends DefaultParallelIteratorRegionSplitter {

    public static SkipRangeParallelIteratorRegionSplitter getInstance(ConnectionQueryServices services,
            TableRef table, Scan scan, SortedSet<HRegionInfo> allTableRegions) {
        return new SkipRangeParallelIteratorRegionSplitter(services, table, scan, allTableRegions);
    }

    protected SkipRangeParallelIteratorRegionSplitter(ConnectionQueryServices services, 
            TableRef table, Scan scan, SortedSet<HRegionInfo> allTableRegions) {
        super(services, table, scan, allTableRegions);
    }

    @Override
    protected List<HRegionInfo> getAllRegions() {
        Set<HRegionInfo> allRegions = new HashSet<HRegionInfo>();
        List<KeyRange> keyRanges = ((SkipScanFilter) scan.getFilter()).generateSplitRanges(maxConcurrency);
        for (KeyRange range: keyRanges) {
            List<HRegionInfo> regions = ParallelIterators.filterRegions(allTableRegions, range.getLowerRange(), range.getUpperRange());
            allRegions.addAll(regions);
        }
        return ImmutableList.<HRegionInfo>copyOf(allRegions);
    }

}
