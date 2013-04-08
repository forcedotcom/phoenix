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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.salesforce.phoenix.compile.ScanRanges;
import com.salesforce.phoenix.compile.StatementContext;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.schema.TableRef;


/**
 * Split the region according to the information contained in the scan's SkipScanFilter.
 */
public class SkipRangeParallelIteratorRegionSplitter extends DefaultParallelIteratorRegionSplitter {

    public static SkipRangeParallelIteratorRegionSplitter getInstance(StatementContext context, TableRef table) {
        return new SkipRangeParallelIteratorRegionSplitter(context, table);
    }

    protected SkipRangeParallelIteratorRegionSplitter(StatementContext context, TableRef table) {
        super(context, table);
    }

    @Override
    protected List<Map.Entry<HRegionInfo, ServerName>> getAllRegions() throws SQLException {
        NavigableMap<HRegionInfo, ServerName> allTableRegions = context.getConnection().getQueryServices().getAllTableRegions(table);
        return filterRegions(allTableRegions, context.getScanRanges());
    }

    public static List<Map.Entry<HRegionInfo, ServerName>> filterRegions(NavigableMap<HRegionInfo, ServerName> allTableRegions, final ScanRanges ranges) {
        Iterable<Map.Entry<HRegionInfo, ServerName>> regions;
        if (ranges == ScanRanges.EVERYTHING) {
            regions = allTableRegions.entrySet();
        } else if (ranges == ScanRanges.NOTHING) {
            return Lists.<Map.Entry<HRegionInfo, ServerName>>newArrayList();
        } else {
            regions = Iterables.filter(allTableRegions.entrySet(),
                    new Predicate<Map.Entry<HRegionInfo, ServerName>>() {
                    @Override
                    public boolean apply(Map.Entry<HRegionInfo, ServerName> region) {
                        KeyRange regionKeyRange = KeyRange.getKeyRange(region.getKey());
                        return ranges.intersect(regionKeyRange);
                    }
            });
        }
        return Lists.newArrayList(regions);
    }

}
