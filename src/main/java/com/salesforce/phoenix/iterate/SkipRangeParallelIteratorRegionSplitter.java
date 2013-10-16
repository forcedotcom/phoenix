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
import java.util.List;

import org.apache.hadoop.hbase.HRegionLocation;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.salesforce.phoenix.compile.ScanRanges;
import com.salesforce.phoenix.compile.StatementContext;
import com.salesforce.phoenix.parse.HintNode;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.schema.SaltingUtil;
import com.salesforce.phoenix.schema.TableRef;


/**
 * Split the region according to the information contained in the scan's SkipScanFilter.
 */
public class SkipRangeParallelIteratorRegionSplitter extends DefaultParallelIteratorRegionSplitter {

    public static SkipRangeParallelIteratorRegionSplitter getInstance(StatementContext context, TableRef table, HintNode hintNode) {
        return new SkipRangeParallelIteratorRegionSplitter(context, table, hintNode);
    }

    protected SkipRangeParallelIteratorRegionSplitter(StatementContext context, TableRef table, HintNode hintNode) {
        super(context, table, hintNode);
    }

    @Override
    protected List<HRegionLocation> getAllRegions() throws SQLException {
        List<HRegionLocation> allTableRegions = context.getConnection().getQueryServices().getAllTableRegions(tableRef.getTable().getName().getBytes());
        return filterRegions(allTableRegions, context.getScanRanges());
    }

    public List<HRegionLocation> filterRegions(List<HRegionLocation> allTableRegions, final ScanRanges ranges) {
        Iterable<HRegionLocation> regions;
        if (ranges == ScanRanges.EVERYTHING) {
            return allTableRegions;
        } else if (ranges == ScanRanges.NOTHING) { // TODO: why not emptyList?
            return Lists.<HRegionLocation>newArrayList();
        } else {
            regions = Iterables.filter(allTableRegions,
                    new Predicate<HRegionLocation>() {
                    @Override
                    public boolean apply(HRegionLocation region) {
                        KeyRange minMaxRange = context.getMinMaxRange();
                        if (minMaxRange != null) {
                            KeyRange range = KeyRange.getKeyRange(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey());
                            if (tableRef.getTable().getBucketNum() != null) {
                                // Add salt byte, as minMaxRange won't have it
                                minMaxRange = SaltingUtil.addSaltByte(region.getRegionInfo().getStartKey(), minMaxRange);
                            }
                            range = range.intersect(minMaxRange);
                            return ranges.intersect(range.getLowerRange(), range.getUpperRange());
                        }
                        return ranges.intersect(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey());
                    }
            });
        }
        return Lists.newArrayList(regions);
    }

}
