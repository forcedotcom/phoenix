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

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;


import com.google.common.collect.Lists;
import com.salesforce.phoenix.compile.StatementContext;
import com.salesforce.phoenix.exception.*;
import com.salesforce.phoenix.execute.RowCounter;
import com.salesforce.phoenix.memory.MemoryManager;
import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.SQLCloseables;
import com.salesforce.phoenix.util.ScanUtil;


/**
 * 
 * Class that executes scans serially for each region in descending order.  Used
 * for scans that have a row count limit and in the most common case are looking
 * for newer rows.
 *
 * @author jtaylor
 * @since 0.1
 */
public class SerialLimitingIterators extends ExplainTable implements ResultIterators {
    private static final int DEFAULT_SPOOL_THRESHOLD_BYTES = 1024 * 100; // 100K

    private final long limit;
    private final RowCounter rowCounter;
    private final List<Scan> regionScans;

    
    public SerialLimitingIterators(StatementContext context, TableRef table, long limit, RowCounter rowCounter) throws SQLException {
        super(context, table);
        this.limit = limit;
        this.rowCounter = rowCounter;
        Set<HRegionInfo> regions = context.getConnection().getQueryServices().getAllTableRegions(this.table);
        regionScans = Lists.newArrayListWithExpectedSize(regions.size());
        for (HRegionInfo region : regions) {
            Scan regionScan;
            try {
                regionScan = new Scan(context.getScan());
            } catch (IOException e) {
                throw new PhoenixIOException(e);
            }
            // Intersect with existing start/stop key
            if (ScanUtil.intersectScanRange(regionScan, region.getStartKey(), region.getEndKey())) {
                regionScans.add(regionScan);
            }
        }
    }
    
    @Override
    public List<PeekingResultIterator> getIterators() throws SQLException {
        ConnectionQueryServices services = context.getConnection().getQueryServices();
        Configuration config = services.getConfig();
        List<PeekingResultIterator> iterators = new ArrayList<PeekingResultIterator>(regionScans.size());
        MemoryManager mm = services.getMemoryManager();
        int spoolThresholdBytes = config.getInt(QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, DEFAULT_SPOOL_THRESHOLD_BYTES);
        boolean success = false;
        long rowCount = 0;
        try {
            for (Scan regionScan : regionScans) {
                ScanUtil.andFilter(regionScan, new PageFilter(limit - rowCount));
                ResultIterator scanner = new TableResultIterator(context, this.table, regionScan);
                SpoolingResultIterator iterator = new SpoolingResultIterator(scanner, mm, spoolThresholdBytes, rowCounter);
                rowCount += iterator.getRowCount();
                iterators.add(iterator);
                assert(rowCount <= limit);
                if (rowCount == limit) {
                    break;
                }
            }
            success = true;
            return iterators;
        } finally {
            if (!success) {
                SQLCloseables.closeAllQuietly(iterators);
            }
        }
    }

    @Override
    public int size() {
        return regionScans.size();
    }


    @Override
    public void explain(List<String> planSteps) {
        StringBuilder buf = new StringBuilder();
        buf.append("CLIENT SERIAL " + limit + " ROW LIMIT ");
        explain(buf.toString(),planSteps);
    }
}
