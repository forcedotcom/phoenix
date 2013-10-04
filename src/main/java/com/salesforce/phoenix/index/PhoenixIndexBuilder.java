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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.Lists;
import com.salesforce.hbase.index.covered.CoveredColumnsIndexBuilder;
import com.salesforce.hbase.index.util.IndexManagementUtil;
import com.salesforce.phoenix.compile.ScanRanges;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.SchemaUtil;

/**
 * Index builder for covered-columns index that ties into phoenix for faster use.
 */
public class PhoenixIndexBuilder extends CoveredColumnsIndexBuilder {

    @Override
    public void batchStarted(MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp) throws IOException {
        // The entire purpose of this method impl is to get the existing rows for the
        // table rows being indexed into the block cache, as the index maintenance code
        // does a point scan per row
        List<KeyRange> keys = Lists.newArrayListWithExpectedSize(miniBatchOp.size());
        List<IndexMaintainer> maintainers = new ArrayList<IndexMaintainer>();
        for (int i = 0; i < miniBatchOp.size(); i++) {
            Mutation m = miniBatchOp.getOperation(i).getFirst();
            keys.add(PDataType.VARBINARY.getKeyRange(m.getRow()));
            maintainers.addAll(getCodec().getIndexMaintainers(m.getAttributesMap()));
        }
        Scan scan = IndexManagementUtil.newLocalStateScan(maintainers);
        ScanRanges scanRanges = ScanRanges.create(Collections.singletonList(keys), SchemaUtil.VAR_BINARY_SCHEMA);
        scanRanges.setScanStartStopRow(scan);
        scan.setFilter(scanRanges.getSkipScanFilter());
        HRegion region = this.env.getRegion();
        RegionScanner scanner = region.getScanner(scan);
        // Run through the scanner using internal nextRaw method
        MultiVersionConsistencyControl.setThreadReadPoint(scanner.getMvccReadPoint());
        region.startRegionOperation();
        try {
            boolean hasMore;
            do {
                List<KeyValue> results = Lists.newArrayList();
                // Results are potentially returned even when the return value of s.next is false
                // since this is an indication of whether or not there are more values after the
                // ones returned
                hasMore = scanner.nextRaw(results, null) && !scanner.isFilterDone();
            } while (hasMore);
        } finally {
            try {
                scanner.close();
            } finally {
                region.closeRegionOperation();
            }
        }
    }

    private PhoenixIndexCodec getCodec() {
        return (PhoenixIndexCodec)this.codec;
    }
    
    @Override
    public byte[] getBatchId(Mutation m){
        return this.codec.getBatchId(m);
    }
}