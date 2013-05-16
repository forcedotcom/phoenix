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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;

import com.google.common.collect.Lists;
import com.salesforce.phoenix.compile.StatementContext;
import com.salesforce.phoenix.execute.ScanRowCounter;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ScanUtil;

/**
 * 
 * ResultIterators that has one serial iterator per salt bucket. Used to do a merge
 * sort against to guarantee that row key order traversal matches non salted tables.
 *
 * @author jtaylor
 * @since 1.2
 */
public class SaltingSerialIterators implements ResultIterators {
    private final List<PeekingResultIterator> iterators;

    public SaltingSerialIterators(StatementContext context, TableRef tableRef, Integer limit) throws SQLException {
        PTable table = tableRef.getTable();
        byte[] startKey = HConstants.EMPTY_START_ROW;
        byte[] endKey = HConstants.EMPTY_END_ROW;
        int i = 0;
        int nBuckets = table.getBucketNum();
        final List<PeekingResultIterator> iterators = Lists.newArrayListWithExpectedSize(nBuckets);
        while (++i < nBuckets) {
            endKey = new byte[] {(byte)i};
            addIterator(context, tableRef, limit, startKey, endKey, iterators);
            startKey = endKey;
        }
        addIterator(context, tableRef, limit, startKey, HConstants.EMPTY_END_ROW, iterators);
        this.iterators = iterators;
    }
    
    private static void addIterator(StatementContext context, TableRef tableRef, Integer limit,
            byte[] startKey, byte[] endKey, List<PeekingResultIterator> iterators) throws SQLException {
        Scan scan = ScanUtil.newScan(context.getScan());
        if (ScanUtil.intersectScanRange(scan, startKey, endKey)) {
            final ResultIterator delegate = new SerialLimitingResultIterator(new TableResultIterator(context, tableRef, scan), limit, new ScanRowCounter());
            iterators.add(new LookAheadResultIterator() {

                @Override
                public void explain(List<String> planSteps) {
                    delegate.explain(planSteps);
                }

                @Override
                public void close() throws SQLException {
                    delegate.close();
                }

                @Override
                protected Tuple advance() throws SQLException {
                    return delegate.next();
                }
                
            });
        }
    }

    @Override
    public List<PeekingResultIterator> getIterators() throws SQLException {
        return iterators;
    }

    @Override
    public int size() {
        return iterators.size();
    }

    @Override
    public void explain(List<String> planSteps) {
        if (!iterators.isEmpty()) {
            iterators.get(0).explain(planSteps);
        }
    }

}
