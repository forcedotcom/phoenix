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
import java.util.concurrent.*;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.*;
import com.salesforce.phoenix.job.JobManager.JobCallable;
import com.salesforce.phoenix.parse.FilterableStatement;
import com.salesforce.phoenix.parse.HintNode;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.*;


/**
 *
 * Class that parallelizes the scan over a table using the ExecutorService provided.  Each region of the table will be scanned in parallel with
 * the results accessible through {@link #getIterators()}
 *
 * @author jtaylor
 * @since 0.1
 */
public class ParallelIterators extends ExplainTable implements ResultIterators {
	private static final Logger logger = LoggerFactory.getLogger(ParallelIterators.class);
    private final List<KeyRange> splits;
    private final ParallelIteratorFactory iteratorFactory;
    
    public static interface ParallelIteratorFactory {
        PeekingResultIterator newIterator(ResultIterator scanner) throws SQLException;
    }

    private static final int DEFAULT_THREAD_TIMEOUT_MS = 60000; // 1min

    static final Function<Map.Entry<HRegionInfo, ServerName>, KeyRange> TO_KEY_RANGE = new Function<Map.Entry<HRegionInfo, ServerName>, KeyRange>() {
        @Override
        public KeyRange apply(Map.Entry<HRegionInfo, ServerName> region) {
            return KeyRange.getKeyRange(region.getKey().getStartKey(), region.getKey().getEndKey());
        }
    };

    public ParallelIterators(StatementContext context, TableRef tableRef, FilterableStatement statement, RowProjector projector, GroupBy groupBy, Integer limit, ParallelIteratorFactory iteratorFactory) throws SQLException {
        super(context, tableRef, groupBy);
        this.splits = getSplits(context, tableRef, statement.getHint());
        this.iteratorFactory = iteratorFactory;
        Scan scan = context.getScan();
        PTable table = tableRef.getTable();
        if (projector.isProjectEmptyKeyValue()) {
            Map<byte [], NavigableSet<byte []>> familyMap = scan.getFamilyMap();
            // If nothing projected into scan and we only have one column family, just allow everything
            // to be projected and use a FirstKeyOnlyFilter to skip from row to row. This turns out to
            // be quite a bit faster.
            if (familyMap.isEmpty() && table.getColumnFamilies().size() == 1) {
                // Project the one column family. We must project a column family since it's possible
                // that there are other non declared column families that we need to ignore.
                scan.addFamily(table.getColumnFamilies().get(0).getName().getBytes());
                ScanUtil.andFilterAtBeginning(scan, new FirstKeyOnlyFilter());
            } else {
                byte[] ecf = SchemaUtil.getEmptyColumnFamily(table.getColumnFamilies());
                // Project empty key value unless the column family containing it has
                // been projected in its entirety.
                if (!familyMap.containsKey(ecf) || familyMap.get(ecf) != null) {
                    scan.addColumn(ecf, QueryConstants.EMPTY_COLUMN_BYTES);
                }
            }
        }
        if (limit != null) {
            ScanUtil.andFilterAtEnd(scan, new PageFilter(limit));
        }
    }

    /**
     * Splits the given scan's key range so that each split can be queried in parallel
     * @param hintNode TODO
     *
     * @return the key ranges that should be scanned in parallel
     */
    // exposed for tests
    public static List<KeyRange> getSplits(StatementContext context, TableRef table, HintNode hintNode) throws SQLException {
        return ParallelIteratorRegionSplitterFactory.getSplitter(context, table, hintNode).getSplits();
    }

    public List<KeyRange> getSplits() {
        return splits;
    }

    /**
     * Executes the scan in parallel across all regions, blocking until all scans are complete.
     * @return the result iterators for the scan of each region
     */
    @Override
    public List<PeekingResultIterator> getIterators() throws SQLException {
        boolean success = false;
        final ConnectionQueryServices services = context.getConnection().getQueryServices();
        ReadOnlyProps props = services.getProps();
        try {
            int numSplits = splits.size();
            List<PeekingResultIterator> iterators = new ArrayList<PeekingResultIterator>(numSplits);
            List<Pair<byte[],Future<PeekingResultIterator>>> futures = new ArrayList<Pair<byte[],Future<PeekingResultIterator>>>(numSplits);
            final UUID scanId = UUID.randomUUID();
            try {
                ExecutorService executor = services.getExecutor();
                for (final KeyRange split : splits) {
                    final Scan splitScan = new Scan(this.context.getScan());
                    // Intersect with existing start/stop key
                    if (ScanUtil.intersectScanRange(splitScan, split.getLowerRange(), split.getUpperRange(), this.context.getScanRanges().useSkipScanFilter())) {
                        Future<PeekingResultIterator> future =
                            executor.submit(new JobCallable<PeekingResultIterator>() {
    
                            @Override
                            public PeekingResultIterator call() throws Exception {
                                // TODO: different HTableInterfaces for each thread or the same is better?
                            	long startTime = System.currentTimeMillis();
                                ResultIterator scanner = new TableResultIterator(context, table, splitScan);
                                if (logger.isDebugEnabled()) {
                                	logger.debug("Id: " + scanId + ", Time: " + (System.currentTimeMillis() - startTime) + "ms, Scan: " + split);
                                }
                                return iteratorFactory.newIterator(scanner);
                            }
    
                            /**
                             * Defines the grouping for round robin behavior.  All threads spawned to process
                             * this scan will be grouped together and time sliced with other simultaneously
                             * executing parallel scans.
                             */
                            @Override
                            public Object getJobId() {
                                return ParallelIterators.this;
                            }
                        });
                        futures.add(new Pair<byte[],Future<PeekingResultIterator>>(split.getLowerRange(),future));
                    }
                }

                int timeoutMs = props.getInt(QueryServices.THREAD_TIMEOUT_MS_ATTRIB, DEFAULT_THREAD_TIMEOUT_MS);
                // Sort futures by row key so that we have a predicatble order we're getting rows back for scans.
                // We're going to wait here until they're finished anyway and this makes testing much easier.
                Collections.sort(futures, new Comparator<Pair<byte[],Future<PeekingResultIterator>>>() {
                    @Override
                    public int compare(Pair<byte[], Future<PeekingResultIterator>> o1, Pair<byte[], Future<PeekingResultIterator>> o2) {
                        return Bytes.compareTo(o1.getFirst(), o2.getFirst());
                    }
                });
                for (Pair<byte[],Future<PeekingResultIterator>> future : futures) {
                    iterators.add(future.getSecond().get(timeoutMs, TimeUnit.MILLISECONDS));
                }

                success = true;
                return iterators;
            } finally {
                if (!success) {
                    for (Pair<byte[],Future<PeekingResultIterator>> future : futures) {
                        future.getSecond().cancel(true);
                    }
                    SQLCloseables.closeAllQuietly(iterators);
                }
            }
        } catch (Exception e) {
            throw ServerUtil.parseServerException(e);
        }
    }

    @Override
    public int size() {
        return this.splits.size();
    }

    @Override
    public void explain(List<String> planSteps) {
        StringBuilder buf = new StringBuilder();
        buf.append("CLIENT PARALLEL " + size() + "-WAY ");
        explain(buf.toString(),planSteps);
    }
}
