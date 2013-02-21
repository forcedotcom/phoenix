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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.salesforce.phoenix.compile.StatementContext;
import com.salesforce.phoenix.execute.RowCounter;
import com.salesforce.phoenix.job.JobManager.JobCallable;
import com.salesforce.phoenix.memory.MemoryManager;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.SQLCloseables;
import com.salesforce.phoenix.util.ScanUtil;


/**
 *
 * Class that parallelizes the scan over a table using the ExecutorService provided.  Each region of the table will be scanned in parallel with
 * the results accessible through {@link #getIterators()}
 *
 * @author jtaylor
 * @since 0.1
 */
public class ParallelIterators extends ExplainTable implements ResultIterators {
    private final RowCounter rowCounter;
    private final List<KeyRange> splits;

    private static final int DEFAULT_THREAD_TIMEOUT_MS = 60000; // 1min
    private static final int DEFAULT_SPOOL_THRESHOLD_BYTES = 1024 * 100; // 100K

    private static final Function<HRegionInfo, KeyRange> TO_KEY_RANGE = new Function<HRegionInfo, KeyRange>() {
        @Override
        public KeyRange apply(HRegionInfo region) {
            return KeyRange.getKeyRange(region);
        }
    };

    public ParallelIterators(StatementContext context, TableRef table, RowCounter rowCounter) throws SQLException {
        super(context, table);
        this.rowCounter = rowCounter;
        this.splits = getSplits(context.getConnection().getQueryServices(), table, context.getScan(), context.getConnection().getQueryServices().getAllTableRegions(table));
    }

    /**
     * Filters out regions that intersect with key range specified by the startKey and stopKey
     * @param allTableRegions all region infos for a given table
     * @param startKey the lower bound of key range, inclusive
     * @param stopKey the upper bound of key range, inclusive
     * @return regions that intersect with the key range given by the startKey and stopKey
     */
    // exposed for tests
    static List<HRegionInfo> filterRegions(SortedSet<HRegionInfo> allTableRegions, byte[] startKey, byte[] stopKey) {
        Iterable<HRegionInfo> regions;
        final KeyRange keyRange = KeyRange.getKeyRange(startKey, true, stopKey, false);
        if (keyRange == KeyRange.EVERYTHING_RANGE) {
            regions = allTableRegions;
        } else {
            regions = Iterables.filter(allTableRegions, new Predicate<HRegionInfo>() {
                @Override
                public boolean apply(HRegionInfo region) {
                    KeyRange regionKeyRange = KeyRange.getKeyRange(region);
                    return keyRange.intersect(regionKeyRange) != KeyRange.EMPTY_RANGE;
                }
            });
        }
        return Lists.newArrayList(regions);
    }

    /**
     * Splits the given scan's key range so that each split can be queried in parallel
     *
     * @param scan the scan to parallelize
     * @param allTableRegions all online regions for the table to be scanned
     * @return the key ranges that should be scanned in parallel
     */
    // exposed for tests
    public static List<KeyRange> getSplits(ConnectionQueryServices services, TableRef table, Scan scan,SortedSet<HRegionInfo> allTableRegions) {
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

        List<HRegionInfo> regions = filterRegions(allTableRegions, scan.getStartRow(), scan.getStopRow());
        if (regions.isEmpty()) {
            return Collections.emptyList();
        }

        int splitsPerRegion = regions.size() >= targetConcurrency ? 1 : (regions.size() > targetConcurrency / 2 ? maxConcurrency : targetConcurrency) / regions.size();
        ListMultimap<Long,KeyRange> keyRangesPerRegion = ArrayListMultimap.create(regions.size(),regions.size() * splitsPerRegion);;
        if (regions.size() >= targetConcurrency) {
            for (HRegionInfo region : regions) {
                keyRangesPerRegion.put(region.getRegionId(),TO_KEY_RANGE.apply(region));
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
                        keyRangesPerRegion.put(region.getRegionId(),TO_KEY_RANGE.apply(region));
                        continue;
                    }
                }
                // TODO: else if scan startRow > region startRow use scan startRow as starting point, but expand out the key
                if (upperUnbound) {
                    stopKey = services.getStatsManager().getMaxKey(table);
                    if (stopKey == null) {
                        keyRangesPerRegion.put(region.getRegionId(),TO_KEY_RANGE.apply(region));
                        continue;
                    }
                }
                // TODO: else if scan endRow < region endRow use scan endRow as ending point, but expand out the key
                // Special case for fully qualified key - use only single region.
                // Maybe decrease parallelization as more of row key was specified?

                byte[][] boundaries = null;
                // Both startKey and stopKey will be empty the first time
                if (Bytes.compareTo(startKey, stopKey) >= 0 || (boundaries = Bytes.split(startKey, stopKey, splitsPerRegion - 1)) == null) {
                    // Bytes.split may return null if the key space
                    // between start and end key is too small
                    keyRangesPerRegion.put(region.getRegionId(),TO_KEY_RANGE.apply(region));
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
        Configuration config = services.getConfig();
        try {
            int numSplits = splits.size();
            List<PeekingResultIterator> iterators = new ArrayList<PeekingResultIterator>(numSplits);
            List<Pair<byte[],Future<PeekingResultIterator>>> futures = new ArrayList<Pair<byte[],Future<PeekingResultIterator>>>(numSplits);
            try {
                ExecutorService executor = services.getExecutor();
                final MemoryManager mm = services.getMemoryManager();
                final int spoolThresholdBytes = config.getInt(QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, DEFAULT_SPOOL_THRESHOLD_BYTES);
                for (KeyRange split : splits) {
                    final Scan splitScan = new Scan(this.context.getScan());
                    // Intersect with existing start/stop key
                    if (ScanUtil.intersectScanRange(splitScan, split.getLowerRange(), split.getUpperRange())) {
                        Future<PeekingResultIterator> future =
                            executor.submit(new JobCallable<PeekingResultIterator>() {
    
                            @Override
                            public PeekingResultIterator call() throws Exception {
                                // TODO: different HTableInterfaces for each thread or the same is better?
                                ResultIterator scanner = new TableResultIterator(context, table, splitScan);
                                return new SpoolingResultIterator(scanner, mm, spoolThresholdBytes, rowCounter);
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

                int timeoutMs = config.getInt(QueryServices.THREAD_TIMEOUT_MS_ATTRIB, DEFAULT_THREAD_TIMEOUT_MS);
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
            throw new SQLException(e);
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
