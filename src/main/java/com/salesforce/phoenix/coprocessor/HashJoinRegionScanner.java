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
package com.salesforce.phoenix.coprocessor;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import com.salesforce.phoenix.cache.GlobalCache;
import com.salesforce.phoenix.cache.TenantCache;
import com.salesforce.phoenix.join.HashJoinInfo;
import com.salesforce.phoenix.parse.JoinTableNode.JoinType;
import com.salesforce.phoenix.schema.tuple.ResultTuple;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.TupleUtil;

public class HashJoinRegionScanner implements RegionScanner {
    
    private final RegionScanner scanner;
    private final ScanProjector projector;
    private final HashJoinInfo joinInfo;
    private Queue<List<KeyValue>> resultQueue;
    private boolean hasMore;
    private TenantCache cache;
    
    public HashJoinRegionScanner(RegionScanner scanner, ScanProjector projector, HashJoinInfo joinInfo, ImmutableBytesWritable tenantId, Configuration conf) throws IOException {
        this.scanner = scanner;
        this.projector = projector;
        this.joinInfo = joinInfo;
        this.resultQueue = new LinkedList<List<KeyValue>>();
        this.hasMore = true;
        if (joinInfo != null) {
            if (tenantId == null)
                throw new IOException("Could not find tenant id for hash cache.");
            for (JoinType type : joinInfo.getJoinTypes()) {
                if (type == JoinType.Right)
                    throw new IOException("The hashed table should not be LHS.");
            }
            this.cache = GlobalCache.getTenantCache(conf, tenantId);
        }
    }
    
    @SuppressWarnings("unchecked")
    private void processResults(List<KeyValue> result, boolean hasLimit) throws IOException {
        if (result.isEmpty())
            return;
        
        if (projector != null) {
            List<KeyValue> kvs = new ArrayList<KeyValue>(result.size());
            for (KeyValue kv : result) {
                kvs.add(projector.getProjectedKeyValue(kv));
            }
            if (joinInfo != null) {
                result = kvs;
            } else {
                resultQueue.offer(kvs);               
            }
        }
        
        if (joinInfo != null) {
            if (hasLimit)
                throw new UnsupportedOperationException("Cannot support join operations in scans with limit");
            
            int count = joinInfo.getJoinIds().length;
            List<Tuple>[] tuples = new List[count];
            Tuple tuple = new ResultTuple(new Result(result));
            boolean cont = true;
            for (int i = 0; i < count; i++) {
                ImmutableBytesWritable key = TupleUtil.getConcatenatedValue(tuple, joinInfo.getJoinExpressions()[i]);
                tuples[i] = cache.getHashCache(joinInfo.getJoinIds()[i]).get(key);
                JoinType type = joinInfo.getJoinTypes()[i];
                if (type == JoinType.Inner && (tuples[i] == null || tuples[i].isEmpty())) {
                    cont = false;
                    break;
                }
            }
            if (cont) {
                resultQueue.offer(result);
                for (int i = 0; i < count; i++) {
                    if (tuples[i] == null || tuples[i].isEmpty())
                        continue;
                    int j = resultQueue.size();
                    while (j-- > 0) {
                        List<KeyValue> lhs = resultQueue.poll();
                        for (Tuple t : tuples[i]) {
                            List<KeyValue> rhs = ((ResultTuple) t).getResult().list();
                            List<KeyValue> joined = new ArrayList<KeyValue>(lhs.size() + rhs.size());
                            joined.addAll(lhs);
                            joined.addAll(rhs); // we don't replace rowkey here, for further reference to the rowkey fields, needs to specify family as well.
                            resultQueue.offer(joined);
                        }
                    }
                }
            }
        }
    }
    
    private boolean shouldAdvance() {
        if (!resultQueue.isEmpty())
            return false;
        
        return hasMore;
    }
    
    private boolean nextInQueue(List<KeyValue> results) {
        if (resultQueue.isEmpty())
            return false;
        
        results.addAll(resultQueue.poll());
        return resultQueue.isEmpty() ? hasMore : true;
    }

    @Override
    public long getMvccReadPoint() {
        return scanner.getMvccReadPoint();
    }

    @Override
    public HRegionInfo getRegionInfo() {
        return scanner.getRegionInfo();
    }

    @Override
    public boolean isFilterDone() {
        return scanner.isFilterDone() && resultQueue.isEmpty();
    }

    @Override
    public boolean nextRaw(List<KeyValue> result, String metric) throws IOException {
        while (shouldAdvance()) {
            List<KeyValue> tempResult = new ArrayList<KeyValue>();
            hasMore = scanner.nextRaw(tempResult, metric);
            processResults(tempResult, false);
        }
        
        return nextInQueue(result);
    }

    @Override
    public boolean nextRaw(List<KeyValue> result, int limit, String metric)
            throws IOException {
        while (shouldAdvance()) {
            List<KeyValue> tempResult = new ArrayList<KeyValue>();
            hasMore = scanner.nextRaw(tempResult, limit, metric);
            processResults(tempResult, true);
        }
        
        return nextInQueue(result);
    }

    @Override
    public boolean reseek(byte[] row) throws IOException {
        return scanner.reseek(row);
    }

    @Override
    public void close() throws IOException {
        scanner.close();
    }

    @Override
    public boolean next(List<KeyValue> result) throws IOException {
        while (shouldAdvance()) {
            List<KeyValue> tempResult = new ArrayList<KeyValue>();
            hasMore = scanner.next(tempResult);
            processResults(tempResult, false);
        }
        
        return nextInQueue(result);
    }

    @Override
    public boolean next(List<KeyValue> result, String metric) throws IOException {
        while (shouldAdvance()) {
            List<KeyValue> tempResult = new ArrayList<KeyValue>();
            hasMore = scanner.next(tempResult, metric);
            processResults(tempResult, false);
        }
        
        return nextInQueue(result);
    }

    @Override
    public boolean next(List<KeyValue> result, int limit) throws IOException {
        while (shouldAdvance()) {
            List<KeyValue> tempResult = new ArrayList<KeyValue>();
            hasMore = scanner.next(tempResult, limit);
            processResults(tempResult, true);
        }
        
        return nextInQueue(result);
    }

    @Override
    public boolean next(List<KeyValue> result, int limit, String metric)
            throws IOException {
        while (shouldAdvance()) {
            List<KeyValue> tempResult = new ArrayList<KeyValue>();
            hasMore = scanner.next(tempResult, limit, metric);
            processResults(tempResult, true);
        }
        
        return nextInQueue(result);
    }

}
