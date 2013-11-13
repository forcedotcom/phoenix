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
package com.salesforce.phoenix.execute;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.hadoop.hbase.client.Scan;

import com.google.common.collect.Lists;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.cache.ServerCacheClient.ServerCache;
import com.salesforce.phoenix.compile.ExplainPlan;
import com.salesforce.phoenix.compile.QueryPlan;
import com.salesforce.phoenix.compile.RowProjector;
import com.salesforce.phoenix.compile.ScanRanges;
import com.salesforce.phoenix.compile.StatementContext;
import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.job.JobManager.JobCallable;
import com.salesforce.phoenix.join.HashCacheClient;
import com.salesforce.phoenix.join.HashJoinInfo;
import com.salesforce.phoenix.parse.FilterableStatement;
import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.query.Scanner;
import com.salesforce.phoenix.schema.TableRef;

public class HashJoinPlan implements QueryPlan {
    
    private BasicQueryPlan plan;
    private HashJoinInfo joinInfo;
    private List<Expression>[] hashExpressions;
    private QueryPlan[] hashPlans;
    
    public HashJoinPlan(BasicQueryPlan plan, HashJoinInfo joinInfo,
            List<Expression>[] hashExpressions, QueryPlan[] hashPlans) {
        this.plan = plan;
        this.joinInfo = joinInfo;
        this.hashExpressions = hashExpressions;
        this.hashPlans = hashPlans;
    }

    @Override
    public Integer getLimit() {
        return plan.getLimit();
    }

    @Override
    public OrderBy getOrderBy() {
        return plan.getOrderBy();
    }

    @Override
    public RowProjector getProjector() {
        return plan.getProjector();
    }

    @Override
    public Scanner getScanner() throws SQLException {
        ImmutableBytesPtr[] joinIds = joinInfo.getJoinIds();
        assert (joinIds.length == hashExpressions.length && joinIds.length == hashPlans.length);
        
        final HashCacheClient hashClient = plan.getContext().getHashClient();
        Scan scan = plan.getContext().getScan();
        final ScanRanges ranges = plan.getContext().getScanRanges();
        
        int count = joinIds.length;
        final ConnectionQueryServices services = getContext().getConnection().getQueryServices();
        ExecutorService executor = services.getExecutor();
        List<Future<ServerCache>> futures = new ArrayList<Future<ServerCache>>(count);
        for (int i = 0; i < count; i++) {
        	final int index = i;
        	futures.add(executor.submit(new JobCallable<ServerCache>() {
        		
				@Override
				public ServerCache call() throws Exception {
					return hashClient.addHashCache(ranges, hashPlans[index].getScanner(), 
							hashExpressions[index], plan.getTableRef());
				}

				@Override
				public Object getJobId() {
					return HashJoinPlan.this;
				}
        	}));
        }
        for (int i = 0; i < count; i++) {
			try {
				ServerCache cache = futures.get(i).get();
				joinIds[i].set(cache.getId());
			} catch (InterruptedException e) {
				throw new SQLException("Hash join execution interrupted.", e);
			} catch (ExecutionException e) {
				throw new SQLException("Encountered exception in hash plan execution.", 
						e.getCause());
			}
        }
        HashJoinInfo.serializeHashJoinIntoScan(scan, joinInfo);
        
        return plan.getScanner();
    }

    @Override
    public List<KeyRange> getSplits() {
        return plan.getSplits();
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        List<String> mainQuerySteps = plan.getExplainPlan().getPlanSteps();
        List<String> planSteps = Lists.newArrayList(mainQuerySteps);
        int count = hashPlans.length;
        planSteps.add("    PARALLEL EQUI-JOIN " + count + " HASH TABLES:");
        for (int i = 0; i < count; i++) {
        	planSteps.add("    BUILD HASH TABLE " + i);
        	List<String> steps = hashPlans[i].getExplainPlan().getPlanSteps();
        	for (String step : steps) {
        		planSteps.add("        " + step);
        	}
        }
        if (joinInfo.getPostJoinFilterExpression() != null) {
        	planSteps.add("    SERVER FILTER BY " + joinInfo.getPostJoinFilterExpression().toString());
        }
        
        return new ExplainPlan(planSteps);
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        return plan.getParameterMetaData();
    }

    @Override
    public StatementContext getContext() {
        return plan.getContext();
    }

    @Override
    public GroupBy getGroupBy() {
        return plan.getGroupBy();
    }

    @Override
    public TableRef getTableRef() {
        return plan.getTableRef();
    }

    @Override
    public FilterableStatement getStatement() {
        return plan.getStatement();
    }

}
