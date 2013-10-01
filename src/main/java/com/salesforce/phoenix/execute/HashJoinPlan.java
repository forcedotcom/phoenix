package com.salesforce.phoenix.execute;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;

import com.salesforce.phoenix.cache.ServerCacheClient.ServerCache;
import com.salesforce.phoenix.compile.ExplainPlan;
import com.salesforce.phoenix.compile.QueryPlan;
import com.salesforce.phoenix.compile.RowProjector;
import com.salesforce.phoenix.compile.StatementContext;
import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.join.HashCacheClient;
import com.salesforce.phoenix.join.HashJoinInfo;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.query.Scanner;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.ImmutableBytesPtr;

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
        
        HashCacheClient hashClient = plan.getContext().getHashClient();
        Scan scan = plan.getContext().getScan();
        KeyRange keyRange = KeyRange.getKeyRange(scan.getStartRow(), scan.getStopRow());
        // TODO replace with Future execution
        for (int i = 0; i < joinIds.length; i++) {
            ServerCache cache = hashClient.addHashCache(hashPlans[i].getScanner(), hashExpressions[i], plan.getTableRef(), keyRange);
            joinIds[i].set(cache.getId());
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
        return plan.getExplainPlan();
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

}
