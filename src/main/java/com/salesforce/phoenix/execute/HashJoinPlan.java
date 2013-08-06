package com.salesforce.phoenix.execute;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.compile.ExplainPlan;
import com.salesforce.phoenix.compile.QueryPlan;
import com.salesforce.phoenix.compile.RowProjector;
import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.join.HashCacheClient;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.query.Scanner;
import com.salesforce.phoenix.schema.TableRef;

public class HashJoinPlan implements QueryPlan {
    
    private BasicQueryPlan plan;
    private ImmutableBytesWritable[] joinIds;
    private List<Expression>[] hashExpressions;
    private QueryPlan[] hashPlans;
    
    public HashJoinPlan(BasicQueryPlan plan, ImmutableBytesWritable[] joinIds,
            List<Expression>[] hashExpressions, QueryPlan[] hashPlans) {
        this.plan = plan;
        this.joinIds = joinIds;
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
        assert (joinIds.length == hashExpressions.length && joinIds.length == hashPlans.length);
        
        HashCacheClient hashClient = plan.getContext().getHashClient();
        // TODO replace with Future execution
        for (int i = 0; i < joinIds.length; i++) {
            hashClient.addHashCache(joinIds[i].get(), hashPlans[i].getScanner(), hashExpressions[i], plan.getTable().getTableName());
        }
        return plan.getScanner();
    }

    @Override
    public List<KeyRange> getSplits() {
        return plan.getSplits();
    }

    @Override
    public TableRef getTable() {
        return plan.getTable();
    }

    @Override
    public boolean isAggregate() {
        return plan.isAggregate();
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        return plan.getExplainPlan();
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        return plan.getParameterMetaData();
    }

}
