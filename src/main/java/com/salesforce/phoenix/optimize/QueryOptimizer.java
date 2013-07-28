package com.salesforce.phoenix.optimize;

import java.sql.SQLException;
import java.util.*;

import com.google.common.collect.Lists;
import com.salesforce.phoenix.compile.*;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.jdbc.PhoenixStatement;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.parse.HintNode.Hint;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.query.QueryServicesOptions;
import com.salesforce.phoenix.schema.*;

public class QueryOptimizer {
    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();

    private final QueryServices services;
    private final boolean useIndexes;

    public QueryOptimizer(QueryServices services) {
        this.services = services;
        this.useIndexes = this.services.getProps().getBoolean(QueryServices.USE_INDEXES_ATTRIB, QueryServicesOptions.DEFAULT_USE_INDEXES);
    }

    public QueryPlan optimize(SelectStatement select, PhoenixStatement statement) throws SQLException {
        List<Object> binds = statement.getParameters();
        PhoenixConnection connection = statement.getConnection();
        QueryCompiler compiler = new QueryCompiler(connection, statement.getMaxRows());
        QueryPlan dataPlan = compiler.compile(select, binds);
        if (!useIndexes) {
            return dataPlan;
        }
        
        PTable dataTable = dataPlan.getTableRef().getTable();
        List<PTable>indexes = dataTable.getIndexes();
        /*
         * Only indexes on tables with immutable rows may be used until we hook up
         * incremental index maintenance. 
         */
        if (!dataTable.isImmutableRows() || indexes.isEmpty() || dataPlan.getTableRef().hasDynamicCols() || dataPlan.getContext().hasHint(Hint.NO_INDEX)) {
            return dataPlan;
        }
        
        List<QueryPlan> plans = Lists.newArrayListWithExpectedSize(1 + indexes.size());
        plans.add(dataPlan);
        ColumnResolver resolver = FromCompiler.getResolver(select, connection);
        SelectStatement translatedSelect = IndexStatementRewriter.translate(select, resolver);
        PTable hintedIndex = findHintedIndex(dataPlan);
        if (hintedIndex != null && addPlan(statement, translatedSelect, hintedIndex, plans)) {
            return plans.get(1);
        }
        for (PTable index : indexes) {
            if (hintedIndex != index) { // skip hinted index if we didn't chose it above
                addPlan(statement, translatedSelect, index, plans);
            }
        }
        
        return chooseBestPlan(select, plans);
    }
    
    private static PTable findHintedIndex(QueryPlan dataPlan) {
        String indexHint = dataPlan.getContext().getHint(Hint.INDEX);
        if (indexHint == null) {
            return null;
        }
        String alias = dataPlan.getTableRef().getTableAlias();
        String prefix = (alias == null ? dataPlan.getTableRef().getTable().getName().getString() : alias) + HintNode.SEPARATOR;
        for (PTable index : dataPlan.getTableRef().getTable().getIndexes()) {
            if (indexHint.contains(prefix + index.getName().getString() + HintNode.TERMINATOR)) {
                return index;
            }
        }
        return null;
    }
    
    private static boolean addPlan(PhoenixStatement statement, SelectStatement translatedSelect, PTable index, List<QueryPlan> plans) throws SQLException {
        List<Object> binds = statement.getParameters();
        PhoenixConnection connection = statement.getConnection();        
        QueryPlan dataPlan = plans.get(0);
        int nColumns = dataPlan.getProjector().getColumnCount();
        String alias = '"' + dataPlan.getTableRef().getTableAlias() + '"'; // double quote in case it's case sensitive
        PSchema schema = dataPlan.getTableRef().getSchema();
        String schemaName = schema.getName().length() == 0 ? null :  '"' + schema.getName() + '"';

        String tableName = '"' + index.getName().getString() + '"';
        List<? extends TableNode> tables = Collections.singletonList(FACTORY.namedTable(alias, FACTORY.table(schemaName, tableName)));
        try {
            SelectStatement indexSelect = FACTORY.select(translatedSelect, tables);
            QueryCompiler compiler = new QueryCompiler(connection, statement.getMaxRows());
            QueryPlan plan = compiler.compile(indexSelect, binds);
            // Checking the index status and number of columns handles the wildcard cases correctly
            // We can't check the status earlier, because the index table may be out-of-date.
            if (plan.getTableRef().getTable().getIndexState() == PIndexState.ACTIVE && plan.getProjector().getColumnCount() == nColumns) {
                plans.add(plan);
                return true;
            }
        } catch (ColumnNotFoundException e) {
            /* Means that a column is being used that's not in our index.
             * Since we currently don't keep stats, we don't know the selectivity of the index.
             * For now, we just don't use this index (as opposed to trying to join back from
             * the index table to the data table.
             */
        }
        return false;
    }
    
    /**
     * Choose the best plan among all the possible ones.
     * Since we don't keep stats yet, we use the following simple algorithm:
     * 1) If the query has an ORDER BY and a LIMIT, choose the plan that has all the ORDER BY expression
     * in the same order as the row key columns.
     * 2) If there are more than one plan that meets (1), choose the plan with:
     *    a) the most row key columns that may be used to form the start/stop scan key.
     *    b) the plan that preserves ordering for a group by.
     *    c) the data table plan
     * @param plans the list of candidate plans
     * @return
     */
    private QueryPlan chooseBestPlan(SelectStatement select, List<QueryPlan> plans) {
        QueryPlan firstPlan = plans.get(0);
        if (plans.size() == 1) {
            return firstPlan;
        }
        
        List<QueryPlan> candidates = Lists.newArrayListWithExpectedSize(plans.size());
        if (firstPlan.getLimit() == null) {
            candidates.addAll(plans);
        } else {
            for (QueryPlan plan : plans) {
                // If ORDER BY optimized out (or not present at all)
                if (plan.getOrderBy().getOrderByExpressions().isEmpty()) {
                    candidates.add(plan);
                }
            }
            if (candidates.isEmpty()) {
                candidates.addAll(plans);
            }
        }
        Collections.sort(candidates, new Comparator<QueryPlan>() {

            @Override
            public int compare(QueryPlan plan1, QueryPlan plan2) {
                int c = plan2.getContext().getScanRanges().getRanges().size() - plan1.getContext().getScanRanges().getRanges().size();
                if (c != 0) return c;
                if (plan1.getGroupBy()!=null && plan2.getGroupBy()!=null) {
                    if (plan1.getGroupBy().isOrderPreserving() != plan2.getGroupBy().isOrderPreserving()) {
                        return plan1.getGroupBy().isOrderPreserving() ? -1 : 1;
                    }
                }
                // All things being equal, just use the data table
                if (plan1.getTableRef().getTable().getType() != PTableType.INDEX) {
                    return -1;
                }
                if (plan2.getTableRef().getTable().getType() != PTableType.INDEX) {
                    return 1;
                }
                
                return 0;
            }
            
        });
        
        return candidates.get(0);
        
    }

    
}
