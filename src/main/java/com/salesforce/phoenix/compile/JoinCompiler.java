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
package com.salesforce.phoenix.compile;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.salesforce.phoenix.expression.AndExpression;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.AliasedNode;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.parse.ParseNodeFactory;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.parse.TableNode;
import com.salesforce.phoenix.parse.JoinTableNode.JoinType;
import com.salesforce.phoenix.schema.TableRef;


public class JoinCompiler {
    
    public enum StarJoinType {
        BASIC,
        EXTENDED,
        NONE,
    }
    
    public static class JoinSpec {
        private TableRef mainTable;
        private List<AliasedNode> select; // all basic nodes related to mainTable, no aggregation.
        private List<ParseNode> preFilters;
        private List<ParseNode> postFilters;
        private List<Expression> postFilterExpressions;
        private List<JoinTable> joinTables;
        private boolean isPostAggregateOrDistinct;
        private ColumnResolver resolver;
        
        private JoinSpec(TableRef table, List<AliasedNode> select, List<ParseNode> preFilters, 
                List<ParseNode> postFilters, List<Expression> postFilterExpressions, 
                List<JoinTable> joinTables, boolean isPostAggregate, ColumnResolver resolver) {
            this.mainTable = table;
            this.select = select;
            this.preFilters = preFilters;
            this.postFilters = postFilters;
            this.postFilterExpressions = postFilterExpressions;
            this.joinTables = joinTables;
            this.isPostAggregateOrDistinct = isPostAggregate;
            this.resolver = resolver;
        }
        
        public TableRef getMainTable() {
            return mainTable;
        }
        
        public List<AliasedNode> getSelect() {
            return select;
        }
        
        public List<ParseNode> getPreFilters() {
            return preFilters;
        }
        
        public List<ParseNode> getPostFilters() {
            return postFilters;
        }
        
        public List<Expression> getPostFilterExpressions() {
            return postFilterExpressions;
        }
        
        public List<JoinTable> getJoinTables() {
            return joinTables;
        }
        
        public boolean isPostAggregateOrDistinct() {
            return isPostAggregateOrDistinct;
        }
        
        public ColumnResolver getColumnResolver() {
            return resolver;
        }
        
        public ParseNode getPreFiltersCombined() {
            if (preFilters == null || preFilters.isEmpty())
                return null;
            
            if (preFilters.size() == 1)
                return preFilters.get(0);
            
            return NODE_FACTORY.and(preFilters);
        }
    }
    
    public static JoinSpec getSubJoinSpec(JoinSpec join) {
        return new JoinSpec(join.mainTable, join.select, join.preFilters, join.postFilters, join.postFilterExpressions, join.joinTables.subList(0, join.joinTables.size() - 2), join.isPostAggregateOrDistinct, join.resolver);
    }
    
    public static class JoinTable {
        private JoinType type;
        private List<Expression> conditions;
        private TableNode tableNode; // original table node
        private TableRef table;
        private List<AliasedNode> select; // all basic nodes related to this table, no aggregation.
        private List<ParseNode> preFilters;
        private List<ParseNode> postFilters;
        private List<Expression> postFilterExpressions; // will be pushed to postFilters in case of star join
        private SelectStatement subquery;
        // equi-joins only
        private List<Expression> leftTableConditions;
        private List<Expression> rightTableConditions;
        
        private JoinTable(JoinType type, List<Expression> conditions, TableNode tableNode, List<AliasedNode> select,
                List<ParseNode> preFilters, List<ParseNode> postFilters, List<Expression> postFilterExpressions, 
                TableRef table, SelectStatement subquery) {
            this.type = type;
            this.conditions = conditions;
            this.tableNode = tableNode;
            this.select = select;
            this.preFilters = preFilters;
            this.postFilters = postFilters;
            this.postFilterExpressions = postFilterExpressions;
            this.table = table;
            this.subquery = subquery;
            // TODO split left and right table conditions;
        }
        
        public JoinType getType() {
            return type;
        }
        
        public List<Expression> getJoinConditions() {
            return conditions;
        }
        
        public TableNode getTableNode() {
            return tableNode;
        }
        
        public TableRef getTable() {
            return table;
        }
        
        public List<AliasedNode> getSelect() {
            return select;
        }
        
        public List<ParseNode> getPreFilters() {
            return preFilters;
        }
        
        public List<ParseNode> getPostFilters() {
            return postFilters;
        }
        
        public List<Expression> getPostFilterExpressions() {
            return postFilterExpressions;
        }
        
        public SelectStatement getSubquery() {
            return subquery;
        }
        
        public ParseNode getPreFiltersCombined() {
            if (preFilters == null || preFilters.isEmpty())
                return null;
            
            if (preFilters.size() == 1)
                return preFilters.get(0);
            
            return NODE_FACTORY.and(preFilters);
        }
        
        public SelectStatement getAsSubquery() {
            if (subquery != null)
                return subquery;
            
            List<TableNode> from = new ArrayList<TableNode>(1);
            from.add(tableNode);
            return NODE_FACTORY.select(from, null, false, select, getPreFiltersCombined(), null, null, null, null, 0, false);
        }
        
        public boolean isEquiJoin() {
            return (leftTableConditions != null && !leftTableConditions.isEmpty());
        }
        
        public List<Expression> getLeftTableConditions() {
            return leftTableConditions;
        }
        
        public List<Expression> getRightTableConditions() {
            return rightTableConditions;
        }
    }
    
    // for creation of new statements
    private static ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();
    
    public static JoinSpec getJoinSpec(SelectStatement statement, PhoenixConnection connection) throws SQLException {
        // TODO
        return null;
    }
    
    public static StarJoinType getStarJoinType(JoinSpec join) {
        assert(!join.getJoinTables().isEmpty());
        
        StarJoinType starJoinType = StarJoinType.BASIC;
        for (JoinTable joinTable : join.getJoinTables()) {
            if (!joinTable.isEquiJoin() 
                    || (joinTable.getType() != JoinType.Left 
                            && joinTable.getType() != JoinType.Inner))
                return StarJoinType.NONE;
            if (starJoinType == StarJoinType.BASIC) {
                for (Expression expr : joinTable.getLeftTableConditions()) {
                    // TODO test if expr consists ref to tables other than mainTable
                    starJoinType = StarJoinType.EXTENDED;
                }
            }
        }
        
        return starJoinType;
    }
    
    public static SelectStatement getSubqueryWithoutJoin(SelectStatement statement, JoinSpec join) {
        return NODE_FACTORY.select(statement.getFrom().subList(0, 1), statement.getHint(), statement.isDistinct(), statement.getSelect(), join.getPreFiltersCombined(), statement.getGroupBy(), statement.getHaving(), statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), statement.isAggregate());
    }
    
    // Get the last join table select statement with fixed-up select and where nodes.
    // Currently does NOT support last join table as a subquery.
    public static SelectStatement getSubqueryForLastJoinTable(SelectStatement statement, JoinSpec join) {
        List<JoinTable> joinTables = join.getJoinTables();
        int count = joinTables.size();
        assert (count > 0);
        JoinTable lastJoinTable = joinTables.get(count - 1);
        if (lastJoinTable.getSubquery() != null) {
            throw new UnsupportedOperationException("Right join table cannot be a subquery.");
        }
        List<TableNode> from = new ArrayList<TableNode>(1);
        from.add(lastJoinTable.getTableNode());
        
        return NODE_FACTORY.select(from, statement.getHint(), statement.isDistinct(), statement.getSelect(), lastJoinTable.getPreFiltersCombined(), statement.getGroupBy(), statement.getHaving(), statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), statement.isAggregate());
    }
    
    // Get subquery with fixed select and where nodes
    public static SelectStatement getSubQueryWithoutLastJoin(SelectStatement statement, JoinSpec join) {
        List<TableNode> from = statement.getFrom();
        assert(from.size() > 1);
        List<JoinTable> joinTables = join.getJoinTables();
        int count = joinTables.size();
        assert (count > 0);
        List<AliasedNode> select = new ArrayList<AliasedNode>();
        List<ParseNode> filters = new ArrayList<ParseNode>();
        select.addAll(join.getSelect());
        filters.addAll(join.getPreFilters());
        for (int i = 0; i < count - 1; i++) {
            select.addAll(joinTables.get(i).getSelect());
            filters.addAll(joinTables.get(i).getPreFilters());
            filters.addAll(joinTables.get(i).getPostFilters());
        }
        ParseNode where = null;
        if (filters.size() == 1) {
            where = filters.get(0);
        } else if (filters.size() > 1) {
            where = NODE_FACTORY.and(filters);
        }
        
        return NODE_FACTORY.select(from.subList(0, from.size() - 1), statement.getHint(), false, select, where, null, null, null, null, statement.getBindCount(), false);
    }
    
    // Get subquery with complete select and where nodes
    // Throws exception if the subquery contains joins.
    public static SelectStatement getSubQueryWithoutLastJoinAsFinalPlan(SelectStatement statement, JoinSpec join) {
        List<TableNode> from = statement.getFrom();
        assert(from.size() > 1);
        if (from.size() > 2)
            throw new UnsupportedOperationException("Left table of a left join cannot contain joins.");
        
        return NODE_FACTORY.select(from.subList(0, from.size() - 1), statement.getHint(), statement.isDistinct(), statement.getSelect(), join.getPreFiltersCombined(), statement.getGroupBy(), statement.getHaving(), statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), statement.isAggregate());
    }
    
    public static Expression getPostJoinFilterExpression(JoinSpec join, JoinTable joinTable) {
        List<Expression> postFilters = new ArrayList<Expression>();
        if (joinTable != null) {
            postFilters.addAll(joinTable.getPostFilterExpressions());
        } else {
            for (JoinTable table : join.getJoinTables()) {
                postFilters.addAll(table.getPostFilterExpressions());
            }
        }
        postFilters.addAll(join.getPostFilterExpressions());
        Expression postJoinFilterExpression = null;
        if (postFilters.size() == 1) {
            postJoinFilterExpression = postFilters.get(0);
        } else if (postFilters.size() > 1) {
            postJoinFilterExpression = new AndExpression(postFilters);
        }
        
        return postJoinFilterExpression;
    }
}
