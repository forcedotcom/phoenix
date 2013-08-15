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

import org.apache.hadoop.hbase.util.Pair;

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
        private List<AliasedNode> select;
        private ParseNode preFilters;
        private List<Expression> postFilters;
        private List<JoinTable> joinTables;
        private boolean isPostAggregateOrDistinct;
        private ColumnResolver resolver;
        
        private JoinSpec(TableRef table, List<AliasedNode> select, ParseNode preFilters, 
                List<Expression> postFilters, List<JoinTable> joinTables, boolean isPostAggregate,
                ColumnResolver resolver) {
            this.mainTable = table;
            this.select = select;
            this.preFilters = preFilters;
            this.postFilters = postFilters;
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
        
        public ParseNode getPreFilters() {
            return preFilters;
        }
        
        public List<Expression> getPostFilters() {
            return postFilters;
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
    }
    
    public static JoinSpec getSubJoinSpec(JoinSpec join) {
        return new JoinSpec(join.mainTable, join.select, join.preFilters, join.postFilters, join.joinTables.subList(0, join.joinTables.size() - 2), join.isPostAggregateOrDistinct, join.resolver);
    }
    
    public static class JoinTable {
        private JoinType type;
        private List<Expression> conditions;
        private TableNode tableNode; // original table node
        private TableRef table;
        private List<AliasedNode> select;
        private ParseNode preFilters;
        private List<Expression> postJoinFilters; // will be pushed to postFilters in case of star join
        private SelectStatement subquery;
        
        private JoinTable(JoinType type, List<Expression> conditions, TableNode tableNode, List<AliasedNode> select,
                ParseNode preFilters, List<Expression> postJoinFilters, TableRef table, SelectStatement subquery) {
            this.type = type;
            this.conditions = conditions;
            this.tableNode = tableNode;
            this.select = select;
            this.preFilters = preFilters;
            this.postJoinFilters = postJoinFilters;
            this.table = table;
            this.subquery = subquery;
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
        
        public ParseNode getPreFilters() {
            return preFilters;
        }
        
        public List<Expression> getPostJoinFilters() {
            return postJoinFilters;
        }
        
        public SelectStatement getSubquery() {
            return subquery;
        }
        
        public SelectStatement getAsSubquery() {
            // TODO
            return subquery;
        }
    }
    
    // for creation of new statements
    private static ParseNodeFactory factory = new ParseNodeFactory();
    
    public static JoinSpec getJoinSpec(SelectStatement statement, PhoenixConnection connection) throws SQLException {
        // TODO
        return null;
    }
    
    public static StarJoinType getStarJoinType(JoinSpec join) {
        // TODO
        return StarJoinType.NONE;
    }
    
    // Left: other table expressions; Right: this table expressions.
    public static Pair<List<Expression>, List<Expression>> splitEquiJoinConditions(JoinTable joinTable) {
        // TODO
        return null;
    }
    
    public static SelectStatement getSubqueryWithoutJoin(SelectStatement statement, JoinSpec join) {
        return factory.select(statement.getFrom().subList(0, 1), statement.getHint(), statement.isDistinct(), statement.getSelect(), join.getPreFilters(), statement.getGroupBy(), statement.getHaving(), statement.getOrderBy(), statement.getLimit(), statement.getBindCount());
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
        
        // TODO distinguish last join in original query or last join in subquery
        return factory.select(from, statement.getHint(), statement.isDistinct(), lastJoinTable.getSelect(), lastJoinTable.getPreFilters(), statement.getGroupBy(), statement.getHaving(), statement.getOrderBy(), statement.getLimit(), statement.getBindCount());
    }
    
    // Get subquery with fixed select and where nodes
    public static SelectStatement getSubQueryWithoutLastJoin(SelectStatement statement) {
        // TODO
        return null;
    }
    
    // Get subquery with complete select and where nodes
    // Throws exception if the subquery contains joins.
    public static SelectStatement getSubQueryWithoutLastJoinAsFinalPlan(SelectStatement statement) {
        // TODO
        return null;
    }
    
    public static Expression getPostJoinFilterExpression(JoinSpec join, JoinTable joinTable) {
        // TODO
        return null;
    }
}
