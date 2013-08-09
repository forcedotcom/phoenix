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
import java.util.List;

import org.apache.hadoop.hbase.util.Pair;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.AliasedNode;
import com.salesforce.phoenix.parse.SelectStatement;
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
        private List<Expression> filters;
        private List<Expression> postFilters;
        private List<JoinTable> joinTables;
        private boolean isPostAggregate;
        private ColumnResolver resolver;
        
        private JoinSpec(TableRef table, List<AliasedNode> select, List<Expression> filters, 
                List<Expression> postFilters, List<JoinTable> joinTables, boolean isPostAggregate,
                ColumnResolver resolver) {
            this.mainTable = table;
            this.select = select;
            this.filters = filters;
            this.postFilters = postFilters;
            this.joinTables = joinTables;
            this.isPostAggregate = isPostAggregate;
            this.resolver = resolver;
        }
        
        public TableRef getMainTable() {
            return mainTable;
        }
        
        public List<AliasedNode> getSelect() {
            return select;
        }
        
        public List<Expression> getFilters() {
            return filters;
        }
        
        public List<Expression> getPostFilters() {
            return postFilters;
        }
        
        public List<JoinTable> getJoinTables() {
            return joinTables;
        }
        
        public boolean isPostAggregate() {
            return isPostAggregate;
        }
        
        public ColumnResolver getColumnResolver() {
            return resolver;
        }
    }
    
    public static JoinSpec getSubJoinSpec(JoinSpec join) {
        return new JoinSpec(join.mainTable, join.select, join.filters, join.postFilters, join.joinTables.subList(0, join.joinTables.size() - 2), join.isPostAggregate, join.resolver);
    }
    
    public static class JoinTable {
        private JoinType type;
        private List<Expression> conditions;
        private List<AliasedNode> select;
        private List<Expression> filters;
        private List<Expression> postJoinFilters; // will be pushed to postFilters in case of star join
        private TableRef table;
        private SelectStatement subquery;
        
        private JoinTable(JoinType type, List<Expression> conditions, List<AliasedNode> select,
                List<Expression> filters, List<Expression> postJoinFilters, TableRef table, SelectStatement subquery) {
            this.type = type;
            this.conditions = conditions;
            this.select = select;
            this.filters = filters;
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
        
        public List<AliasedNode> getSelect() {
            return select;
        }
        
        public List<Expression> getFilters() {
            return filters;
        }
        
        public List<Expression> getPostJoinFilters() {
            return postJoinFilters;
        }
        
        public TableRef getTable() {
            return table;
        }
        
        public SelectStatement getSubquery() {
            return subquery;
        }
        
        public SelectStatement getAsSubquery() {
            // TODO
            return subquery;
        }
    }
    
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
    
    public static SelectStatement newSelectWithoutJoin(SelectStatement statement) {
        // TODO
        return null;
    }
    
    // Get the last join table select statement with fixed-up select and where nodes.
    // Currently does NOT support last join table as a subquery.
    public static SelectStatement newSelectForLastJoin(SelectStatement statement, JoinSpec join) {
        // TODO
        return null;
    }
    
    // Get subquery with fixed select and where nodes
    public static SelectStatement getSubQuery(SelectStatement statement) {
        // TODO
        return null;
    }
    
    // Get subquery with complete select and where nodes
    // Throws exception if the subquery contains joins.
    public static SelectStatement getSubQueryForFinalPlan(SelectStatement statement) {
        // TODO
        return null;
    }
}
