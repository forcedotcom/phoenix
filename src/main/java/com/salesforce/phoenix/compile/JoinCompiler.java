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
        private TableRef table;
        private List<AliasedNode> select;
        private List<Expression> filters;
        private List<Expression> postFilters;
        private List<JoinTable> joinTables;
        private boolean isPostAggregate;
        
        private JoinSpec(TableRef table, List<AliasedNode> select, List<Expression> filters, 
                List<Expression> postFilters, List<JoinTable> joinTables, boolean isPostAggregate) {
            this.table = table;
            this.select = select;
            this.filters = filters;
            this.postFilters = postFilters;
            this.joinTables = joinTables;
            this.isPostAggregate = isPostAggregate;
        }
                
        public List<JoinTable> getJoinTables() {
            return joinTables;
        }
    }
    
    public static JoinSpec getSubJoinSpec(JoinSpec join) {
        return new JoinSpec(join.table, join.select, join.filters, join.postFilters, join.joinTables.subList(0, join.joinTables.size() - 2), join.isPostAggregate);
    }
    
    public static class JoinTable {
        private JoinType type;
        private List<Expression> conditions;
        private List<AliasedNode> select;
        private List<Expression> filters;
        private List<Expression> postJoinFilters; // will be pushed to postFilters in case of star join
        private TableRef table;
        private SelectStatement subquery;
        
        public JoinType getType() {
            return type;
        }
    }
    
    public interface JoinedColumnResolver extends ColumnResolver {
        public JoinSpec getJoinTables();
    }
    
    public static JoinedColumnResolver getResolver(SelectStatement statement, PhoenixConnection connection) throws SQLException {
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
}
