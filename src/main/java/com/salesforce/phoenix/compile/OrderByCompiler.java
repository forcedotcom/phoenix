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

import static com.google.common.base.Preconditions.checkNotNull;

import java.sql.SQLException;
import java.util.*;


import com.google.common.collect.*;
import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.parse.OrderByNode;
import com.salesforce.phoenix.parse.SelectStatement;

/**
 * Validates ORDER BY clause and builds up a list of referenced columns.
 * 
 * @author syyang
 * @since 0.1
 */
public class OrderByCompiler {
    public static class OrderBy {
        public static final OrderBy EMPTY_ORDER_BY = new OrderBy(false, Collections.<OrderingColumn>emptyList());
        
        private final boolean isAggregate;
        private final List<OrderingColumn> orderingColumns;
        
        private OrderBy(boolean isAggregate, List<OrderingColumn> orderingColumns) {
            this.isAggregate = isAggregate;
            this.orderingColumns = ImmutableList.copyOf(orderingColumns);
        }

        public boolean isAggregate() {
            return isAggregate;
        }

        public List<OrderingColumn> getOrderingColumns() {
            return orderingColumns;
        }
    }
    /**
     * Gets a list of columns in the ORDER BY clause
     * 
     * @param statement the select statement
     * @param context the query context for tracking various states
     * associated with the given select statement
     * @param limit 
     * @param groupBy the list of columns in the GROUP BY clause
     * @return the list of columns in the ORDER BY clause
     * @throws SQLException
     */
    public static OrderBy getOrderBy(SelectStatement statement,
                                    StatementContext context,
                                    GroupBy groupBy, Integer limit) throws SQLException {
        if (statement.getOrderBy().isEmpty()) {
            return OrderBy.EMPTY_ORDER_BY;
        }
        // accumulate columns in ORDER BY
        OrderingColumns visitor = new OrderingColumns(context, groupBy);
        Expression nonAggregateExpression = null;
        for (OrderByNode node : statement.getOrderBy()) {
            Expression expression = node.getOrderByParseNode().accept(visitor);
            // Detect mix of aggregate and non aggregates (i.e. ORDER BY txns, SUM(txns)
            if (! (expression instanceof LiteralExpression) ) { // Filter out top level literals
                if (!visitor.isAggregate()) {
                    nonAggregateExpression = expression;
                }
                if (nonAggregateExpression != null) {
                    if (context.isAggregate()) {
                        ExpressionCompiler.throwNonAggExpressionInAggException(nonAggregateExpression.toString());
                    } else if (limit == null) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNSUPPORTED_ORDER_BY_QUERY).build().buildException();
                    }
                }
                OrderingColumn col = new OrderingColumn(node, expression);
                visitor.addOrderingColumn(col);
            }
            visitor.reset();
        }

        return new OrderBy(context.isAggregate(), visitor.getOrderingColumns());
    }


    /**
     * A container for a column that appears in ORDER BY clause.
     */
    public static class OrderingColumn {
        private final Expression expression;
        private final boolean nullsLast;
        private final boolean ascending;
        
        private OrderingColumn(OrderByNode node, Expression expression) {
            checkNotNull(node);
            checkNotNull(expression);
            this.expression = expression;
            this.nullsLast = node.getNullsLast();
            this.ascending = node.getOrderAscending();
        }

        public Expression getExpression() {
            return expression;
        }
        
        public boolean isNullsLast() {
            return nullsLast;
        }
        
        public boolean isAscending() {
            return ascending;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o != null && o.getClass() == OrderingColumn.class) {
                OrderingColumn that = (OrderingColumn)o;
                return nullsLast == that.nullsLast
                    && ascending == that.ascending
                    && expression.equals(that.expression);
            }
            return false;
        }
        
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (nullsLast ? 0 : 1);
            result = prime * result + (ascending ? 0 : 1);
            result = prime * result + expression.hashCode();
            return result;
        }
        
        @Override
        public String toString() {
            return this.getExpression() + (ascending ? " asc" : " desc") + " nulls " + (nullsLast ? "last" : "first");
        }
    }
    
    private OrderByCompiler() {
    }
    
    private static class OrderingColumns extends ExpressionCompiler {
        private final Set<OrderingColumn> visited = Sets.newHashSet();
        private final List<OrderingColumn> orderingColumns = Lists.newArrayList();
        
        private OrderingColumns(StatementContext context, GroupBy groupBy) {
            super(context, groupBy);
        }
        
        private List<OrderingColumn> getOrderingColumns() {
            return orderingColumns;
        }
        
        private void addOrderingColumn(OrderingColumn orderingColumn) {
            if (!visited.contains(orderingColumn)) {
                orderingColumns.add(orderingColumn);
                visited.add(orderingColumn);
            }
        }
    }
}
