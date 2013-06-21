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
import java.util.*;

import com.google.common.collect.*;
import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.expression.*;
import com.salesforce.phoenix.parse.OrderByNode;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.schema.ColumnModifier;

/**
 * Validates ORDER BY clause and builds up a list of referenced columns.
 * 
 * @author syyang
 * @since 0.1
 */
public class OrderByCompiler {
    public static class OrderBy {
        public static final OrderBy EMPTY_ORDER_BY = new OrderBy(false, Collections.<OrderByExpression>emptyList());
        
        private final boolean isAggregate;
        private final List<OrderByExpression> orderByExpressions;
        
        private OrderBy(boolean isAggregate, List<OrderByExpression> orderByExpressions) {
            this.isAggregate = isAggregate;
            this.orderByExpressions = ImmutableList.copyOf(orderByExpressions);
        }

        public boolean isAggregate() {
            return isAggregate;
        }

        public List<OrderByExpression> getOrderByExpressions() {
            return orderByExpressions;
        }
    }
    /**
     * Gets a list of columns in the ORDER BY clause
     * @param context the query context for tracking various states
     * associated with the given select statement
     * @param orderByNodes the list of ORDER BY expressions
     * @param groupBy the list of columns in the GROUP BY clause
     * @param isDistinct true if SELECT DISTINCT and false otherwise
     * @param limit the row limit or null if no limit
     * @param aliasParseNodeMap the map of aliased parse nodes used
     * to resolve alias usage in the ORDER BY clause
     * 
     * @return the compiled ORDER BY clause
     * @throws SQLException
     */
    public static OrderBy getOrderBy(StatementContext context,
                                     List<OrderByNode> orderByNodes,
                                     GroupBy groupBy, boolean isDistinct,
                                     Integer limit, Map<String, ParseNode> aliasParseNodeMap) throws SQLException {
        if (orderByNodes.isEmpty()) {
            return OrderBy.EMPTY_ORDER_BY;
        }
        // accumulate columns in ORDER BY
        OrderByVisitor visitor = new OrderByVisitor(context, groupBy, aliasParseNodeMap);
        Expression nonAggregateExpression = null;
        for (OrderByNode node : orderByNodes) {
            Expression expression = node.getNode().accept(visitor);
            // Detect mix of aggregate and non aggregates (i.e. ORDER BY txns, SUM(txns)
            if (! (expression instanceof LiteralExpression) ) { // Filter out top level literals
                if (!visitor.isAggregate()) {
                    nonAggregateExpression = expression;
                }
                if (nonAggregateExpression != null) {
                    if (context.isAggregate()) {
                        if (isDistinct) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.ORDER_BY_NOT_IN_SELECT_DISTINCT)
                            .setMessage(nonAggregateExpression.toString()).build().buildException();
                        }
                        ExpressionCompiler.throwNonAggExpressionInAggException(nonAggregateExpression.toString());
                    }
                }
                boolean isAscending = node.isAscending();
                if (expression.getColumnModifier() == ColumnModifier.SORT_DESC) {
                    isAscending = !isAscending;
                }
                OrderByExpression col = new OrderByExpression(expression, node.isNullsLast(), isAscending);
                visitor.addOrderByExpression(col);
            }
            visitor.reset();
        }

        return new OrderBy(context.isAggregate(), visitor.getOrderByExpressions());
    }


    private OrderByCompiler() {
    }
    
    private static class OrderByVisitor extends AliasingExpressionCompiler {
        private final Set<OrderByExpression> visited = Sets.newHashSet();
        private final List<OrderByExpression> orderByExpressions = Lists.newArrayList();
        
        private OrderByVisitor(StatementContext context, GroupBy groupBy, Map<String, ParseNode> aliasParseNodeMap) {
            super(context, groupBy, aliasParseNodeMap);
        }
        
        private List<OrderByExpression> getOrderByExpressions() {
            return orderByExpressions;
        }
        
        private void addOrderByExpression(OrderByExpression orderByExpression) {
            if (!visited.contains(orderByExpression)) {
                orderByExpressions.add(orderByExpression);
                visited.add(orderByExpression);
            }
        }
    }
}
