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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.TrackOrderPreservingExpressionCompiler.Ordering;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.OrderByExpression;
import com.salesforce.phoenix.parse.FilterableStatement;
import com.salesforce.phoenix.parse.OrderByNode;
import com.salesforce.phoenix.query.ConnectionQueryServices.Feature;
import com.salesforce.phoenix.schema.ColumnModifier;

/**
 * Validates ORDER BY clause and builds up a list of referenced columns.
 * 
 * @author syyang
 * @since 0.1
 */
public class OrderByCompiler {
    public static class OrderBy {
        public static final OrderBy EMPTY_ORDER_BY = new OrderBy(Collections.<OrderByExpression>emptyList());
        /**
         * Used to indicate that there was an ORDER BY, but it was optimized out because
         * rows are already returned in this order. 
         */
        public static final OrderBy FWD_ROW_KEY_ORDER_BY = new OrderBy(Collections.<OrderByExpression>emptyList());
        public static final OrderBy REV_ROW_KEY_ORDER_BY = new OrderBy(Collections.<OrderByExpression>emptyList());
        
        private final List<OrderByExpression> orderByExpressions;
        
        private OrderBy(List<OrderByExpression> orderByExpressions) {
            this.orderByExpressions = ImmutableList.copyOf(orderByExpressions);
        }

        public List<OrderByExpression> getOrderByExpressions() {
            return orderByExpressions;
        }
    }
    /**
     * Gets a list of columns in the ORDER BY clause
     * @param context the query context for tracking various states
     * associated with the given select statement
     * @param statement TODO
     * @param groupBy the list of columns in the GROUP BY clause
     * @param limit the row limit or null if no limit
     * @return the compiled ORDER BY clause
     * @throws SQLException
     */
    public static OrderBy compile(StatementContext context,
                                  FilterableStatement statement,
                                  GroupBy groupBy, Integer limit) throws SQLException {
        List<OrderByNode> orderByNodes = statement.getOrderBy();
        if (orderByNodes.isEmpty()) {
            return OrderBy.EMPTY_ORDER_BY;
        }
        // accumulate columns in ORDER BY
        TrackOrderPreservingExpressionCompiler visitor = 
                new TrackOrderPreservingExpressionCompiler(context, groupBy, 
                        orderByNodes.size(), Ordering.ORDERED);
        LinkedHashSet<OrderByExpression> orderByExpressions = Sets.newLinkedHashSetWithExpectedSize(orderByNodes.size());
        for (OrderByNode node : orderByNodes) {
            boolean isAscending = node.isAscending();
            Expression expression = node.getNode().accept(visitor);
            if (!expression.isStateless() && visitor.addEntry(expression, isAscending ? null : ColumnModifier.SORT_DESC)) {
                // Detect mix of aggregate and non aggregates (i.e. ORDER BY txns, SUM(txns)
                if (!visitor.isAggregate()) {
                    if (statement.isAggregate() || statement.isDistinct()) {
                        // Detect ORDER BY not in SELECT DISTINCT: SELECT DISTINCT count(*) FROM t ORDER BY x
                        if (statement.isDistinct()) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.ORDER_BY_NOT_IN_SELECT_DISTINCT)
                            .setMessage(expression.toString()).build().buildException();
                        }
                        ExpressionCompiler.throwNonAggExpressionInAggException(expression.toString());
                    }
                }
                if (expression.getDataType().isArrayType()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.ORDER_BY_ARRAY_NOT_SUPPORTED)
                    .setMessage(expression.toString()).build().buildException();
                }
                if (expression.getColumnModifier() == ColumnModifier.SORT_DESC) {
                    isAscending = !isAscending;
                }
                OrderByExpression orderByExpression = new OrderByExpression(expression, node.isNullsLast(), isAscending);
                orderByExpressions.add(orderByExpression);
            }
            visitor.reset();
        }
       
        if (orderByExpressions.isEmpty()) {
            return OrderBy.EMPTY_ORDER_BY;
        }
        // If we're ordering by the order returned by the scan, we don't need an order by
        if (visitor.isOrderPreserving()) {
            if (visitor.isReverse()) {
                if (context.getConnection().getQueryServices().supportsFeature(Feature.REVERSE_SCAN)) {
                    return OrderBy.REV_ROW_KEY_ORDER_BY;
                }
            } else {
                return OrderBy.FWD_ROW_KEY_ORDER_BY;
            }
        }

        return new OrderBy(Lists.newArrayList(orderByExpressions.iterator()));
    }


    private OrderByCompiler() {
    }
}
