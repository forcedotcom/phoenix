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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.http.annotation.Immutable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.salesforce.phoenix.compile.TrackOrderPreservingExpressionCompiler.Entry;
import com.salesforce.phoenix.compile.TrackOrderPreservingExpressionCompiler.Ordering;
import com.salesforce.phoenix.coprocessor.GroupedAggregateRegionObserver;
import com.salesforce.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.expression.CoerceExpression;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.parse.AliasedNode;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.schema.AmbiguousColumnException;
import com.salesforce.phoenix.schema.ColumnNotFoundException;
import com.salesforce.phoenix.schema.PDataType;


/**
 * 
 * Validates GROUP BY clause and builds a {@link GroupBy} instance to encapsulate the
 * group by expressions.
 *
 * @author jtaylor
 * @since 0.1
 */
public class GroupByCompiler {
    @Immutable
    public static class GroupBy {
        private final List<Expression> expressions;
        private final List<Expression> keyExpressions;
        private final String scanAttribName;
        public static final GroupByCompiler.GroupBy EMPTY_GROUP_BY = new GroupBy(new GroupByBuilder());
        
        private GroupBy(GroupByBuilder builder) {
            this.expressions = ImmutableList.copyOf(builder.expressions);
            this.keyExpressions = ImmutableList.copyOf(builder.keyExpressions);
            this.scanAttribName = builder.scanAttribName;
            assert(expressions.size() == keyExpressions.size());
        }
        
        public List<Expression> getExpressions() {
            return expressions;
        }
        
        public List<Expression> getKeyExpressions() {
            return keyExpressions;
        }
        
        public String getScanAttribName() {
            return scanAttribName;
        }
        
        public boolean isEmpty() {
            return expressions.isEmpty();
        }
        
        public static class GroupByBuilder {
            private String scanAttribName;
            private List<Expression> expressions = Collections.emptyList();
            private List<Expression> keyExpressions = Collections.emptyList();

            public GroupByBuilder() {
            }
            
            public GroupByBuilder setScanAttribName(String scanAttribName) {
                this.scanAttribName = scanAttribName;
                return this;
            }
            
            public GroupByBuilder setExpressions(List<Expression> expressions) {
                this.expressions = expressions;
                return this;
            }
            
            public GroupByBuilder setKeyExpressions(List<Expression> keyExpressions) {
                this.keyExpressions = keyExpressions;
                return this;
            }
            
            public GroupBy build() {
                return new GroupBy(this);
            }
        }

        public boolean isOrderPreserving() {
            return !GroupedAggregateRegionObserver.UNORDERED_GROUP_BY_EXPRESSIONS.equals(scanAttribName);
        }
        
        public void explain(List<String> planSteps) {
            if (scanAttribName != null) {
                if (UngroupedAggregateRegionObserver.UNGROUPED_AGG.equals(scanAttribName)) {
                    planSteps.add("    SERVER AGGREGATE INTO SINGLE ROW");
                } else if (GroupedAggregateRegionObserver.UNORDERED_GROUP_BY_EXPRESSIONS.equals(scanAttribName)) {
                    planSteps.add("    SERVER AGGREGATE INTO DISTINCT ROWS BY " + getExpressions());                    
                } else {
                    planSteps.add("    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY " + getExpressions());                    
                }
            }
        }
    }

    /**
     * Get list of columns in the GROUP BY clause.
     * @param context query context kept between compilation of different query clauses
     * @param statement SQL statement being compiled
     * @return the {@link GroupBy} instance encapsulating the group by clause
     * @throws ColumnNotFoundException if column name could not be resolved
     * @throws AmbiguousColumnException if an unaliased column name is ambiguous across multiple tables
     */
    public static GroupBy compile(StatementContext context, SelectStatement statement) throws SQLException {
        List<ParseNode> groupByNodes = statement.getGroupBy();
        /**
         * Distinct can use an aggregate plan if there's no group by.
         * Otherwise, we need to insert a step after the Merge that dedups.
         * Order by only allowed on columns in the select distinct
         */
        if (groupByNodes.isEmpty()) {
            if (statement.isAggregate()) {
                return new GroupBy.GroupByBuilder().setScanAttribName(UngroupedAggregateRegionObserver.UNGROUPED_AGG).build();
            }
            if (!statement.isDistinct()) {
                return GroupBy.EMPTY_GROUP_BY;
            }
            
            groupByNodes = Lists.newArrayListWithExpectedSize(statement.getSelect().size());
            for (AliasedNode aliasedNode : statement.getSelect()) {
                groupByNodes.add(aliasedNode.getNode());
            }
        }

       // Accumulate expressions in GROUP BY
        TrackOrderPreservingExpressionCompiler groupByVisitor =
                new TrackOrderPreservingExpressionCompiler(context, 
                        GroupBy.EMPTY_GROUP_BY, groupByNodes.size(), 
                        Ordering.UNORDERED);
        for (ParseNode node : groupByNodes) {
            Expression expression = node.accept(groupByVisitor);
            if (groupByVisitor.isAggregate()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.AGGREGATE_IN_GROUP_BY)
                    .setMessage(expression.toString()).build().buildException();
            }
            groupByVisitor.addEntry(expression);
            groupByVisitor.reset();
        }
        
        List<Entry> groupByEntries = groupByVisitor.getEntries();
        if (groupByEntries.isEmpty()) {
            return GroupBy.EMPTY_GROUP_BY;
        }
        
        boolean isRowKeyOrderedGrouping = groupByVisitor.isOrderPreserving();
        List<Expression> expressions = Lists.newArrayListWithCapacity(groupByEntries.size());
        List<Expression> keyExpressions = expressions;
        String groupExprAttribName;
        // This is true if the GROUP BY is composed of only PK columns. We further check here that
        // there are no "gaps" in the PK columns positions used (i.e. we start with the first PK
        // column and use each subsequent one in PK order).
        if (isRowKeyOrderedGrouping) {
            groupExprAttribName = GroupedAggregateRegionObserver.KEY_ORDERED_GROUP_BY_EXPRESSIONS;
            for (Entry groupByEntry : groupByEntries) {
                expressions.add(groupByEntry.getExpression());
            }
        } else {
            /*
             * Otherwise, our coprocessor needs to collect all distinct groups within a region, sort them, and
             * hold on to them until the scan completes.
             */
            groupExprAttribName = GroupedAggregateRegionObserver.UNORDERED_GROUP_BY_EXPRESSIONS;
            /*
             * Put fixed length nullables at the end, so that we can represent null by the absence of the trailing
             * value in the group by key. If there is more than one, we'll need to convert the ones not at the end
             * into a Decimal so that we can use an empty byte array as our representation for null (which correctly
             * maintains the sort order). We convert the Decimal back to the appropriate type (Integer or Long) when
             * it's retrieved from the result set.
             * 
             * More specifically, order into the following buckets:
             *   1) non nullable fixed width
             *   2) variable width
             *   3) nullable fixed width
             * Within each bucket, order based on the column position in the schema. Putting the fixed width values
             * in the beginning optimizes access to subsequent values.
             */
            Collections.sort(groupByEntries, new Comparator<Entry>() {
                @Override
                public int compare(Entry o1, Entry o2) {
                    Expression e1 = o1.getExpression();
                    Expression e2 = o2.getExpression();
                    boolean isFixed1 = e1.getDataType().isFixedWidth();
                    boolean isFixed2 = e2.getDataType().isFixedWidth();
                    boolean isFixedNullable1 = e1.isNullable() &&isFixed1;
                    boolean isFixedNullable2 = e2.isNullable() && isFixed2;
                    if (isFixedNullable1 == isFixedNullable2) {
                        if (isFixed1 == isFixed2) {
                            // Not strictly necessary, but forces the order to match the schema
                            // column order (with PK columns before value columns).
                            return o1.getColumnPosition() - o2.getColumnPosition();
                        } else if (isFixed1) {
                            return -1;
                        } else {
                            return 1;
                        }
                    } else if (isFixedNullable1) {
                        return 1;
                    } else {
                        return -1;
                    }
                }
            });
            for (Entry groupByEntry : groupByEntries) {
                expressions.add(groupByEntry.getExpression());
            }
            for (int i = expressions.size()-2; i >= 0; i--) {
                Expression expression = expressions.get(i);
                PDataType keyType = getKeyType(expression);
                if (keyType == expression.getDataType()) {
                    continue;
                }
                // Copy expressions only when keyExpressions will be different than expressions
                if (keyExpressions == expressions) {
                    keyExpressions = new ArrayList<Expression>(expressions);
                }
                // Wrap expression in an expression that coerces the expression to the required type..
                // This is done so that we have a way of expressing null as an empty key when more
                // than one fixed and nullable types are used in a group by clause
                keyExpressions.set(i, CoerceExpression.create(expression, keyType));
            }
        }

        // Set attribute with serialized expressions for coprocessor
        // FIXME: what if group by is empty (i.e. only literals)?
        GroupedAggregateRegionObserver.serializeIntoScan(context.getScan(), groupExprAttribName, keyExpressions);
        GroupBy groupBy = new GroupBy.GroupByBuilder().setScanAttribName(groupExprAttribName).setExpressions(expressions).setKeyExpressions(keyExpressions).build();
        return groupBy;
    }
    
    private static PDataType getKeyType(Expression expression) {
        PDataType type = expression.getDataType();
        if (!expression.isNullable() || !type.isFixedWidth()) {
            return type;
        }
        if (type.isCoercibleTo(PDataType.DECIMAL)) {
            return PDataType.DECIMAL;
        }
        // Should never happen
        throw new IllegalStateException("Multiple occurrences of type " + type + " may not occur in a GROUP BY clause");
    }
    
    private GroupByCompiler() {
    }
}
