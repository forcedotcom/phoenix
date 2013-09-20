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
package com.salesforce.phoenix.parse;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import com.salesforce.phoenix.expression.function.CountAggregateFunction;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunctionInfo;

/**
 * 
 * Top level node representing a SQL statement
 *
 * @author jtaylor
 * @since 0.1
 */
public class SelectStatement implements FilterableStatement {
    public static final SelectStatement SELECT_ONE =
            new SelectStatement(
                    Collections.<TableNode>emptyList(), null, false, 
                    Collections.<AliasedNode>singletonList(new AliasedNode(null,new LiteralParseNode(1))),
                    null, Collections.<ParseNode>emptyList(),
                    null, Collections.<OrderByNode>emptyList(),
                    null, 0, false);
    public static final SelectStatement COUNT_ONE =
            new SelectStatement(
                    Collections.<TableNode>emptyList(), null, false,
                    Collections.<AliasedNode>singletonList(
                    new AliasedNode(null, 
                        new AggregateFunctionParseNode(
                                CountAggregateFunction.NORMALIZED_NAME, 
                                LiteralParseNode.STAR, 
                                new BuiltInFunctionInfo(CountAggregateFunction.class, CountAggregateFunction.class.getAnnotation(BuiltInFunction.class))))),
                    null, Collections.<ParseNode>emptyList(), 
                    null, Collections.<OrderByNode>emptyList(), 
                    null, 0, true);
    public static SelectStatement create(SelectStatement select, HintNode hint) {
        if (select.getHint() == hint) {
            return select;
        }
        return new SelectStatement(select.getFrom(), hint, select.isDistinct(), 
                select.getSelect(), select.getWhere(), select.getGroupBy(), select.getHaving(), 
                select.getOrderBy(), select.getLimit(), select.getBindCount(), select.isAggregate());
    }
    
    private final List<TableNode> fromTable;
    private final HintNode hint;
    private final boolean isDistinct;
    private final List<AliasedNode> select;
    private final ParseNode where;
    private final List<ParseNode> groupBy;
    private final ParseNode having;
    private final List<OrderByNode> orderBy;
    private final LimitNode limit;
    private final int bindCount;
    private final boolean isAggregate;
    
    // Filter out constants from GROUP BY as they're useless
    private static List<ParseNode> filterGroupByConstants(List<ParseNode> nodes) {
        List<ParseNode> filteredNodes = nodes;
        for (int i = 0; i < nodes.size(); i++) {
            ParseNode node = nodes.get(i);
            if (node.isConstant()) {
                if (filteredNodes == nodes) {
                    filteredNodes = Lists.newArrayListWithExpectedSize(nodes.size());
                    filteredNodes.addAll(nodes.subList(0, i));
                }
            } else if (filteredNodes != nodes) {
                filteredNodes.add(node);
            }
        }
        return filteredNodes;
    }
    
    // Filter out constants from ORDER BY as they're useless
    private static List<OrderByNode> filterOrderByConstants(List<OrderByNode> nodes) {
        List<OrderByNode> filteredNodes = nodes;
        for (int i = 0; i < nodes.size(); i++) {
            ParseNode node = nodes.get(i).getNode();
            if (node.isConstant()) {
                if (filteredNodes == nodes) {
                    filteredNodes = Lists.newArrayListWithExpectedSize(nodes.size());
                    filteredNodes.addAll(nodes.subList(0, i));
                }
            } else if (filteredNodes != nodes) {
                filteredNodes.add(nodes.get(i));
            }
        }
        return filteredNodes;
    }
    
    protected SelectStatement(List<? extends TableNode> from, HintNode hint, boolean isDistinct, List<AliasedNode> select, ParseNode where, List<ParseNode> groupBy, ParseNode having, List<OrderByNode> orderBy, LimitNode limit, int bindCount, boolean isAggregate) {
        this.fromTable = Collections.unmodifiableList(from);
        this.hint = hint == null ? HintNode.EMPTY_HINT_NODE : hint;
        this.isDistinct = isDistinct;
        this.select = Collections.unmodifiableList(select);
        this.where = where;
        this.groupBy = Collections.unmodifiableList(filterGroupByConstants(groupBy));
        this.having = having;
        this.orderBy = Collections.unmodifiableList(filterOrderByConstants(orderBy));
        this.limit = limit;
        this.bindCount = bindCount;
        this.isAggregate = isAggregate || !this.groupBy.isEmpty() || this.having != null;
    }
    
    @Override
    public boolean isDistinct() {
        return isDistinct;
    }
    
    @Override
    public LimitNode getLimit() {
        return limit;
    }
    
    @Override
    public int getBindCount() {
        return bindCount;
    }
    
    public List<TableNode> getFrom() {
        return fromTable;
    }
    
    @Override
    public HintNode getHint() {
        return hint;
    }
    
    public List<AliasedNode> getSelect() {
        return select;
    }
    /**
     * Gets the where condition, or null if none.
     */
    @Override
    public ParseNode getWhere() {
        return where;
    }
    
    /**
     * Gets the group-by, containing at least 1 element, or null, if none.
     */
    public List<ParseNode> getGroupBy() {
        return groupBy;
    }
    
    public ParseNode getHaving() {
        return having;
    }
    
    /**
     * Gets the order-by, containing at least 1 element, or null, if none.
     */
    @Override
    public List<OrderByNode> getOrderBy() {
        return orderBy;
    }

    @Override
    public boolean isAggregate() {
        return isAggregate;
    }
}
