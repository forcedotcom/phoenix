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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

/**
 * 
 * Base class for visitors that rewrite the expression node hierarchy
 *
 * @author jtaylor 
 * @since 0.1
 */
public class ParseNodeRewriter extends TraverseAllParseNodeVisitor<ParseNode> {
    
    protected static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();

    
    /**
     * Rewrite the select statement by switching any constants to the right hand side
     * of the expression.
     * @param statement the select statement
     * @return new select statement
     * @throws SQLException 
     */
    public static SelectStatement rewrite(SelectStatement statement, ParseNodeRewriter rewriter) throws SQLException {
        ParseNode where = statement.getWhere();
        ParseNode normWhere = where;
        if (where != null) {
            rewriter.reset();
            normWhere = where.accept(rewriter);
        }
        ParseNode having = statement.getHaving();
        ParseNode normHaving= having;
        if (having != null) {
            rewriter.reset();
            normHaving = having.accept(rewriter);
        }
        List<AliasedNode> selectNodes = statement.getSelect();
        List<AliasedNode> normSelectNodes = selectNodes;
        for (int i = 0; i < selectNodes.size(); i++) {
            AliasedNode aliasedNode = selectNodes.get(i);
            ParseNode selectNode = aliasedNode.getNode();
            rewriter.reset();
            ParseNode normSelectNode = selectNode.accept(rewriter);
            if (selectNode == normSelectNode) {
                if (selectNodes != normSelectNodes) {
                    normSelectNodes.add(aliasedNode);
                }
                continue;
            }
            if (selectNodes == normSelectNodes) {
                normSelectNodes = Lists.newArrayList(selectNodes.subList(0, i));
            }
            normSelectNodes.add(NODE_FACTORY.aliasedNode(aliasedNode.getAlias(), normSelectNode));
        }
        List<ParseNode> groupByNodes = statement.getGroupBy();
        List<ParseNode> normGroupByNodes = groupByNodes;
        for (int i = 0; i < groupByNodes.size(); i++) {
            ParseNode groupByNode = groupByNodes.get(i);
            rewriter.reset();
            ParseNode normGroupByNode = groupByNode.accept(rewriter);
            if (groupByNode == normGroupByNode) {
                if (groupByNodes != normGroupByNodes) {
                    normGroupByNodes.add(groupByNode);
                }
                continue;
            }
            if (groupByNodes == normGroupByNodes) {
                normGroupByNodes = Lists.newArrayList(groupByNodes.subList(0, i));
            }
            normGroupByNodes.add(normGroupByNode);
        }
        List<OrderByNode> orderByNodes = statement.getOrderBy();
        List<OrderByNode> normOrderByNodes = orderByNodes;
        for (int i = 0; i < orderByNodes.size(); i++) {
            OrderByNode orderByNode = orderByNodes.get(i);
            ParseNode node = orderByNode.getNode();
            rewriter.reset();
            ParseNode normNode = node.accept(rewriter);
            if (node == normNode) {
                if (orderByNodes != normOrderByNodes) {
                    normOrderByNodes.add(orderByNode);
                }
                continue;
            }
            if (orderByNodes == normOrderByNodes) {
                normOrderByNodes = Lists.newArrayList(orderByNodes.subList(0, i));
            }
            normOrderByNodes.add(NODE_FACTORY.orderBy(normNode, orderByNode.isNullsLast(), orderByNode.isAscending()));
        }
        // Return new SELECT statement with updated WHERE clause
        if (normWhere == where && 
                normHaving == having && 
                selectNodes == normSelectNodes && 
                groupByNodes == normGroupByNodes &&
                orderByNodes == normOrderByNodes) {
            return statement;
        }
        return NODE_FACTORY.select(statement.getFrom(), statement.getHint(), statement.isDistinct(),
                normSelectNodes, normWhere, normGroupByNodes, normHaving, normOrderByNodes,
                statement.getLimit(), statement.getBindCount(), statement.isAggregate());
    }
    
    protected void reset() {
    }
    
    private static interface CompoundNodeFactory {
        ParseNode createNode(List<ParseNode> children);
    }
    
    private ParseNode leaveCompoundNode(CompoundParseNode node, List<ParseNode> children, CompoundNodeFactory factory) {
        if (children.equals(node.getChildren())) {
            return node;
        } else { // Child nodes have been inverted (because a literal was found on LHS)
            return factory.createNode(children);
        }
    }
    
    @Override
    public ParseNode visitLeave(AndParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.and(children);
            }
        });
    }

    @Override
    public ParseNode visitLeave(OrParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.or(children);
            }
        });
    }
    
    @Override
    public ParseNode visitLeave(SubtractParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.subtract(children);
            }
        });
    }
    
    @Override
    public ParseNode visitLeave(AddParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.add(children);
            }
        });
    }
    
    @Override
    public ParseNode visitLeave(MultiplyParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.multiply(children);
            }
        });
    }
    
    @Override
    public ParseNode visitLeave(DivideParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.divide(children);
            }
        });
    }
    
    @Override
    public ParseNode visitLeave(final FunctionParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.function(node.getName(),children);
            }
        });
    }
    
    @Override
    public ParseNode visitLeave(CaseParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.caseWhen(children);
            }
        });
    }

    @Override
    public ParseNode visitLeave(final LikeParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.like(children.get(0),children.get(1),node.isNegate());
            }
        });
    }
    
    @Override
    public ParseNode visitLeave(NotParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.not(children.get(0));
            }
        });
    }
    
    @Override
    public ParseNode visitLeave(final InListParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.inList(children, node.isNegate());
            }
        });
    }
    
    @Override
    public ParseNode visitLeave(final IsNullParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.isNull(children.get(0), node.isNegate());
            }
        });
    }
    
    @Override
    public ParseNode visitLeave(final ComparisonParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.comparison(node.getFilterOp(), children.get(0), children.get(1));
            }
        });
    }
    
    @Override
    public ParseNode visitLeave(final BetweenParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                if(node.isNegate()) {
                    return NODE_FACTORY.not(NODE_FACTORY.and(children));
                } else {
                    return NODE_FACTORY.and(children);
                }
            }
        });
    }
    
    @Override
    public ParseNode visit(ColumnParseNode node) throws SQLException {
        return node;
    }

    @Override
    public ParseNode visit(LiteralParseNode node) throws SQLException {
        return node;
    }

    @Override
    public ParseNode visit(BindParseNode node) throws SQLException {
        return node;
    }
    
    @Override
    public ParseNode visit(WildcardParseNode node) throws SQLException {
        return node;
    }
    
    @Override
    public ParseNode visit(FamilyParseNode node) throws SQLException {
        return node;
    }
    
    @Override
    public List<ParseNode> newElementList(int size) {
        return new ArrayList<ParseNode>(size);
    }
    
    @Override
    public ParseNode visitLeave(StringConcatParseNode node, List<ParseNode> l) throws SQLException {
        return node;
    }

    @Override
    public void addElement(List<ParseNode> l, ParseNode element) {
        if (element != null) {
            l.add(element);
        }
    }
}
