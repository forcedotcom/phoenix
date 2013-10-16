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
import java.util.Map;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.phoenix.compile.ColumnResolver;
import com.salesforce.phoenix.schema.AmbiguousColumnException;
import com.salesforce.phoenix.schema.ColumnNotFoundException;

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
        Map<String,ParseNode> aliasMap = rewriter.getAliasMap();
        List<TableNode> from = statement.getFrom();
        List<TableNode> normFrom = from;
        if (from.size() > 1) {
        	for (int i = 1; i < from.size(); i++) {
        		TableNode tableNode = from.get(i);
        		if (tableNode instanceof JoinTableNode) {
        			JoinTableNode joinTableNode = (JoinTableNode) tableNode;
        			ParseNode onNode = joinTableNode.getOnNode();
        			rewriter.reset();
        			ParseNode normOnNode = onNode.accept(rewriter);
        			if (onNode == normOnNode) {
        				if (from != normFrom) {
        					normFrom.add(tableNode);
        				}
        				continue;
        			}
        			if (from == normFrom) {
        				normFrom = Lists.newArrayList(from.subList(0, i));
        			}
        			TableNode normTableNode = NODE_FACTORY.join(joinTableNode.getType(), normOnNode, joinTableNode.getTable());
        			normFrom.add(normTableNode);
        		} else if (from != normFrom) {
					normFrom.add(tableNode);
				}
        	}
        }
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
            AliasedNode normAliasNode = NODE_FACTORY.aliasedNode(aliasedNode.getAlias(), normSelectNode);
            normSelectNodes.add(normAliasNode);
        }
        // Add to map in separate pass so that we don't try to use aliases
        // while processing the select expressions
        if (aliasMap != null) {
            for (int i = 0; i < normSelectNodes.size(); i++) {
                AliasedNode aliasedNode = normSelectNodes.get(i);
                ParseNode selectNode = aliasedNode.getNode();
                String alias = aliasedNode.getAlias();
                if (alias != null) {
                    aliasMap.put(alias, selectNode);
                }
            }
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
        if (normFrom == from && 
        		normWhere == where && 
                normHaving == having && 
                selectNodes == normSelectNodes && 
                groupByNodes == normGroupByNodes &&
                orderByNodes == normOrderByNodes) {
            return statement;
        }
        return NODE_FACTORY.select(normFrom, statement.getHint(), statement.isDistinct(),
                normSelectNodes, normWhere, normGroupByNodes, normHaving, normOrderByNodes,
                statement.getLimit(), statement.getBindCount(), statement.isAggregate());
    }
    
    private Map<String, ParseNode> getAliasMap() {
        return aliasMap;
    }

    private final ColumnResolver resolver;
    private final Map<String, ParseNode> aliasMap;
    
    protected ParseNodeRewriter() {
        aliasMap = null;
        resolver = null;
    }
    
    protected ParseNodeRewriter(ColumnResolver resolver, int maxAliasCount) {
        this.resolver = resolver;
        aliasMap = Maps.newHashMapWithExpectedSize(maxAliasCount);
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
    public ParseNode visitLeave(final CastParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.cast(children.get(0), node.getDataType());
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
    
    /**
     * Rewrites expressions of the form (a, b, c) = (1, 2) as a = 1 and b = 2 and c is null
     * as this is equivalent and already optimized
     * @param lhs
     * @param rhs
     * @param andNodes
     * @throws SQLException 
     */
    private void rewriteRowValueConstuctorEqualityComparison(ParseNode lhs, ParseNode rhs, List<ParseNode> andNodes) throws SQLException {
        if (lhs instanceof RowValueConstructorParseNode && rhs instanceof RowValueConstructorParseNode) {
            int i = 0;
            for (; i < Math.min(lhs.getChildren().size(),rhs.getChildren().size()); i++) {
                rewriteRowValueConstuctorEqualityComparison(lhs.getChildren().get(i), rhs.getChildren().get(i), andNodes);
            }
            for (; i < lhs.getChildren().size(); i++) {
                rewriteRowValueConstuctorEqualityComparison(lhs.getChildren().get(i), null, andNodes);
            }
            for (; i < rhs.getChildren().size(); i++) {
                rewriteRowValueConstuctorEqualityComparison(null, rhs.getChildren().get(i), andNodes);
            }
        } else if (lhs instanceof RowValueConstructorParseNode) {
            rewriteRowValueConstuctorEqualityComparison(lhs.getChildren().get(0), rhs, andNodes);
            for (int i = 1; i < lhs.getChildren().size(); i++) {
                rewriteRowValueConstuctorEqualityComparison(lhs.getChildren().get(i), null, andNodes);
            }
        } else if (rhs instanceof RowValueConstructorParseNode) {
            rewriteRowValueConstuctorEqualityComparison(lhs, rhs.getChildren().get(0), andNodes);
            for (int i = 1; i < rhs.getChildren().size(); i++) {
                rewriteRowValueConstuctorEqualityComparison(null, rhs.getChildren().get(i), andNodes);
            }
        } else if (lhs == null && rhs == null) { // null == null will end up making the query degenerate
            andNodes.add(NODE_FACTORY.comparison(CompareOp.EQUAL, null, null).accept(this));
        } else if (lhs == null) { // AND rhs IS NULL
            andNodes.add(NODE_FACTORY.isNull(rhs, false).accept(this));
        } else if (rhs == null) { // AND lhs IS NULL
            andNodes.add(NODE_FACTORY.isNull(lhs, false).accept(this));
        } else { // AND lhs = rhs
            andNodes.add(NODE_FACTORY.comparison(CompareOp.EQUAL, lhs, rhs).accept(this));
        }
    }
    
    @Override
    public ParseNode visitLeave(final ComparisonParseNode node, List<ParseNode> nodes) throws SQLException {
        ParseNode normNode = leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.comparison(node.getFilterOp(), children.get(0), children.get(1));
            }
        });
        
        CompareOp op = node.getFilterOp();
        if (op == CompareOp.EQUAL || op == CompareOp.NOT_EQUAL) {
            // Rewrite row value constructor in = or != expression, as this is the same as if it was
            // used in an equality expression for each individual part.
            ParseNode lhs = normNode.getChildren().get(0);
            ParseNode rhs = normNode.getChildren().get(1);
            if (lhs instanceof RowValueConstructorParseNode || rhs instanceof RowValueConstructorParseNode) {
                List<ParseNode> andNodes = Lists.newArrayListWithExpectedSize(Math.max(lhs.getChildren().size(), rhs.getChildren().size()));
                rewriteRowValueConstuctorEqualityComparison(lhs,rhs,andNodes);
                normNode = NODE_FACTORY.and(andNodes);
                if (op == CompareOp.NOT_EQUAL) {
                    normNode = NODE_FACTORY.not(normNode);
                }
            }
        }
        return normNode;
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
        // If we're resolving aliases and we have an unqualified ColumnParseNode,
        // check if we find the name in our alias map.
        if (aliasMap != null && node.getTableName() == null) {
            ParseNode aliasedNode = aliasMap.get(node.getName());
            // If we found something, then try to resolve it unless the two nodes are the same
            if (aliasedNode != null && !node.equals(aliasedNode)) {
                try {
                    // If we're able to resolve it, that means we have a conflict
                    resolver.resolveColumn(node.getSchemaName(), node.getTableName(), node.getName());
                    throw new AmbiguousColumnException(node.getName());
                } catch (ColumnNotFoundException e) {
                    // Not able to resolve alias as a column name as well, so we use the alias
                    return aliasedNode;
                }
            }
        }
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
    public ParseNode visit(FamilyWildcardParseNode node) throws SQLException {
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

    @Override
    public ParseNode visitLeave(RowValueConstructorParseNode node, List<ParseNode> children) throws SQLException {
        if (node.isConstant()) {
            // Strip trailing nulls from rvc as they have no meaning
            if (children.get(children.size()-1) == null) {
                children = Lists.newArrayList(children);
                do {
                    children.remove(children.size()-1);
                } while (children.size() > 0 && children.get(children.size()-1) == null);
                // If we're down to a single child, it's not a rvc anymore
                if (children.size() == 0) {
                    return null;
                }
                if (children.size() == 1) {
                    return children.get(0);
                }
            }
        }
        return leaveCompoundNode(node, children, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.rowValueConstructor(children);
            }
        });
    }
}
