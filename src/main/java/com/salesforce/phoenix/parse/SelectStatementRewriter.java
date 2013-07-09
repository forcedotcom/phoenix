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
import java.util.*;



/**
 * 
 * Class that creates a new select statement by filtering out nodes.
 * Currently only supports filtering out boolean nodes (i.e. nodes
 * that may be ANDed and ORed together.
 *
 * TODO: generize this
 * @author jtaylor
 * @since 0.1
 */
public class SelectStatementRewriter extends ParseNodeRewriter {
    
    /**
     * Rewrite the select statement by filtering out expression nodes from the WHERE clause
     * @param statement the select statement from which to filter.
     * @param removeNodes expression nodes to filter out of WHERE clause.
     * @return new select statement
     * @throws SQLException 
     */
    public static SelectStatement removeFromWhereClause(SelectStatement statement, Set<ParseNode> removeNodes) throws SQLException {
        if (removeNodes.isEmpty()) {
            return statement;
        }
        ParseNode where = statement.getWhere();
        SelectStatementRewriter rewriter = new SelectStatementRewriter(removeNodes);
        where = where.accept(rewriter);
        // Return new SELECT statement with updated WHERE clause
        return NODE_FACTORY.select(statement, where, statement.getHaving());
    }
    
    /**
     * Rewrite the select statement by filtering out expression nodes from the HAVING clause
     * and anding them with the WHERE clause.
     * @param statement the select statement from which to move the nodes.
     * @param moveNodes expression nodes to filter out of HAVING clause and add to WHERE clause.
     * @return new select statement
     * @throws SQLException 
     */
    public static SelectStatement moveFromHavingToWhereClause(SelectStatement statement, Set<ParseNode> moveNodes) throws SQLException {
        if (moveNodes.isEmpty()) {
            return statement;
        }
        ParseNode andNode = NODE_FACTORY.and(new ArrayList<ParseNode>(moveNodes));
        ParseNode having = statement.getHaving();
        SelectStatementRewriter rewriter = new SelectStatementRewriter(moveNodes);
        having = having.accept(rewriter);
        ParseNode where = statement.getWhere();
        if (where == null) {
            where = andNode;
        } else {
            where = NODE_FACTORY.and(Arrays.asList(where,andNode));
        }
        // Return new SELECT statement with updated WHERE and HAVING clauses
        return NODE_FACTORY.select(statement, where, having);
    }
    
    private static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();

    private final Set<ParseNode> removeNodes;
    
    private SelectStatementRewriter(Set<ParseNode> removeNodes) {
        this.removeNodes = removeNodes;
    }
    
    private static interface CompoundNodeFactory {
        ParseNode createNode(List<ParseNode> children);
    }
    
    private boolean enterCompoundNode(ParseNode node) {
        if (removeNodes.contains(node)) {
            return false;
        }
        return true;
    }
    
    private ParseNode leaveCompoundNode(CompoundParseNode node, List<ParseNode> children, CompoundNodeFactory factory) {
        int newSize = children.size();
        int oldSize = node.getChildren().size();
        if (newSize == oldSize) {
            return node;
        } else if (newSize > 1) {
            return factory.createNode(children);
        } else if (newSize == 1) {
            // TODO: keep or collapse? Maybe be helpful as context of where a problem occurs if a node could not be consumed
            return(children.get(0));
        } else {
            return null;
        }
    }
    
    @Override
    public boolean visitEnter(AndParseNode node) throws SQLException {
        return enterCompoundNode(node);
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
    public boolean visitEnter(OrParseNode node) throws SQLException {
        return enterCompoundNode(node);
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
    public boolean visitEnter(ComparisonParseNode node) throws SQLException {
        if (removeNodes.contains(node)) {
            return false;
        }
        return true;
    }

    @Override
    public ParseNode visitLeave(ComparisonParseNode node, List<ParseNode> c) throws SQLException {
        return c.isEmpty() ? null : node;
    }
    
    @Override
    public boolean visitEnter(LikeParseNode node) throws SQLException {
        if (removeNodes.contains(node)) {
            return false;
        }
        return true;
    }
    
    @Override
    public ParseNode visitLeave(LikeParseNode node, List<ParseNode> c) throws SQLException {
        return c.isEmpty() ? null : node;
    }
    
    @Override
    public boolean visitEnter(InListParseNode node) throws SQLException {
        if (removeNodes.contains(node)) {
            return false;
        }
        return true;
    }
    
    @Override
    public ParseNode visitLeave(InListParseNode node, List<ParseNode> c) throws SQLException {
        return c.isEmpty() ? null : node;
    }
}
