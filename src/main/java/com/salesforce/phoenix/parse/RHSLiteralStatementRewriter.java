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
import java.util.List;


/**
 * 
 * Class that creates a new select statement ensuring that a literal always occurs
 * on the RHS (i.e. if literal found on the LHS, then the operator is reversed and
 * the literal is put on the RHS)
 *
 * @author jtaylor
 * @since 0.1
 */
public class RHSLiteralStatementRewriter extends ParseNodeRewriter {
    
    /**
     * Rewrite the select statement by filtering out expression nodes from the WHERE clause
     * @param statement the select statement from which to filter.
     * @return new select statement
     * @throws SQLException 
     */
    public static SelectStatement normalizeWhereClause(SelectStatement statement) throws SQLException {
        ParseNode where = statement.getWhere();
        ParseNode normWhere = where;
        if (where != null) {
            RHSLiteralStatementRewriter rewriter = new RHSLiteralStatementRewriter();
            normWhere = where.accept(rewriter);
        }
        ParseNode having = statement.getHaving();
        ParseNode normHaving= having;
        if (having != null) {
            RHSLiteralStatementRewriter rewriter = new RHSLiteralStatementRewriter();
            normHaving = having.accept(rewriter);
        }
        // Return new SELECT statement with updated WHERE clause
        if (normWhere == where && normHaving == having) {
            return statement;
        }
        return NODE_FACTORY.select(statement, normWhere, normHaving);
    }
    
    private static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();

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
    public ParseNode visitLeave(ComparisonParseNode node, List<ParseNode> nodes) throws SQLException {
         if (nodes.get(0).isConstant() && !nodes.get(1).isConstant()) {
             node = node.invert(NODE_FACTORY);
         }
         return node;
    }
}
