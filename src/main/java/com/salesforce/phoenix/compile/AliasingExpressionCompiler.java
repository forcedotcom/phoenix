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
import java.util.Map;

import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.parse.ColumnParseNode;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.schema.ColumnNotFoundException;

/**
 * 
 * Expression compiler that may reference an expression through an alias
 *
 * @author jtaylor
 * @since 1.2
 */
public class AliasingExpressionCompiler extends ExpressionCompiler {
    private final Map<String, ParseNode> aliasParseNodeMap;

    AliasingExpressionCompiler(StatementContext context, Map<String, ParseNode> aliasParseNodeMap) {
        this(context, GroupBy.EMPTY_GROUP_BY, aliasParseNodeMap);
    }
    
    AliasingExpressionCompiler(StatementContext context, GroupBy groupBy, Map<String, ParseNode> aliasParseNodeMap) {
        super(context, groupBy);
        this.aliasParseNodeMap = aliasParseNodeMap;
    }
    
    @Override
    public Expression visit(ColumnParseNode node) throws SQLException {
        try {
            return super.visit(node);
        } catch (ColumnNotFoundException e) {
            // If we cannot find the column reference, check out alias map instead
            ParseNode aliasedNode = aliasParseNodeMap.get(node.getName());
            if (aliasedNode != null) { // If we found an alias, in-line the parse nodes
                return aliasedNode.accept(this);
            }
            throw e;
        }
    }
    
}
