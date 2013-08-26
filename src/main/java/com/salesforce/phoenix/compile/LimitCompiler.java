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

import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.schema.*;


public class LimitCompiler {
    private static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();
    
    public static final PDatum LIMIT_DATUM = new PDatum() {
        @Override
        public boolean isNullable() {
            return false;
        }
        @Override
        public PDataType getDataType() {
            return PDataType.INTEGER;
        }
        @Override
        public Integer getByteSize() {
            return getDataType().getByteSize();
        }
        @Override
        public Integer getMaxLength() {
            return null;
        }
        @Override
        public Integer getScale() {
            return null;
        }
		@Override
		public ColumnModifier getColumnModifier() {
			return null;
		}
    };
    
    private LimitCompiler() {
    }

    public static Integer compile(StatementContext context, FilterableStatement statement) throws SQLException {
        LimitNode limitNode = statement.getLimit();
        if (limitNode == null) {
            return null;
        }
        LimitParseNodeVisitor visitor = new LimitParseNodeVisitor(context);
        limitNode.getLimitParseNode().accept(visitor);
        return visitor.getLimit();
    }
    
    private static class LimitParseNodeVisitor extends TraverseNoParseNodeVisitor<Void> {
        private final StatementContext context;
        private Integer limit;
        
        private LimitParseNodeVisitor(StatementContext context) {
            this.context = context;
        }
        
        public Integer getLimit() {
            return limit;
        }
        
        @Override
        public Void visit(LiteralParseNode node) throws SQLException {
            Object limitValue = node.getValue();
            // If limit is null, leave this.limit set to zero
            // This means that we've bound limit to null for the purpose of
            // collecting parameter metadata.
            if (limitValue != null) {
                Integer limit = (Integer)LIMIT_DATUM.getDataType().toObject(limitValue, node.getType());
                if (limit.intValue() >= 0) { // TODO: handle LIMIT 0
                    this.limit = limit;
                }
            }
            return null;
        }
    
        @Override
        public Void visit(BindParseNode node) throws SQLException {
            Object value = context.getBindManager().getBindValue(node);
            context.getBindManager().addParamMetaData(node, LIMIT_DATUM);
            // Resolve the bind value, create a LiteralParseNode, and call the visit method for it.
            // In this way, we can deal with just having a literal on one side of the expression.
            visit(NODE_FACTORY.literal(value, LIMIT_DATUM.getDataType()));
            return null;
        }
    }

}
