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

import com.salesforce.phoenix.compile.StatementContext;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.function.CeilDateExpression;
import com.salesforce.phoenix.expression.function.CeilDecimalExpression;
import com.salesforce.phoenix.expression.function.CeilTimestampExpression;
import com.salesforce.phoenix.expression.function.ScalarFunction;
import com.salesforce.phoenix.schema.PDataType;

public class CeilParseNode extends FunctionParseNode {
    
    CeilParseNode(String name, List<ParseNode> children, BuiltInFunctionInfo info) {
        super(name, children, info);
    }

    @Override
    public ScalarFunction create(List<Expression> children, StatementContext context) throws SQLException {
        final Expression firstChild = children.get(0);
        final PDataType firstChildDataType = firstChild.getDataType();
        if(firstChildDataType.isCoercibleTo(PDataType.DATE)) {
            return new CeilDateExpression(children);
        } else if(firstChildDataType == PDataType.TIMESTAMP) {
            return new CeilTimestampExpression(children) ;
        } else if(firstChildDataType.isCoercibleTo(PDataType.DECIMAL)) {
            return new CeilDecimalExpression(children);
        } else {
            throw new SQLException("Invalid data type: " + firstChildDataType);
        }
    }
    
    /**
     * When ceiling off decimals, user need not specify the scale. In such cases, 
     * we need to prevent the function from getting evaluated as null. This is really
     * a hack. A better way would have been if {@link BuiltInFunctionInfo} provided a 
     * way of associating default values for each permissible data type.
     * Till then, this will have to do.
     */
    @Override
    public boolean evalToNullIfParamIsNull(StatementContext context, int index) throws SQLException {
        return false;
    }

}
