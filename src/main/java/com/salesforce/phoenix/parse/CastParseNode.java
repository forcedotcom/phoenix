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
import java.util.Collections;
import java.util.List;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.function.RoundDecimalExpression;
import com.salesforce.phoenix.expression.function.RoundTimestampExpression;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.TypeMismatchException;

/**
 * 
 * Node representing the CAST operator in SQL.
 * 
 * @author samarth.jain
 * @since 0.1
 *
 */
public class CastParseNode extends UnaryParseNode {
	
	private final PDataType dt;
	
	CastParseNode(ParseNode expr, String dataType) {
		super(expr);
		dt = PDataType.fromSqlTypeName(dataType);
	}
	
	CastParseNode(ParseNode expr, PDataType dataType) {
		super(expr);
		dt = dataType;
	}

	@Override
    public <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException {
        List<T> l = Collections.emptyList();
        if (visitor.visitEnter(this)) {
            l = acceptChildren(visitor);
        }
        return visitor.visitLeave(this, l);
    }

	public PDataType getDataType() {
		return dt;
	}
	
	public static Expression convertToRoundExpressionIfNeeded(PDataType fromDataType, PDataType targetDataType, List<Expression> expressions) throws SQLException {
	    Expression firstChildExpr = expressions.get(0);
	    if(fromDataType == targetDataType) {
	        return firstChildExpr;
	    } else if(fromDataType == PDataType.DECIMAL && targetDataType.isCoercibleTo(PDataType.LONG)) {
	        return new RoundDecimalExpression(expressions);
	    } else if((fromDataType == PDataType.TIMESTAMP || fromDataType == PDataType.UNSIGNED_TIMESTAMP) && targetDataType.isCoercibleTo(PDataType.DATE)) {
	        return RoundTimestampExpression.create(expressions);
	    } else if(!fromDataType.isCoercibleTo(targetDataType)) {
	        throw new TypeMismatchException(fromDataType, targetDataType, firstChildExpr.toString());
	    }
	    return firstChildExpr;
	}
}
