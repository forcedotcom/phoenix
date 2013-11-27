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
package com.salesforce.phoenix.expression;

import java.util.List;

import com.salesforce.phoenix.expression.visitor.ExpressionVisitor;
import com.salesforce.phoenix.schema.PDataType;


/**
 * 
 * Subtract expression implementation
 *
 * @author jtaylor
 * @since 0.1
 */
public abstract class MultiplyExpression extends ArithmeticExpression {
    private Integer maxLength;
    private Integer scale;

    public MultiplyExpression() {
    }

    public MultiplyExpression(List<Expression> children) {
        super(children);
        for (int i=0; i<children.size(); i++) {
            Expression childExpr = children.get(i);
            if (i == 0) {
                maxLength = childExpr.getMaxLength();
                scale = childExpr.getScale();
            } else {
                maxLength = getPrecision(maxLength, childExpr.getMaxLength(), scale, childExpr.getScale());
                scale = getScale(maxLength, childExpr.getMaxLength(), scale, childExpr.getScale());
            }
        }
    }

    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        List<T> l = acceptChildren(visitor, visitor.visitEnter(this));
        T t = visitor.visitLeave(this, l);
        if (t == null) {
            t = visitor.defaultReturn(this, l);
        }
        return t;
    }

    @Override
    public String getOperatorString() {
        return " * ";
    }
    
    private static Integer getPrecision(Integer lp, Integer rp, Integer ls, Integer rs) {
    	if (ls == null || rs == null) {
    		return PDataType.MAX_PRECISION;
    	}
        int val = lp + rp;
        return Math.min(PDataType.MAX_PRECISION, val);
    }

    private static Integer getScale(Integer lp, Integer rp, Integer ls, Integer rs) {
    	// If we are adding a decimal with scale and precision to a decimal
    	// with no precision nor scale, the scale system does not apply.
    	if (ls == null || rs == null) {
    		return null;
    	}
        int val = ls + rs;
        return Math.min(PDataType.MAX_PRECISION, val);
    }
    
    @Override
    public Integer getScale() {
        return scale;
    }

    @Override
    public Integer getMaxLength() {
        return maxLength;
    }
}
