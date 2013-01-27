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
package phoenix.compile;

import java.util.Iterator;
import java.util.Map;

import phoenix.expression.Expression;

import com.google.common.collect.Maps;

/**
 * 
 * Class to manage list of expressions inside of a select statement by
 * deduping them.
 *
 * @author jtaylor
 * @since 0.1
 */
public class ExpressionManager {
    // Use a Map instead of a Set because we need to get and return
    // the existing Expression
    private final Map<Expression, Expression> expressionMap;
    
    public ExpressionManager() {
        expressionMap = Maps.newHashMap();
    }
    
    /**
     * Add the expression to the set of known expressions for the select
     * clause. If the expression is already in the set, then the new one
     * passed in is ignored.
     * @param expression the new expression to add
     * @return the new expression if not already present in the set and
     * the existing one otherwise.
     */
    public Expression addIfAbsent(Expression expression) {
        Expression existingExpression = expressionMap.get(expression);
        if (existingExpression == null) {
            expressionMap.put(expression, expression);
            return expression;
        }
        return existingExpression;
    }
    
    public int getExpressionCount() {
        return expressionMap.size();
    }
    
    public Iterator<Expression> getExpressions() {
        return expressionMap.keySet().iterator();
    }
}
