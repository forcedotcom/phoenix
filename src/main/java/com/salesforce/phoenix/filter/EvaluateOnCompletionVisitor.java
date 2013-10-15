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
package com.salesforce.phoenix.filter;

import java.util.Iterator;

import com.salesforce.phoenix.expression.CaseExpression;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.IsNullExpression;
import com.salesforce.phoenix.expression.RowKeyColumnExpression;
import com.salesforce.phoenix.expression.RowValueConstructorExpression;
import com.salesforce.phoenix.expression.visitor.TraverseAllExpressionVisitor;


/**
 * 
 * Implementation of ExpressionVisitor for the expression used by the
 * BooleanExpressionFilter that looks for expressions that need to be
 * evaluated upon completion. Examples include:
 * - CaseExpression with an else clause, since upon completion, the
 * else clause would apply if the when clauses could not be evaluated
 * due to the absense of a value.
 * - IsNullExpression that's not negated, since upon completion, we
 * know definitively that a column value was not found.
 * - row key columns are used, since we may never have encountered a
 * key value column of interest, but the expression may evaluate to true
 * just based on the row key columns.
 * @author jtaylor
 * @since 0.1
 */
public class EvaluateOnCompletionVisitor extends TraverseAllExpressionVisitor<Void> {
    private boolean evaluateOnCompletion = false;
    
    public boolean evaluateOnCompletion() {
        return evaluateOnCompletion;
    }
    
    @Override
    public Iterator<Expression> visitEnter(IsNullExpression node) {
        evaluateOnCompletion |= !node.isNegate();
        return null;
    }
    @Override
    public Iterator<Expression> visitEnter(CaseExpression node) {
        evaluateOnCompletion |= node.hasElse();
        return null;
    }
    @Override
    public Void visit(RowKeyColumnExpression node) {
        evaluateOnCompletion = true;
        return null;
    }
    @Override
    public Iterator<Expression> visitEnter(RowValueConstructorExpression node) {
        evaluateOnCompletion = true;
        return null;
    }

}
