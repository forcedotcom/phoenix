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
package phoenix.expression.visitor;

import java.util.Iterator;
import java.util.List;

import phoenix.expression.*;
import phoenix.expression.function.ScalarFunction;
import phoenix.expression.function.SingleAggregateFunction;

/**
 * 
 * Visitor for an expression (which may contain other nested expressions)
 *
 * @author jtaylor
 * @since 0.1
 */
public interface ExpressionVisitor<E> {
    public E defaultReturn(Expression node, List<E> l);
    public Iterator<Expression> defaultIterator(Expression node);
    
    public Iterator<Expression> visitEnter(AndExpression node);
    public E visitLeave(AndExpression node, List<E> l);
    
    public Iterator<Expression> visitEnter(OrExpression node);
    public E visitLeave(OrExpression node, List<E> l);
    
    public Iterator<Expression> visitEnter(ScalarFunction node);
    public E visitLeave(ScalarFunction node, List<E> l);
    
    public Iterator<Expression> visitEnter(ComparisonExpression node);
    public E visitLeave(ComparisonExpression node, List<E> l);

    public Iterator<Expression> visitEnter(LikeExpression node);
    public E visitLeave(LikeExpression node, List<E> l);
    
    public Iterator<Expression> visitEnter(SingleAggregateFunction node);
    public E visitLeave(SingleAggregateFunction node, List<E> l);
    
    public Iterator<Expression> visitEnter(CaseExpression node);
    public E visitLeave(CaseExpression node, List<E> l);
    
    public Iterator<Expression> visitEnter(NotExpression node);
    public E visitLeave(NotExpression node, List<E> l);

    public Iterator<Expression> visitEnter(InListExpression node);
    public E visitLeave(InListExpression node, List<E> l);

    public Iterator<Expression> visitEnter(IsNullExpression node);
    public E visitLeave(IsNullExpression node, List<E> l);

    public Iterator<Expression> visitEnter(SubtractExpression node);
    public E visitLeave(SubtractExpression node, List<E> l);
    
    public Iterator<Expression> visitEnter(MultiplyExpression node);
    public E visitLeave(MultiplyExpression node, List<E> l);
    
    public Iterator<Expression> visitEnter(AddExpression node);
    public E visitLeave(AddExpression node, List<E> l);
    
    public Iterator<Expression> visitEnter(DivideExpression node);
    public E visitLeave(DivideExpression node, List<E> l);
    
    public E visit(LiteralExpression node);
    public E visit(RowKeyColumnExpression node);
    public E visit(KeyValueColumnExpression node);
    
	public Iterator<Expression> visitEnter(StringConcatExpression node);
	public E visitLeave(StringConcatExpression node, List<E> l);
	
	
}
