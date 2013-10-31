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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Writable;

import com.salesforce.phoenix.expression.visitor.ExpressionVisitor;
import com.salesforce.phoenix.schema.PDatum;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Interface for general expression evaluation
 *
 * @author jtaylor
 * @since 0.1
 */
public interface Expression extends PDatum, Writable {
    /**
     * Access the value by setting a pointer to it (as opposed to making
     * a copy of it which can be expensive)
     * @param tuple Single row result during scan iteration
     * @param ptr Pointer to byte value being accessed
     * @return true if the expression could be evaluated (i.e. ptr was set)
     * and false otherwise
     */
    boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr);
    
    /**
     * Means of traversing expression tree through visitor.
     * @param visitor
     */
    <T> T accept(ExpressionVisitor<T> visitor);
    
    /**
     * @return the child expressions
     */
    List<Expression> getChildren();
    
    /**
     * Resets the state of a expression back to its initial state and
     * enables the expession to be evaluated incrementally (which
     * occurs during filter evaluation where we see one key value at
     * a time; it's possible to evaluate immediately rather than
     * wait until all key values have been seen). Note that when
     * evaluating incrementally, you must call this method before
     * processing a new row.
     */
    void reset();
    
    /**
     * @return true if the expression represents a constant value and
     * false otherwise.
     */
    boolean isConstant();
}
