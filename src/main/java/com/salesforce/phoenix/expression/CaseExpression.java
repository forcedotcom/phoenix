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

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.expression.visitor.ExpressionVisitor;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * CASE/WHEN expression implementation
 *
 * @author jtaylor
 * @since 0.1
 */
public class CaseExpression extends BaseCompoundExpression {
    private static final int FULLY_EVALUATE = -1;
    
    private short evalIndex = FULLY_EVALUATE;
    private boolean foundIndex;
    private PDataType returnType;
   
    public CaseExpression() {
    }
    
    private static List<Expression> coerceIfNecessary(List<Expression> children) throws SQLException {
        boolean isChildTypeUnknown = false;
        PDataType returnType = children.get(0).getDataType();
        for (int i = 2; i < children.size(); i+=2) {
            Expression child = children.get(i);
            PDataType childType = child.getDataType();
            if (childType == null) {
                isChildTypeUnknown = true;
            } else if (returnType == null) {
                returnType = childType;
                isChildTypeUnknown = true;
            } else if (returnType == childType || childType.isCoercibleTo(returnType)) {
                continue;
            } else if (returnType.isCoercibleTo(childType)) {
                returnType = childType;
            } else {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CONVERT_TYPE)
                    .setMessage("Case expressions must have common type: " + returnType + " cannot be coerced to " + childType)
                    .build().buildException();
            }
        }
        // If we found an "unknown" child type and the return type is a number
        // make the return type be the most general number type of DECIMAL.
        if (isChildTypeUnknown && returnType.isCoercibleTo(PDataType.DECIMAL)) {
            returnType = PDataType.DECIMAL;
        }
        List<Expression> newChildren = children;
        for (int i = 0; i < children.size(); i+=2) {
            Expression child = children.get(i);
            PDataType childType = child.getDataType();
            if (childType != returnType) {
                if (newChildren == children) {
                    newChildren = new ArrayList<Expression>(children);
                }
                newChildren.set(i, CoerceExpression.create(child, returnType));
            }
        }
        return newChildren;
    }
    /**
     * Construct CASE/WHEN expression
     * @param expressions list of expressions in the form of:
     *  ((<result expression>, <boolean expression>)+, [<optional else result expression>])
     * @throws SQLException if return type of case expressions do not match and cannot
     *  be coerced to a common type
     */
    public CaseExpression(List<Expression> expressions) throws SQLException {
        super(coerceIfNecessary(expressions));
        returnType = children.get(0).getDataType();
    }
    
    private boolean isPartiallyEvaluating() {
        return evalIndex != FULLY_EVALUATE;
    }
    
    public boolean hasElse() {
        return children.size() % 2 != 0;
    }
    
    @Override
    public boolean isNullable() {
        // If any expression is nullable or there's no else clause
        // return true since null may be returned.
        if (super.isNullable() || !hasElse()) {
            return true;
        }
        return children.get(children.size()-1).isNullable();
    }

    @Override
    public PDataType getDataType() {
        return returnType;
    }

    @Override
    public void reset() {
        foundIndex = false;
        evalIndex = 0;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        this.returnType = PDataType.values()[WritableUtils.readVInt(input)];
    }
    
    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeVInt(output, this.returnType.ordinal());
    }
    
    public int evaluateIndexOf(Tuple tuple, ImmutableBytesWritable ptr) {
        if (foundIndex) {
            return evalIndex;
        }
        int size = children.size();
        // If we're doing partial evaluation, start where we left off
        for (int i = isPartiallyEvaluating() ? evalIndex : 0; i < size; i+=2) {
            // Short circuit if we see our stop value
            if (i+1 == size) {
                return i;
            }
            // If we get null, we have to re-evaluate from that point (special case this in filter, like is null)
            // We may only run this when we're done/have all values
            boolean evaluated = children.get(i+1).evaluate(tuple, ptr);
            if (evaluated && Boolean.TRUE.equals(PDataType.BOOLEAN.toObject(ptr))) {
                if (isPartiallyEvaluating()) {
                    foundIndex = true;
                }
                return i;
            }
            if (isPartiallyEvaluating()) {
                if (evaluated || tuple.isImmutable()) {
                    evalIndex+=2;
                } else {
                    /*
                     * Return early here if incrementally evaluating and we don't
                     * have all the key values yet. We can't continue because we'd
                     * potentially be bypassing cases which we could later evaluate
                     * once we have more column values.
                     */
                    return -1;
                }
            }
        }
        // No conditions matched, return size to indicate that we were able
        // to evaluate all cases, but didn't find any matches.
        return size;
    }
    
    /**
     * Only expression that currently uses the isPartial flag. The IS NULL
     * expression will use it too. TODO: We could alternatively have a non interface
     * method, like setIsPartial in which we set to false prior to calling
     * evaluate.
     */
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        int index = evaluateIndexOf(tuple, ptr);
        if (index < 0) {
            return false;
        } else if (index == children.size()) {
            ptr.set(PDataType.NULL_BYTES);
            return true;
        }
        if (children.get(index).evaluate(tuple, ptr)) {
            return true;
        }
        return false;
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
    public String toString() {
        StringBuilder buf = new StringBuilder("CASE ");
        for (int i = 0; i < children.size() - 1; i+=2) {
            buf.append("WHEN ");
            buf.append(children.get(i+1));
            buf.append(" THEN ");
            buf.append(children.get(i));
        }
        if (hasElse()) {
            buf.append(" ELSE " + children.get(children.size()-1));
        }
        buf.append(" END");
        return buf.toString();
    }
    
}
