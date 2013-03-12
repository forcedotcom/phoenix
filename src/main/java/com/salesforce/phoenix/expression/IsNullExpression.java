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
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.visitor.ExpressionVisitor;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Implementation of IS NULL and IS NOT NULL expression
 *
 * @author jtaylor
 * @since 0.1
 */
public class IsNullExpression extends BaseSingleExpression {
    private boolean isNegate;

    public IsNullExpression() {
    }
    
    public IsNullExpression(Expression expression, boolean negate) {
        super(expression);
        this.isNegate = negate;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) throws SQLException  {
        boolean evaluated = getChild().evaluate(tuple, ptr);
        if (evaluated) {
            ptr.set(isNegate ^ ptr.getLength() == 0 ? PDataType.TRUE_BYTES : PDataType.FALSE_BYTES);
            return true;
        }
        if (tuple.isImmutable()) {
            ptr.set(isNegate ? PDataType.FALSE_BYTES : PDataType.TRUE_BYTES);
            return true;
        }
        
        return false;
    }

    public boolean isNegate() {
        return isNegate;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        isNegate = input.readBoolean();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        output.writeBoolean(isNegate);
    }

    @Override
    public PDataType getDataType() {
        return PDataType.BOOLEAN;
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
        StringBuilder buf = new StringBuilder(children.get(0).toString());
        if (isNegate) {
            buf.append(" IS NOT NULL");
        } else {
            buf.append(" IS NULL");
        }
        return buf.toString();
    }
}
