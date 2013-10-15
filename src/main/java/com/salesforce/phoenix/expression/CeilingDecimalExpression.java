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

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.visitor.ExpressionVisitor;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;


public class CeilingDecimalExpression extends BaseSingleExpression {
    private static final MathContext CEILING_CONTEXT = new MathContext(0, RoundingMode.CEILING);
    
    public CeilingDecimalExpression() {
    }
    
    public CeilingDecimalExpression(Expression child)  {
        super(child);
    }
    
    protected MathContext getMathContext() {
        return CEILING_CONTEXT;
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression child =  getChild();
        if (child.evaluate(tuple, ptr)) {
            PDataType childType = child.getDataType();
            childType.coerceBytes(ptr, childType, child.getColumnModifier(), null);
            BigDecimal value = (BigDecimal) childType.toObject(ptr);
            value = value.round(getMathContext());
            byte[] b = childType.toBytes(value, child.getColumnModifier());
            ptr.set(b);
            return true;
        }
        return false;
    }

    @Override
    public ColumnModifier getColumnModifier() {
            return getChild().getColumnModifier();
    }    

    @Override
    public final PDataType getDataType() {
        return  getChild().getDataType();
    }
    
    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        return getChild().accept(visitor);
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("CEIL(");
        for (int i = 0; i < children.size() - 1; i++) {
            buf.append(getChild().toString());
        }
        buf.append(")");
        return buf.toString();
    }
}
