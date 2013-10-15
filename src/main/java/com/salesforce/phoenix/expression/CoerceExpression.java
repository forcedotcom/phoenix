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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.expression.visitor.ExpressionVisitor;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;


public class CoerceExpression extends BaseSingleExpression {
    private PDataType toType;
    private ColumnModifier toMod;
    private Integer byteSize;
    
    public CoerceExpression() {
    }

    public static Expression create(Expression expression, PDataType toType) throws SQLException {
        return toType == expression.getDataType() ? expression : expression instanceof LiteralExpression ? LiteralExpression.newConstant(((LiteralExpression)expression).getValue(), toType) : new CoerceExpression(expression, toType);
    }
    
    //Package protected for tests
    CoerceExpression(Expression expression, PDataType toType) {
        this(expression, toType, null, null);
    }
    
    CoerceExpression(Expression expression, PDataType toType, ColumnModifier toMod, Integer byteSize) {
        super(expression);
        this.toType = toType;
        this.toMod = toMod;
        this.byteSize = byteSize;
    }

    @Override
    public Integer getByteSize() {
        return byteSize;
    }
    
    @Override
    public Integer getMaxLength() {
        return byteSize;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((byteSize == null) ? 0 : byteSize.hashCode());
        result = prime * result + ((toMod == null) ? 0 : toMod.hashCode());
        result = prime * result + ((toType == null) ? 0 : toType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        CoerceExpression other = (CoerceExpression)obj;
        if (byteSize == null) {
            if (other.byteSize != null) return false;
        } else if (!byteSize.equals(other.byteSize)) return false;
        if (toMod != other.toMod) return false;
        if (toType != other.toType) return false;
        return true;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        toType = PDataType.values()[WritableUtils.readVInt(input)];
        toMod = ColumnModifier.fromSystemValue(WritableUtils.readVInt(input));
        int byteSize = WritableUtils.readVInt(input);
        this.byteSize = byteSize == -1 ? null : byteSize;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeVInt(output, toType.ordinal());
        WritableUtils.writeVInt(output, ColumnModifier.toSystemValue(toMod));
        WritableUtils.writeVInt(output, byteSize == null ? -1 : byteSize);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (getChild().evaluate(tuple, ptr)) {
            getDataType().coerceBytes(ptr, getChild().getDataType(), getChild().getColumnModifier(), getColumnModifier());
            return true;
        }
        return false;
    }

    @Override
    public PDataType getDataType() {
        return toType;
    }
    
    @Override
    public ColumnModifier getColumnModifier() {
            return toMod;
    }    

    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        return getChild().accept(visitor);
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("TO_" + toType.toString() + "(");
        for (int i = 0; i < children.size() - 1; i++) {
            buf.append(children.get(i) + ", ");
        }
        buf.append(children.get(children.size()-1) + ")");
        return buf.toString();
    }
}
