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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.expression.visitor.ExpressionVisitor;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ByteUtil;



/**
 * 
 * Accessor for a literal value.
 *
 * @author jtaylor
 * @since 0.1
 */
public class LiteralExpression extends BaseTerminalExpression {
    public static final LiteralExpression NULL_EXPRESSION = new LiteralExpression(null);
    private static final LiteralExpression[] TYPED_NULL_EXPRESSIONS = new LiteralExpression[PDataType.values().length];
    static {
        for (int i = 0; i < TYPED_NULL_EXPRESSIONS.length; i++) {
            TYPED_NULL_EXPRESSIONS[i] = new LiteralExpression(PDataType.values()[i]);
        }
    }
    public static final LiteralExpression FALSE_EXPRESSION = new LiteralExpression(Boolean.FALSE);
    public static final LiteralExpression TRUE_EXPRESSION = new LiteralExpression(Boolean.TRUE);

    private Object value;
    private PDataType type;
    private byte[] byteValue;
    private Integer maxLength;
                
    // TODO: cache?
    public static LiteralExpression newConstant(Object value) {
        if (Boolean.FALSE.equals(value)) {
            return FALSE_EXPRESSION;
        }
        if (Boolean.TRUE.equals(value)) {
            return TRUE_EXPRESSION;
        }
        if (value == null) {
            return NULL_EXPRESSION;
        }
        PDataType type = PDataType.fromLiteral(value);
        byte[] b = type.toBytes(value);
        if (b.length == 0) {
            return TYPED_NULL_EXPRESSIONS[type.ordinal()];
        }
        if (type == PDataType.VARCHAR) {
            String s = (String) value;
            if (s.length() == b.length) { // single byte characters only
                type = PDataType.CHAR;
            }
        }
        return new LiteralExpression(value, type, b);
    }
    
    // TODO: cache?
    public static LiteralExpression newConstant(Object value, PDataType type) throws SQLException {
        if (value == null) {
            if (type == null) {
                return NULL_EXPRESSION;
            }
            return TYPED_NULL_EXPRESSIONS[type.ordinal()];
        }
        PDataType actualType = PDataType.fromLiteral(value);
        if (!actualType.isCoercibleTo(type, value)) {
            throw new TypeMismatchException(type, actualType, value.toString());
        }
        value = type.toObject(value, actualType);
        try {
            byte[] b = type.toBytes(value);
            if (b.length == 0) {
                return TYPED_NULL_EXPRESSIONS[type.ordinal()];
            }
            return new LiteralExpression(value, type, b);
        } catch (IllegalDataException e) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.ILLEGAL_DATA).setRootCause(e).build().buildException();
        }
    }
    
    public LiteralExpression() {
    }

    protected LiteralExpression(Object value) {
        this.value = value;
        this.type = PDataType.fromLiteral(value);
        if (type == null) {
            this.byteValue = PDataType.NULL_BYTES;
        } else {
            this.byteValue = this.type.toBytes(this.value);
        }
        this.maxLength = byteValue.length;
    }
    
    private LiteralExpression(Object value, PDataType type, byte[] byteValue) {
        this.value = value;
        this.type = type;
        this.byteValue = byteValue;
        this.maxLength = byteValue.length;
    }
    
    @Override
    public String toString() {
        return type != null && type.isCoercibleTo(PDataType.VARCHAR) ? "'" + value + "'" : "" + value;
    }

    private LiteralExpression(PDataType type) {
        this.type = type;
        this.value = null;
        this.byteValue = ByteUtil.EMPTY_BYTE_ARRAY;
        this.maxLength = 0;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        LiteralExpression other = (LiteralExpression)obj;
        if (value == null) {
            if (other.value != null) return false;
        } else if (!value.equals(other.value)) return false;
        return true;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.byteValue = Bytes.readByteArray(input);
        if (this.byteValue.length > 0) {
            this.type = PDataType.values()[WritableUtils.readVInt(input)];
            this.value = this.type.toObject(byteValue);
        }
        maxLength = this.byteValue.length;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        Bytes.writeByteArray(output, byteValue);
        if (this.byteValue.length > 0) {
            WritableUtils.writeVInt(output, this.type.ordinal());
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        // Literal always evaluates, even when it returns null
        ptr.set(byteValue);
        return true;
    }

    @Override
    public PDataType getDataType() {
        return type;
    }

    @Override
    public Integer getMaxLength() {
        return maxLength;
    }

    @Override
    public boolean isNullable() {
        return value == null;
    }

    public Object getValue() {
        return value;
    }
    
    public byte[] getBytes() {
        return byteValue;
    }
    
    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
