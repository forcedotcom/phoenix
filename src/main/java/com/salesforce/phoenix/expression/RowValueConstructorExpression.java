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

/**
 * Implementation for row value constructor (a,b,c) expression.
 * 
 * @author samarth.jain
 * @since 0.1
 */
package com.salesforce.phoenix.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.visitor.ExpressionVisitor;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.IndexUtil;
import com.salesforce.phoenix.util.TrustedByteArrayOutputStream;

public class RowValueConstructorExpression extends BaseCompoundExpression {
    
    private ImmutableBytesWritable ptrs[];
    private ImmutableBytesWritable literalExprPtr;
    private int counter;
    private int size;
    
    
    public RowValueConstructorExpression() {}
    
    public RowValueConstructorExpression(List<Expression> l, boolean isConstant) {
        super(l);
        children = l;
        counter = 0;
        size = 0;
        init(isConstant);
    }

    public int getEstimatedSize() {
        return size;
    }
    
    public boolean isConstant() {
        return literalExprPtr != null;
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
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        init(input.readBoolean());
    }
    
    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        output.writeBoolean(literalExprPtr != null);
    }
    
    private void init(boolean isConstant) {
        ptrs = new ImmutableBytesWritable[children.size()];
        if(isConstant) {
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            this.evaluate(null, ptr);
            literalExprPtr = ptr;
        }
    }
    
    @Override
    public PDataType getDataType() {
        return PDataType.VARBINARY;
    }
    
    @Override
    public void reset() {
        counter = 0;
        size = 0;
        Arrays.fill(ptrs, null);
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if(literalExprPtr != null) {
            // if determined during construction that the row value constructor is just comprised of literal expressions, 
            // let's just return the ptr we have already computed and be done with evaluation.
            ptr.set(literalExprPtr.get(), literalExprPtr.getOffset(), literalExprPtr.getLength());
            return true;
        }
        try {
            int j;
            final int numChildExpressions = children.size();
            for(j = counter; j < numChildExpressions; j++) {
                final Expression expression = children.get(j);
                if(expression != null && expression.evaluate(tuple, ptr)) {
                    if(expression.getDataType() != null) {
                        PDataType dt = IndexUtil.getIndexColumnDataType(true , expression.getDataType());
                        dt.coerceBytes(ptr, expression.getDataType(), expression.getColumnModifier(), expression.getColumnModifier());
                        ptrs[j] = new ImmutableBytesWritable();
                        ptrs[j].set(ptr.get(), ptr.getOffset(), ptr.getLength());
                        size = size + (dt.isFixedWidth() ? ptr.getLength() : ptr.getLength() + 1); // 1 extra for the separator byte.
                        counter++;
                    }
                } else if(tuple == null || tuple.isImmutable()) {
                    ptrs[j] = new ImmutableBytesWritable();
                    ptrs[j].set(ByteUtil.EMPTY_BYTE_ARRAY);
                    counter++;
                } else {
                    return false;
                }
            }
            
            if(j == numChildExpressions) {
                TrustedByteArrayOutputStream output = new TrustedByteArrayOutputStream(size);
                try {
                    for(int i = 0; i< numChildExpressions; i++) {
                        ImmutableBytesWritable tempPtr = ptrs[i];
                        if(tempPtr != null) {
                            if(i > 0) {
                                final Expression expression = children.get(i);
                                if (!IndexUtil.getIndexColumnDataType(true , expression.getDataType()).isFixedWidth()) {
                                    output.write(QueryConstants.SEPARATOR_BYTE);
                                }
                            }
                            output.write(tempPtr.get(), tempPtr.getOffset(), tempPtr.getLength());
                        }
                    }
                    int outputSize = output.size();
                    byte[] outputBytes = output.getBuffer();
                    int numSeparatorByte = 0;
                    for(int k = outputSize -1 ; k >=0 ; k--) {
                        if(outputBytes[k] == QueryConstants.SEPARATOR_BYTE) {
                            numSeparatorByte++;
                        } else {
                            break;
                        }    
                    }
                    ptr.set(outputBytes, 0, outputSize - numSeparatorByte);
                    return true;
                } finally {
                    output.close();
                }
            }  
            return false;
        } catch (IOException e) {
            throw new RuntimeException(e); //Impossible.
        }
    }
}
