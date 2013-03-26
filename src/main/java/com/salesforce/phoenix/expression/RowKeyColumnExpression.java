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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.visitor.ExpressionVisitor;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PDatum;
import com.salesforce.phoenix.schema.RowKeyValueAccessor;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Class to access a value stored in the row key
 *
 * @author jtaylor
 * @since 0.1
 */
public class RowKeyColumnExpression  extends ColumnExpression {
    private PDataType fromType;
    private RowKeyValueAccessor accessor;
    private ColumnModifier columnModifier = null;
    
    private final String name;
    
    public RowKeyColumnExpression() {
        name = null; // Only on client
    }
    
    public RowKeyColumnExpression(PDatum datum, RowKeyValueAccessor accessor) {
        this(datum, accessor, datum.getDataType());
    }
    
    public RowKeyColumnExpression(PColumn col, RowKeyValueAccessor accessor) {
        this((PDatum)col, accessor);
        this.columnModifier = col.getColumnModifier();
    }    
    
    public RowKeyColumnExpression(PDatum datum, RowKeyValueAccessor accessor, PDataType fromType) {
        super(datum);
        this.accessor = accessor;
        this.fromType = fromType;
        this.name = datum.toString();
    }
    
    public int getPosition() {
        return accessor.getIndex();
    }
    
    public ColumnModifier getSortOrder() {
    	return columnModifier;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((accessor == null) ? 0 : accessor.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return name == null ? "PK[" + accessor.getIndex() + "]" : name;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!super.equals(obj)) return false;
        if (getClass() != obj.getClass()) return false;
        RowKeyColumnExpression other = (RowKeyColumnExpression)obj;
        return accessor.equals(other.accessor);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        tuple.getKey(ptr);
        int offset = accessor.getOffset(ptr.get(), ptr.getOffset());
        // Null is represented in the last expression of a multi-part key 
        // by the bytes not being present.
        if (offset < ptr.getOffset() + ptr.getLength()) {
            byte[] buffer = ptr.get();
            int maxByteSize = ptr.getLength() - (offset - ptr.getOffset());
            int fixedByteSize = -1;
            // FIXME: fixedByteSize <= maxByteSize ? fixedByteSize : 0 required because HBase passes bogus keys to filter to position scan (HBASE-6562)
            if (fromType.isFixedWidth()) {
                fixedByteSize = getByteSize();
                fixedByteSize = fixedByteSize <= maxByteSize ? fixedByteSize : 0;
            }
            int length = fixedByteSize >= 0 ? fixedByteSize  : accessor.getLength(buffer, offset, maxByteSize);
            // In the middle of the key, an empty variable length byte array represents null
            if (length > 0) {
                if (type == fromType) {
                    ptr.set(buffer,offset,length);
                } else {
                    ptr.set(type.toBytes(type.toObject(buffer, offset, length, fromType)));
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        accessor = new RowKeyValueAccessor();
        accessor.readFields(input);
        fromType = type; // fromType only needed on client side
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        accessor.write(output);
    }
    
    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
