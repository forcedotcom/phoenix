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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.expression.visitor.ExpressionVisitor;
import com.salesforce.phoenix.schema.*;
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
    private byte[] cfPrefix;
    
    private final String name;
    
    public RowKeyColumnExpression() {
        name = null; // Only on client
        cfPrefix = null;
    }
    
    public RowKeyColumnExpression(PDatum datum, RowKeyValueAccessor accessor) {
        this(datum, accessor, datum.getDataType());
    }
    
    public RowKeyColumnExpression(PDatum datum, RowKeyValueAccessor accessor, byte[] cfPrefix) {
        this(datum, accessor, datum.getDataType(), cfPrefix);
    }
    
    public RowKeyColumnExpression(PDatum datum, RowKeyValueAccessor accessor, PDataType fromType) {
        this(datum, accessor, fromType, null);
    }
    
    public RowKeyColumnExpression(PDatum datum, RowKeyValueAccessor accessor, PDataType fromType, byte[] cfPrefix) {
        super(datum);
        this.accessor = accessor;
        this.fromType = fromType;
        this.name = datum.toString();
        this.cfPrefix = cfPrefix;
    }
    
    public int getPosition() {
        return accessor.getIndex();
    }
    
    public byte[] getCFPrefix() {
        return cfPrefix;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((accessor == null) ? 0 : accessor.hashCode());
        result = prime * result + ((cfPrefix == null) ? 0 : Bytes.hashCode(cfPrefix));
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
        return accessor.equals(other.accessor) 
            && ((cfPrefix == null && other.cfPrefix == null) 
                    || (cfPrefix != null && other.cfPrefix != null && Bytes.equals(cfPrefix, other.cfPrefix)));
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        boolean success = getKey(tuple, ptr);
        if (success == false)
            return false;
        
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
    
    protected boolean getKey(Tuple tuple, ImmutableBytesWritable ptr) {
        if (cfPrefix == null) {
            tuple.getKey(ptr);
            return true;
        }
        
        return tuple.getKey(ptr, cfPrefix);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        accessor = new RowKeyValueAccessor();
        accessor.readFields(input);
        fromType = type; // fromType only needed on client side
        byte[] prefix = Bytes.readByteArray(input);
        cfPrefix = prefix.length == 0 ? null : prefix;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        accessor.write(output);
        if (cfPrefix == null) {
            Bytes.writeByteArray(output, new byte[0]);
        } else {
            Bytes.writeByteArray(output, cfPrefix);
        }
    }
    
    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
