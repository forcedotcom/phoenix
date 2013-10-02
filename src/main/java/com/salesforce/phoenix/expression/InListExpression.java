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
import java.util.*;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.Sets;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.expression.visitor.ExpressionVisitor;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ByteUtil;

/*
 * Implementation of a SQL foo IN (a,b,c) expression. Other than the first
 * expression, child expressions must be constants.
 *
 * TODO: optimize this for row key columns by having multiple scans move
 * from key to key using a custom filter with a hint
 */
public class InListExpression extends BaseSingleExpression {
    private Set<ImmutableBytesPtr> values;
    private int valuesByteLength;
    private boolean containsNull;
    private ImmutableBytesPtr value = new ImmutableBytesPtr();

    public InListExpression() {
    }

    public InListExpression(List<Expression> children) throws SQLException {
        super(children.get(0));
        PDataType type = getChild().getDataType();
        Set<ImmutableBytesPtr> values = Sets.newHashSetWithExpectedSize(children.size()-1);
        for (int i = 1; i < children.size(); i++) {
            LiteralExpression child = (LiteralExpression)children.get(i);
            PDataType childType = child.getDataType();
            if (childType != type) {
                throw new IllegalStateException("Type mismatch: expected " + type + " but got " + child.getDataType() + " for " + this);
            }
            ImmutableBytesPtr ptr = new ImmutableBytesPtr();
            child.evaluate(null, ptr);
            if (ptr.getLength() == 0) {
                containsNull = true;
            } else {
                if (values.add(ptr)) {
                    valuesByteLength += ptr.getLength();
                }
            }
        }
        // Sort values by byte value
        ImmutableBytesPtr[] valuesArray = values.toArray(new ImmutableBytesPtr[values.size()]);
        Arrays.sort(valuesArray, ByteUtil.BYTES_PTR_COMPARATOR);
        this.values = new LinkedHashSet<ImmutableBytesPtr>(Arrays.asList(valuesArray));
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getChild().evaluate(tuple, ptr)) {
            return false;
        }
        value.set(ptr);
        if (values.contains(value)) {
            ptr.set(PDataType.TRUE_BYTES);
            return true;
        }
        if (containsNull) { // If any null value and value not found
            return false;
        }
        ptr.set(PDataType.FALSE_BYTES);
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (containsNull ? 1231 : 1237);
        result = prime * result + values.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        InListExpression other = (InListExpression)obj;
        if (containsNull != other.containsNull) return false;
        if (!values.equals(other.values)) return false;
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDataType.BOOLEAN;
    }

    @Override
    public boolean isNullable() {
        return super.isNullable() || containsNull;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        boolean fixedWidth = getChild().getDataType().isFixedWidth();
        containsNull = input.readBoolean();
        byte[] valuesBytes = Bytes.readByteArray(input);
        valuesByteLength = valuesBytes.length;
        int len = fixedWidth ? valuesByteLength / getChild().getByteSize() : WritableUtils.readVInt(input);
        values = Sets.newLinkedHashSetWithExpectedSize(len);
        int offset = 0;
        for (int i = 0; i < len; i++) {
            int valueLen = fixedWidth ? getChild().getByteSize() : WritableUtils.readVInt(input);
            values.add(new ImmutableBytesPtr(valuesBytes,offset,valueLen));
            offset += valueLen;
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        output.writeBoolean(containsNull);
        WritableUtils.writeVInt(output, valuesByteLength);
        for (ImmutableBytesPtr ptr : values) {
            output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
        }
        if (!getChild().getDataType().isFixedWidth()) {
            WritableUtils.writeVInt(output, values.size());
            for (ImmutableBytesPtr ptr : values) {
                WritableUtils.writeVInt(output, ptr.getLength());
            }
        }
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

    /**
     * Gets the list of values in the IN expression, in
     * sorted order.
     * @return the list of values in the IN expression
     */
    public List<byte[]> getKeys() {
      List<byte[]> keys = new ArrayList<byte[]>(values.size());
      for (ImmutableBytesPtr value : values) {
        keys.add(value.getOffset() == 0 && value.getLength() == value.get().length ? value.get() : value.copyBytes());
      }
      return keys;
    }

    public ImmutableBytesWritable getMinKey() {
        ImmutableBytesWritable minKey = null;
        for (ImmutableBytesPtr value : values) {
            if (minKey == null || Bytes.compareTo(value.get(), value.getOffset(), value.getLength(), minKey.get(), minKey.getOffset(), minKey.getLength()) < 0)  {
                minKey = value;
            }
        }
        return minKey;
    }

    public ImmutableBytesWritable getMaxKey() {
        ImmutableBytesWritable maxKey = null;
        for (ImmutableBytesPtr value : values) {
            if (maxKey == null || Bytes.compareTo(value.get(), value.getOffset(), value.getLength(), maxKey.get(), maxKey.getOffset(), maxKey.getLength()) > 0)  {
                maxKey = value;
            }
        }
        return maxKey;
    }

    @Override
    public String toString() {
        Expression firstChild = children.get(0);
        PDataType type = firstChild.getDataType();
        boolean isString = type.isCoercibleTo(PDataType.VARCHAR);
        StringBuilder buf = new StringBuilder(firstChild + " IN (");
        if (containsNull) {
            buf.append("null,");
        }
        for (ImmutableBytesPtr value : values) {
            if (isString) buf.append('\'');
            buf.append(type.toObject(value));
            if (isString) buf.append('\'');
            buf.append(',');
        }
        buf.setCharAt(buf.length()-1,')');
        return buf.toString();
    }
}
