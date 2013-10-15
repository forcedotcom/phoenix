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
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.expression.visitor.ExpressionVisitor;
import com.salesforce.phoenix.schema.ConstraintViolationException;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ByteUtil;

/*
 * Implementation of a SQL foo IN (a,b,c) expression. Other than the first
 * expression, child expressions must be constants.
 *
 */
public class InListExpression extends BaseSingleExpression {
    private LinkedHashSet<ImmutableBytesPtr> values;
    private ImmutableBytesPtr minValue;
    private ImmutableBytesPtr maxValue;
    private int valuesByteLength;
    private boolean containsNull;
    private int fixedWidth = -1;
    private List<Expression> keys;
    private ImmutableBytesPtr value = new ImmutableBytesPtr();

    public static Expression create (List<Expression> children, ImmutableBytesWritable ptr) throws SQLException {
        Expression firstChild = children.get(0);
        PDataType firstChildType = firstChild.getDataType();

        boolean addedNull = false;
        List<Expression> inChildren = Lists.newArrayListWithExpectedSize(children.size());
        inChildren.add(firstChild);
        for (int i = 1; i < children.size(); i++) {
            Expression rhs = children.get(i);
            if (rhs.evaluate(null, ptr)) {
                if (ptr.getLength() == 0) {
                    if (!addedNull) {
                        addedNull = true;
                        inChildren.add(LiteralExpression.newConstant(null, PDataType.VARBINARY));
                    }
                } else {
                    // Don't specify the firstChild column modifier here, as we specify it in the LiteralExpression creation below
                    try {
                        firstChildType.coerceBytes(ptr, rhs.getDataType(), rhs.getColumnModifier(), null);
                        rhs = LiteralExpression.newConstant(ByteUtil.copyKeyBytesIfNecessary(ptr), PDataType.VARBINARY, firstChild.getColumnModifier());
                        inChildren.add(rhs);
                    } catch (ConstraintViolationException e) { // Ignore and continue
                    }
                }
            }
        }
        if (inChildren.size() == 1) {
            return LiteralExpression.FALSE_EXPRESSION;
        }
        if (inChildren.size() == 2 && addedNull) {
            return LiteralExpression.newConstant(null, PDataType.BOOLEAN);
        }
        Expression expression;
        // TODO: if inChildren.isEmpty() then Oracle throws a type mismatch exception. This means
        // that none of the list elements match in type and there's no null element. We'd return
        // false in this case. Should we throw?
        if (inChildren.size() == 2) {
            expression = new ComparisonExpression(CompareOp.EQUAL, inChildren);
        } else {
            expression = new InListExpression(inChildren);
        }
        return expression;
    }
    
    public InListExpression() {
    }

    private InListExpression(List<Expression> children) throws SQLException {
        super(children.get(0));
        this.keys = Lists.newArrayListWithExpectedSize(children.size()-1);
        Set<ImmutableBytesPtr> values = Sets.newHashSetWithExpectedSize(children.size()-1);
        int fixedWidth = -1;
        boolean isFixedLength = true;
        for (int i = 1; i < children.size(); i++) {
            ImmutableBytesPtr ptr = new ImmutableBytesPtr();
            Expression child = children.get(i);
            assert(child.getDataType() == PDataType.VARBINARY);
            child.evaluate(null, ptr);
            if (ptr.getLength() == 0) {
                containsNull = true;
            } else {
                if (values.add(ptr)) {
                    int length = ptr.getLength();
                    if (fixedWidth == -1) {
                        fixedWidth = length;
                    } else {
                        isFixedLength &= fixedWidth == length;
                    }
                    
                    valuesByteLength += ptr.getLength();
                    keys.add(child);
                }
            }
        }
        this.fixedWidth = isFixedLength ? fixedWidth : -1;
        // Sort values by byte value so we can get min/max easily
        ImmutableBytesPtr[] valuesArray = values.toArray(new ImmutableBytesPtr[values.size()]);
        Arrays.sort(valuesArray, ByteUtil.BYTES_PTR_COMPARATOR);
        this.minValue = valuesArray[0];
        this.maxValue = valuesArray[valuesArray.length-1];
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
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
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

    private int readValue(DataInput input, byte[] valuesBytes, int offset, ImmutableBytesPtr ptr) throws IOException {
        int valueLen = fixedWidth == -1 ? WritableUtils.readVInt(input) : fixedWidth;
        values.add(new ImmutableBytesPtr(valuesBytes,offset,valueLen));
        return offset + valueLen;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        containsNull = input.readBoolean();
        fixedWidth = WritableUtils.readVInt(input);
        byte[] valuesBytes = Bytes.readByteArray(input);
        valuesByteLength = valuesBytes.length;
        int len = fixedWidth == -1 ? WritableUtils.readVInt(input) : valuesByteLength / fixedWidth;
        values = Sets.newLinkedHashSetWithExpectedSize(len);
        int offset = 0;
        int i  = 0;
        if (i < len) {
            offset = readValue(input, valuesBytes, offset, minValue = new ImmutableBytesPtr());
            while (++i < len-1) {
                offset = readValue(input, valuesBytes, offset, new ImmutableBytesPtr());
            }
            if (i < len) {
                offset = readValue(input, valuesBytes, offset, maxValue = new ImmutableBytesPtr());
            } else {
                maxValue = minValue;
            }
        } else {
            minValue = maxValue = new ImmutableBytesPtr(ByteUtil.EMPTY_BYTE_ARRAY);
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        output.writeBoolean(containsNull);
        WritableUtils.writeVInt(output, fixedWidth);
        WritableUtils.writeVInt(output, valuesByteLength);
        for (ImmutableBytesPtr ptr : values) {
            output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
        }
        if (fixedWidth == -1) {
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
    public List<Expression> getKeys() {
        return keys;
    }

    public ImmutableBytesWritable getMinKey() {
        return minValue;
    }

    public ImmutableBytesWritable getMaxKey() {
        return maxValue;
    }

    @Override
    public String toString() {
        int maxToStringLen = 200;
        Expression firstChild = children.get(0);
        PDataType type = firstChild.getDataType();
        StringBuilder buf = new StringBuilder(firstChild + " IN (");
        if (containsNull) {
            buf.append("null,");
        }
        for (ImmutableBytesPtr value : values) {
            if (firstChild.getColumnModifier() != null) {
                type.coerceBytes(value, type, firstChild.getColumnModifier(), null);
            }
            buf.append(type.toStringLiteral(value, null));
            buf.append(',');
            if (buf.length() >= maxToStringLen) {
                buf.append("... ");
                break;
            }
        }
        buf.setCharAt(buf.length()-1,')');
        return buf.toString();
    }
}
