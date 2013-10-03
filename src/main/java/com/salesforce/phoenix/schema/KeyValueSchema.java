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
package com.salesforce.phoenix.schema;

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.http.annotation.Immutable;

import com.salesforce.phoenix.expression.aggregator.Aggregator;
import com.salesforce.phoenix.util.ByteUtil;


/**
 * 
 * Simple flat schema over a byte array where fields may be any of {@link PDataType}.
 * Optimized for positional access by index.
 *
 * @author jtaylor
 * @since 0.1
 */
@Immutable
public class KeyValueSchema extends ValueSchema {
    
    protected KeyValueSchema(int minNullable, List<Field> fields) {
        super(minNullable, fields);
    }

    public static class KeyValueSchemaBuilder extends ValueSchemaBuilder {

        public KeyValueSchemaBuilder(int minNullable) {
            super(minNullable);
        }
        
        @Override
        public KeyValueSchema build() {
            List<Field> condensedFields = buildFields();
            return new KeyValueSchema(this.minNullable, condensedFields);
        }

        @Override
        public KeyValueSchemaBuilder setMaxFields(int nFields) {
            super.setMaxFields(nFields);
            return this;
        }
        
        public KeyValueSchemaBuilder addField(PDatum datum) {
            super.addField(datum, fields.size() <  this.minNullable, null);
            return this;
        }
    }
    
    public boolean isNull(int position, ValueBitSet bitSet) {
        int nBit = position - getMinNullable();
        return (nBit >= 0 && !bitSet.get(nBit));
    }
    
    private static byte[] ensureSize(byte[] b, int offset, int size) {
        if (size > b.length) {
            byte[] bBigger = new byte[Math.max(b.length * 2, size)];
            System.arraycopy(b, 0, bBigger, 0, b.length);
            return bBigger;
        }
        return b;
    }

    /**
     * @return byte representation of the KeyValueSchema
     */
    public byte[] toBytes(Aggregator[] aggregators, ValueBitSet valueSet, ImmutableBytesWritable ptr) {
        int offset = 0;
        int index = 0;
        valueSet.clear();
        int minNullableIndex = getMinNullable();
        byte[] b = new byte[getEstimatedValueLength() + valueSet.getEstimatedLength()];
        List<Field> fields = getFields();
        // We can get away with checking if only nulls are left in the outer loop,
        // since repeating fields will not span the non-null/null boundary.
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            PDataType type = field.getDataType();
            for (int j = 0; j < field.getCount(); j++) {
                if (aggregators[index].evaluate(null, ptr)) { // Skip null values
                    if (index >= minNullableIndex) {
                        valueSet.set(index - minNullableIndex);
                    }
                    if (!type.isFixedWidth()) {
                        b = ensureSize(b, offset, offset + getVarLengthBytes(ptr.getLength()));
                        offset = writeVarLengthField(ptr, b, offset);
                    } else {
                        int nBytes = ptr.getLength();
                        b = ensureSize(b, offset, offset + nBytes);
                        System.arraycopy(ptr.get(), ptr.getOffset(), b, offset, nBytes);
                        offset += nBytes;
                    }
                }
                index++;
            }
        }
        // Add information about which values were set at end of value,
        // so that we can quickly access them without needing to walk
        // through the values using the schema.
        // TODO: if there aren't any non null values, don't serialize anything
        b = ensureSize(b, offset, offset + valueSet.getEstimatedLength());
        offset = valueSet.toBytes(b, offset);

        if (offset == b.length) {
            return b;
        } else {
            byte[] bExact = new byte[offset];
            System.arraycopy(b, 0, bExact, 0, offset);
            return bExact;
        }
    }

    private int getVarLengthBytes(int length) {
        return length + WritableUtils.getVIntSize(length);
    }
    
    private int writeVarLengthField(ImmutableBytesWritable ptr, byte[] b, int offset) {
        int length = ptr.getLength();
        offset += ByteUtil.vintToBytes(b, offset, length);
        System.arraycopy(ptr.get(), ptr.getOffset(), b, offset, length);                        
        offset += length;
        return offset;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="NP_BOOLEAN_RETURN_NULL", 
            justification="Designed to return null.")
    public Boolean iterator(byte[] src, int srcOffset, int srcLength, ImmutableBytesWritable ptr, int position, ValueBitSet valueBitSet) {
        ptr.set(src, srcOffset, 0);
        int maxOffset = srcOffset + srcLength;
        Boolean hasValue = null;
        for (int i = 0; i < position; i++) {
            hasValue = next(ptr, i, maxOffset, valueBitSet);
        }
        return hasValue;
    }
    
    public Boolean iterator(ImmutableBytesWritable srcPtr, ImmutableBytesWritable ptr, int position, ValueBitSet valueSet) {
        return iterator(srcPtr.get(),srcPtr.getOffset(),srcPtr.getLength(), ptr, position, valueSet);
    }
    
    public Boolean iterator(ImmutableBytesWritable ptr, int position, ValueBitSet valueSet) {
        return iterator(ptr, ptr, position, valueSet);
    }
    
    public Boolean iterator(ImmutableBytesWritable ptr) {
        return iterator(ptr, ptr, 0, ValueBitSet.EMPTY_VALUE_BITSET);
    }
    
    /**
     * Move the bytes ptr to the next position relative to the current ptr
     * @param ptr bytes pointer pointing to the value at the positional index
     * provided.
     * @param position zero-based index of the next field in the value schema
     * @param maxOffset max possible offset value when iterating
     * @return true if a value was found and ptr was set, false if the value is null and ptr was not
     * set, and null if the value is null and there are no more values
      */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="NP_BOOLEAN_RETURN_NULL", 
            justification="Designed to return null.")
    public Boolean next(ImmutableBytesWritable ptr, int position, int maxOffset, ValueBitSet valueSet) {
        if (ptr.getOffset() + ptr.getLength() >= maxOffset) {
            ptr.set(ptr.get(), maxOffset, 0);
            return null;
        }
        if (position >= getFieldCount()) {
            return null;
        }
        // Move the pointer past the current value and set length
        // to 0 to ensure you never set the ptr past the end of the
        // backing byte array.
        ptr.set(ptr.get(), ptr.getOffset() + ptr.getLength(), 0);
        if (!isNull(position, valueSet)) {
            Field field = this.getField(position);
            if (field.getDataType().isFixedWidth()) {
                ptr.set(ptr.get(),ptr.getOffset(), field.getByteSize());
            } else {
                int length = ByteUtil.vintFromBytes(ptr);
                ptr.set(ptr.get(),ptr.getOffset(),length);
            }
            return ptr.getLength() > 0;
        }
        return false;
    }
}
