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

import java.io.*;
import java.util.*;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.http.annotation.Immutable;


import com.google.common.collect.ImmutableList;
import com.salesforce.phoenix.expression.aggregator.Aggregator;

/**
 * 
 * Simple flat schema over a byte array where fields may be any of {@link PDataType}.
 * Optimized for positional access by index.
 *
 * @author jtaylor
 * @since 0.1
 */
@Immutable
public abstract class ValueSchema {
    private static final int ESTIMATED_VARIABLE_LENGTH_SIZE = 10;
    private final int[] fieldIndexByPosition;
    private final List<Field> fields;
    private final int estimatedLength;
    private final boolean isFixedLength;
    private final boolean isMaxLength;
    private final int minNullable;
    
    protected ValueSchema(int minNullable, List<Field> fields) {
        this.minNullable = minNullable;
        this.fields = ImmutableList.copyOf(fields);
        int estimatedLength = 0;
        boolean isMaxLength = true, isFixedLength = true;
        int positions = 0;
        for (Field field : fields) {
            int fieldEstLength = 0;
            PDataType type = field.getType();
            Integer theMaxLength = type.getMaxLength();
            if (type.isFixedWidth()) {
                fieldEstLength += (theMaxLength == null ? field.getMaxLength() : theMaxLength);
            } else {
                isFixedLength = false;
                // Account for vint for length if not fixed
                if (theMaxLength == null) {
                    isMaxLength = false;
                    fieldEstLength += ESTIMATED_VARIABLE_LENGTH_SIZE;
                } else {
                    fieldEstLength += WritableUtils.getVIntSize(theMaxLength);
                    fieldEstLength = theMaxLength;
                }
            }
            positions += field.getCount();
            estimatedLength += fieldEstLength * field.getCount();
        }
        fieldIndexByPosition = new int[positions];
        for (int i = 0, j= 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            Arrays.fill(fieldIndexByPosition, j, j + field.getCount(), i);
            j += field.getCount();
        }
        this.isFixedLength = isFixedLength;
        this.isMaxLength = isMaxLength;
        this.estimatedLength = estimatedLength;
    }

    public int getFieldCount() {
        return fieldIndexByPosition.length;
    }
    
    public List<Field> getFields() {
        return fields;
    }
    
    /**
     * @return true if all types are fixed width
     */
    public boolean isFixedLength() {
        return isFixedLength;
    }
    
    /**
     * @return true if {@link #getEstimatedValueLength()} returns the maximum length
     * of a serialized value for this schema
     */
    public boolean isMaxLength() {
        return isMaxLength;
    }
    
    /**
     * @return estimated size in bytes of a serialized value for this schema
     */
    public int getEstimatedValueLength() {
        return estimatedLength;
    }
    
    /**
     * Non-nullable fields packed to the left so that we do not need to store trailing nulls.
     * Knowing the minimum position of a nullable field enables this.
     * @return the minimum position of a nullable field
     */
    public int getMinNullable() {
        return minNullable;
    }
    
    public static final class Field implements Writable {
        private int count;
        private PDataType type;
        private int maxLength = 0;
        
        public Field() {
        }
        
        private Field(PDatum datum, int count) {
            this.type = datum.getDataType();
            this.count = count;
            if (this.type.isFixedWidth() && this.type.getMaxLength() == null) {
                this.maxLength = datum.getMaxLength();
            }
        }
        
        private Field(Field field, int count) {
            this.type = field.getType();
            this.maxLength = field.getMaxLength();
            this.count = count;
        }
        
        public final PDataType getType() {
            return type;
        }
        
        public final int getMaxLength() {
            return maxLength;
        }
        
        public final int getCount() {
            return count;
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            this.type = PDataType.values()[WritableUtils.readVInt(input)];
            this.count = WritableUtils.readVInt(input);
            if (this.type.isFixedWidth() && this.type.getMaxLength() == null) {
                this.maxLength = WritableUtils.readVInt(input);
            }
        }

        @Override
        public void write(DataOutput output) throws IOException {
            WritableUtils.writeVInt(output, type.ordinal());
            WritableUtils.writeVInt(output, count);
            if (type.isFixedWidth() && type.getMaxLength() == null) {
                WritableUtils.writeVInt(output, maxLength);
            }
        }
    }
    
    public abstract static class ValueSchemaBuilder {
        private List<Field> fields = new ArrayList<Field>();
        protected int nFields = Integer.MAX_VALUE;
        protected int minNullable;
        
        protected List<Field> buildFields() {
            List<Field> condensedFields = new ArrayList<Field>(fields.size());
            for (int i = 0; i < Math.min(nFields,fields.size()); ) {
                Field field = fields.get(i);
                int count = 1;
                // Prevent repeating fields from spanning across non-null/null boundary
                while ( ++i < fields.size() && i != this.minNullable && field.getType() == fields.get(i).getType() && field.getMaxLength() == fields.get(i).getMaxLength()) {
                    count++;
                }
                condensedFields.add(count == 1 ? field : new Field(field,count));
            }
            return condensedFields;
        }

        abstract public ValueSchema build();

        public ValueSchemaBuilder setMinNullable(int minNullable) {
            this.minNullable = minNullable;
            return this;
        }

        public ValueSchemaBuilder setMaxFields(int nFields) {
            this.nFields = nFields;
            return this;
        }
        
        public ValueSchemaBuilder addField(PDatum datum) {
            fields.add(new Field(datum, 1));
            return this;
        }
    }
    
    public boolean isNull(int position, ValueBitSet bitSet) {
        int nBit = position - getMinNullable();
        return (nBit >= 0 && !bitSet.get(nBit));
    }
    
    /**
     * Set the bytes ptr to the value at the zero-based positional index for
     * a byte array valid against this schema.
     * @param ptr bytes pointer whose offset is set to the beginning of the
     *  bytes representing a byte array valid against this schema. The ptr
     *  will be scoped down to point to the value at the position specified.
     * @param position position the zero-based positional index of the value
     * @return true if a value was found at the position and false if it is null.
     */
    public boolean setAccessor(ImmutableBytesWritable ptr, int position, ValueBitSet bitSet) {
        if (isNull(position, bitSet)) {
            return false;
        }
        setAccessor(ptr, 0, position, bitSet);
        return true;
    }
    
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="NP_BOOLEAN_RETURN_NULL",
            justification="Returns null by design.")
    protected Boolean positionPtr(ImmutableBytesWritable ptr, int position, ValueBitSet bitSet) {
        if (position >= getFieldCount()) {
            return null;
        }
        if (position < this.minNullable || bitSet.get(position - this.minNullable)) {
            // Move the pointer past the current value and set length
            // to 0 to ensure you never set the ptr past the end of the
            // backing byte array.
            ptr.set(ptr.get(), ptr.getOffset() + ptr.getLength(), 0);
            int length = nextField(ptr, fields.get(fieldIndexByPosition[position]), 1);
            ptr.set(ptr.get(),ptr.getOffset()-length,length);
            return ptr.getLength() > 0;
        }
        return false;
    }
    
    public Boolean first(ImmutableBytesWritable ptr, int position, ValueBitSet bitSet) {
        ptr.set(ptr.get(), ptr.getOffset(), 0);
        return positionPtr(ptr, position, bitSet);
    }
    
    /**
     * Move the bytes ptr to the position after the positional index provided
     * @param ptr bytes pointer pointing to the value at the positional index
     * provided.
     * @param position zero-based index of the field in the value schema at
     *  which to position the ptr.
     * @param bitSet bit set representing whether or not a value is null
     * @return true if there is a field after position and false otherwise.
     */
    public Boolean next(ImmutableBytesWritable ptr, int position, ValueBitSet bitSet) {
        return positionPtr(ptr, position, bitSet);
    }
    
    private int adjustReadFieldCount(int position, int nFields, ValueBitSet bitSet) {
        int nBit = position - this.minNullable;
        if (nBit < 0) {
            return nFields;
        } else {
            return nFields - bitSet.getNullCount(nBit, nFields);
        }
    }
    
    /**
     * Similar to {@link #setAccessor(ImmutableBytesWritable, int)}, but allows for the bytes
     * pointer to be serially stepped through all or a subset of the values.
     * @param ptr pointer to bytes offset of startPosition (the length does not matter).
     *  Upon return, will be positioned to the value at endPosition.
     * @param startPosition the position at which the ptr offset is positioned
     * @param endPosition the position at which the ptr should be positioned upon return. The
     *  length of ptr will be updated to the length of the value at endPosition.
     *  TODO: should be able to use first for this instead
     */
    private void setAccessor(ImmutableBytesWritable ptr, int startPosition, int endPosition, ValueBitSet bitSet) {
        int length = 0;
        int position = startPosition;
        while (position <= endPosition) {
            Field field = fields.get(fieldIndexByPosition[position]);
            // Need to step one by one for nullable fields, since any one of them could be null
            int nRepeats = field.getCount();
            // If the desired position is midway between the repeating field,
            // then adjust nFields down properly
            int nFields = Math.min(nRepeats, endPosition - position + 1);
            int nSkipFields = adjustReadFieldCount(position, nFields, bitSet);
            if (nSkipFields > 0) {
                length = nextField(ptr, field, nSkipFields); // remember last length so we can back up at end
            }
            position += nFields;
        }
        ptr.set(ptr.get(),ptr.getOffset()-length,length);
    }
    
    public void serialize(DataOutput output) throws IOException {
        WritableUtils.writeVInt(output, minNullable);
        WritableUtils.writeVInt(output, fields.size());
        for (int i = 0; i < fields.size(); i++) {
            fields.get(i).write(output);
        }
    }
    
    public Field getField(int position) {
        return fields.get(fieldIndexByPosition[position]);
    }
    
    protected static byte[] ensureSize(byte[] b, int offset, int size) {
        if (size > b.length) {
            byte[] bBigger = new byte[Math.min(b.length * 2, size)];
            System.arraycopy(b, 0, bBigger, 0, offset);
            return bBigger;
        }
        return b;
    }

    protected int nextField(ImmutableBytesWritable ptr, Field field, int nFields) {
        if (field.getType().isFixedWidth()) {
            return positionFixedLength(ptr, field, nFields);
        } else {
            return positionVarLength(ptr, field, nFields);
        }
    }
    
    protected int positionFixedLength(ImmutableBytesWritable ptr, Field field, int nFields) {
        PDataType type = field.getType();
        int length = (type.getMaxLength() == null) ? field.getMaxLength() : type.getMaxLength();
        ptr.set(ptr.get(),ptr.getOffset() + nFields * length, ptr.getLength());
        return length;
    }
    
    abstract protected int positionVarLength(ImmutableBytesWritable ptr, Field field, int nFields);
    abstract protected int writeVarLengthField(ImmutableBytesWritable ptr, byte[] b, int offset);
    
    /**
     * @return byte representation of the ValueSchema
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
            PDataType type = field.getType();
            for (int j = 0; j < field.getCount(); j++) {
                if (aggregators[index].evaluate(null, ptr)) { // Skip null values
                    if (index >= minNullableIndex) {
                        valueSet.set(index - minNullableIndex);
                    }
                    if (!type.isFixedWidth()) {
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
    
}
