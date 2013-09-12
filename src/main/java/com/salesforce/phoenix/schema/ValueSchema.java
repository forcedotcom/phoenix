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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * 
 * Simple flat schema over a byte array where fields may be any of {@link PDataType}.
 * Optimized for positional access by index.
 *
 * @author jtaylor
 * @since 0.1
 */
public abstract class ValueSchema implements Writable {
    public static final int ESTIMATED_VARIABLE_LENGTH_SIZE = 10;
    private int[] fieldIndexByPosition;
    private List<Field> fields;
    private int estimatedLength;
    private boolean isFixedLength;
    private boolean isMaxLength;
    private int minNullable;
    
    public ValueSchema() {
    }
    
    protected ValueSchema(int minNullable, List<Field> fields) {
        init(minNullable, fields);
    }

    private void init(int minNullable, List<Field> fields) {
        this.minNullable = minNullable;
        this.fields = ImmutableList.copyOf(fields);
        int estimatedLength = 0;
        boolean isMaxLength = true, isFixedLength = true;
        int positions = 0;
        for (Field field : fields) {
            int fieldEstLength = 0;
            PDataType type = field.getType();
            Integer byteSize = type.getByteSize();
            if (type.isFixedWidth()) {
                fieldEstLength += field.getByteSize();
            } else {
                isFixedLength = false;
                // Account for vint for length if not fixed
                if (byteSize == null) {
                    isMaxLength = false;
                    fieldEstLength += ESTIMATED_VARIABLE_LENGTH_SIZE;
                } else {
                    fieldEstLength += WritableUtils.getVIntSize(byteSize);
                    fieldEstLength = byteSize;
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
        private int byteSize = 0;
        
        public Field() {
        }
        
        private Field(PDatum datum, int count) {
            this.type = datum.getDataType();
            this.count = count;
            if (this.type.isFixedWidth() && this.type.getByteSize() == null) {
                this.byteSize = datum.getByteSize();
            }
        }
        
        private Field(Field field, int count) {
            this.type = field.getType();
            this.byteSize = field.byteSize;
            this.count = count;
        }
        
        public final PDataType getType() {
            return type;
        }
        
        public final int getByteSize() {
            return type.getByteSize() == null ? byteSize : type.getByteSize();
        }
        
        public final int getCount() {
            return count;
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            this.type = PDataType.values()[WritableUtils.readVInt(input)];
            this.count = WritableUtils.readVInt(input);
            if (this.type.isFixedWidth() && this.type.getByteSize() == null) {
                this.byteSize = WritableUtils.readVInt(input);
            }
        }

        @Override
        public void write(DataOutput output) throws IOException {
            WritableUtils.writeVInt(output, type.ordinal());
            WritableUtils.writeVInt(output, count);
            if (type.isFixedWidth() && type.getByteSize() == null) {
                WritableUtils.writeVInt(output, byteSize);
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
                while ( ++i < fields.size() && i != this.minNullable && field.getType() == fields.get(i).getType() && field.getByteSize() == fields.get(i).getByteSize()) {
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
    
    public int getEstimatedByteSize() {
        int size = 0;
        size += WritableUtils.getVIntSize(minNullable);
        size += WritableUtils.getVIntSize(fields.size());
        size += fields.size() * 3;
        return size;
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
    
    @Override
    public void readFields(DataInput in) throws IOException {
        int minNullable = WritableUtils.readVInt(in);
        int nFields = WritableUtils.readVInt(in);
        List<Field> fields = Lists.newArrayListWithExpectedSize(nFields);
        for (int i = 0; i < nFields; i++) {
            Field field = new Field();
            field.readFields(in);
            fields.add(field);
        }
        init(minNullable, fields);
    }
         
    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, minNullable);
        WritableUtils.writeVInt(out, fields.size());
        for (int i = 0; i < fields.size(); i++) {
            fields.get(i).write(out);
        }
    }

}
