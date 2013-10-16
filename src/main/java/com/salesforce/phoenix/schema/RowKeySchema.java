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

import static com.salesforce.phoenix.query.QueryConstants.SEPARATOR_BYTE;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.query.QueryConstants;


/**
 * 
 * Schema for the bytes in a RowKey. For the RowKey, we use a null byte
 * to terminate a variable length type, while for KeyValue bytes we
 * write the length of the var char preceding the value. We can't do
 * that for a RowKey because it would affect the sort order.
 *
 * @author jtaylor
 * @since 0.1
 */
public class RowKeySchema extends ValueSchema {
    public static final RowKeySchema EMPTY_SCHEMA = new RowKeySchema(0,Collections.<Field>emptyList())
    ;
    
    public RowKeySchema() {
    }
    
    protected RowKeySchema(int minNullable, List<Field> fields) {
        super(minNullable, fields);
    }

    public static class RowKeySchemaBuilder extends ValueSchemaBuilder {
        public RowKeySchemaBuilder(int maxFields) {
            super(maxFields);
            setMaxFields(maxFields);
        }
        
        @Override
        public RowKeySchemaBuilder addField(PDatum datum, boolean isNullable, ColumnModifier columnModifier) {
            super.addField(datum, isNullable, columnModifier);
            return this;
        }

        @Override
        public RowKeySchema build() {
            List<Field> condensedFields = buildFields();
            return new RowKeySchema(this.minNullable, condensedFields);
        }
    }

    public int getMaxFields() {
        return this.getMinNullable();
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="NP_BOOLEAN_RETURN_NULL", 
            justification="Designed to return null.")
    public Boolean iterator(byte[] src, int srcOffset, int srcLength, ImmutableBytesWritable ptr, int position) {
        Boolean hasValue = null;
        ptr.set(src, srcOffset, 0);
        int maxOffset = srcOffset + srcLength;
        for (int i = 0; i < position; i++) {
            hasValue = next(ptr, i, maxOffset);
        }
        return hasValue;
    }
    
    public Boolean iterator(ImmutableBytesWritable srcPtr, ImmutableBytesWritable ptr, int position) {
        return iterator(srcPtr.get(), srcPtr.getOffset(), srcPtr.getLength(), ptr, position);
    }
    
    public Boolean iterator(byte[] src, ImmutableBytesWritable ptr, int position) {
        return iterator(src, 0, src.length, ptr, position);
    }
    
    public int iterator(byte[] src, int srcOffset, int srcLength, ImmutableBytesWritable ptr) {
        int maxOffset = srcOffset + srcLength;
        iterator(src, srcOffset, srcLength, ptr, 0);
        return maxOffset;
    }
    
    public int iterator(byte[] src, ImmutableBytesWritable ptr) {
        return iterator(src, 0, src.length, ptr);
    }
    
    public int iterator(ImmutableBytesWritable ptr) {
        return iterator(ptr.get(),ptr.getOffset(),ptr.getLength(), ptr);
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
    public Boolean next(ImmutableBytesWritable ptr, int position, int maxOffset) {
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
        // If positioned at SEPARATOR_BYTE, skip it.
        if (position > 0 && !getField(position-1).getDataType().isFixedWidth()) {
            ptr.set(ptr.get(), ptr.getOffset()+ptr.getLength()+1, 0);
        }
        Field field = this.getField(position);
        if (field.getDataType().isFixedWidth()) {
            ptr.set(ptr.get(),ptr.getOffset(), field.getByteSize());
        } else {
            if (position+1 == getFieldCount() ) { // Last field has no terminator
                ptr.set(ptr.get(), ptr.getOffset(), maxOffset - ptr.getOffset());
            } else {
                byte[] buf = ptr.get();
                int offset = ptr.getOffset();
                while (offset < maxOffset && buf[offset] != SEPARATOR_BYTE) {
                    offset++;
                }
                ptr.set(buf, ptr.getOffset(), offset - ptr.getOffset());
            }
        }
        return ptr.getLength() > 0;
    }
    
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="NP_BOOLEAN_RETURN_NULL", 
            justification="Designed to return null.")
    public Boolean previous(ImmutableBytesWritable ptr, int position, int minOffset) {
        if (position < 0) {
            return null;
        }
        Field field = this.getField(position);
        if (field.getDataType().isFixedWidth()) {
            ptr.set(ptr.get(), ptr.getOffset()-field.getByteSize(), field.getByteSize());
            return true;
        }
        // If ptr has length of zero, it is assumed that we're at the end of the row key
        int offsetAdjustment = position + 1 == this.getFieldCount() || ptr.getLength() == 0 ? 0 : 1;
        if (position == 0) {
            ptr.set(ptr.get(), minOffset, ptr.getOffset() - minOffset - offsetAdjustment);
            return true;
        }
        field = this.getField(position-1);
        // Field before the one we want to position at is variable length
        // In this case, we can search backwards for our separator byte
        // to determine the length
        if (!field.getDataType().isFixedWidth()) {
            byte[] buf = ptr.get();
            int offset = ptr.getOffset()-1-offsetAdjustment;
            while (offset > minOffset /* sanity check*/ && buf[offset] != QueryConstants.SEPARATOR_BYTE) {
                offset--;
            }
            if (offset == minOffset) { // shouldn't happen
                ptr.set(buf, minOffset, ptr.getOffset()-minOffset-1);
            } else {
                ptr.set(buf,offset+1,ptr.getOffset()-1-offsetAdjustment-offset); // Don't include null terminator in length
            }
            return true;
        }
        int i,fixedOffset = field.getByteSize();
        for (i = position-2; i >= 0 && this.getField(i).getDataType().isFixedWidth(); i--) {
            fixedOffset += this.getField(i).getByteSize();
        }
        // All of the previous fields are fixed width, so we can calculate the offset
        // based on the total fixed offset
        if (i < 0) {
            int length = ptr.getOffset() - fixedOffset - minOffset - offsetAdjustment;
            ptr.set(ptr.get(),minOffset+fixedOffset, length);
            return true;
        }
        // Otherwise we're stuck with starting from the minOffset and working all the way forward,
        // because we can't infer the length of the previous position.
        return iterator(ptr.get(), minOffset, ptr.getOffset() - minOffset - offsetAdjustment, ptr, position+1);
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="NP_BOOLEAN_RETURN_NULL", 
            justification="Designed to return null.")
    public Boolean reposition(ImmutableBytesWritable ptr, int oldPosition, int newPosition, int minOffset, int maxOffset) {
        if (newPosition == oldPosition) {
            return ptr.getLength() > 0;
        }
        Boolean hasValue = null;
        if (newPosition > oldPosition) {
            do {
                hasValue = next(ptr, ++oldPosition, maxOffset);
            }  while (hasValue != null && oldPosition < newPosition) ;
        } else {
            int nVarLengthFromBeginning = 0;
            for (int i = 0; i <= newPosition; i++) {
                if (!this.getField(i).getDataType().isFixedWidth()) {
                    nVarLengthFromBeginning++;
                }
            }
            int nVarLengthBetween = 0;
            for (int i = oldPosition - 1; i >= newPosition; i--) {
                if (!this.getField(i).getDataType().isFixedWidth()) {
                    nVarLengthBetween++;
                }
            }
            if (nVarLengthBetween > nVarLengthFromBeginning) {
                return iterator(ptr.get(), minOffset, maxOffset-minOffset, ptr, newPosition+1);
            }
            do  {
                hasValue = previous(ptr, --oldPosition, minOffset);
            } while (hasValue != null && oldPosition > newPosition);
        }
        
        return hasValue;
    }
}
