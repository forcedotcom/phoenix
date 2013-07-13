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
import com.salesforce.phoenix.util.ByteUtil;


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
        @Override
        public RowKeySchemaBuilder setMinNullable(int minNullable) {
            super.setMinNullable(minNullable);
            return this;
        }

        @Override
        public RowKeySchemaBuilder setMaxFields(int nFields) {
            super.setMaxFields(nFields);
            return this;
        }
        
        @Override
        public RowKeySchemaBuilder addField(PDatum datum) {
            super.addField(datum);
            return this;
        }
        
        @Override
        public RowKeySchema build() {
            List<Field> condensedFields = buildFields();
            return new RowKeySchema(this.minNullable, condensedFields);
        }
    }

    @Override
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="NP_BOOLEAN_RETURN_NULL", 
            justification="Designed to return null.")
    public Boolean next(ImmutableBytesWritable ptr, int position, int maxOffset, ValueBitSet bitSet) {
        if (ptr.getOffset() + ptr.getLength() >= maxOffset) {
            ptr.set(ptr.get(), maxOffset, 0);
            return null;
        }
        // If positioned at SEPARATOR_BYTE, skip it.
        if (position > 0 && !getField(position-1).getType().isFixedWidth() && position < getMaxFields() && ptr.get()[ptr.getOffset()+ptr.getLength()] == QueryConstants.SEPARATOR_BYTE) {
            ptr.set(ptr.get(), ptr.getOffset()+ptr.getLength()+1, 0);
        }
        return super.next(ptr,position,maxOffset, bitSet);
    }
    
    @Override
    protected int positionVarLength(ImmutableBytesWritable ptr, int position, int nFields, int maxOffset) {
        int len = 0;
        int initialOffset = ptr.getOffset();
        while (nFields-- > 0) {
            byte[] buf = ptr.get();
            int offset = initialOffset;
            if (position+1 == getFieldCount() && nFields == 0) { // Last field has no terminator
                ptr.set(buf, maxOffset, 0);
                return maxOffset - initialOffset;
            }
            while (offset < maxOffset && buf[offset] != SEPARATOR_BYTE) {
                offset++;
            }
            ptr.set(buf, offset, 0);
            len = offset - initialOffset;
            initialOffset = offset + 1; // skip separator byte
        }
        return len;
    }

    @Override
    protected int getVarLengthBytes(int length) {
        return length + 1; // Size in bytes plus one for the separator byte
    }
    
    @Override
    protected int writeVarLengthField(ImmutableBytesWritable ptr, byte[] b, int offset) {
        int length = ptr.getLength();
        System.arraycopy(ptr.get(), ptr.getOffset(), b, offset, length);
        offset += length + 1;
        b[offset-1] = QueryConstants.SEPARATOR_BYTE;
        return offset;
    }
    
    public int getMaxFields() {
        return this.getMinNullable();
    }
    
    /**
     * Given potentially a partial key, but one that is valid against
     * this row key schema, increment it to the next key in the row
     * key schema key space.
     * @param ptr pointer to the key to be incremented
     * @return a new byte array with the incremented key
     */
    public byte[] nextKey(ImmutableBytesWritable ptr) {
        byte[] buf = ptr.get();
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        byte[] key;
        if (!this.getField(this.getMaxFields()-1).getType().isFixedWidth()) {
            // Add a SEPARATOR byte at the end if we have a complete key with a variable
            // length at the end
            if (this.setAccessor(ptr, this.getMaxFields()-1, ValueBitSet.EMPTY_VALUE_BITSET)) {
                key = new byte[length+1];
                System.arraycopy(buf, offset, key, 0, length);
                key[length] = QueryConstants.SEPARATOR_BYTE;
                ByteUtil.nextKey(key, key.length);
                return key;
            }
        }
        // No separator needed because we either have a fixed width value at the end
        // or we have a partial key which would be terminated with a separator byte.
        key = new byte[length];
        System.arraycopy(buf, offset, key, 0, length);
        ByteUtil.nextKey(key, key.length);
        return key;
    }
    
}
