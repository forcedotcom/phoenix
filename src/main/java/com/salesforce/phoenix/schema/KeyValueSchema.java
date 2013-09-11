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

import com.salesforce.phoenix.query.QueryConstants;
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

        @Override
        public KeyValueSchema build() {
            List<Field> condensedFields = buildFields();
            return new KeyValueSchema(this.minNullable, condensedFields);
        }

        @Override
        public KeyValueSchemaBuilder setMinNullable(int minNullable) {
            super.setMinNullable(minNullable);
            return this;
        }

        @Override
        public KeyValueSchemaBuilder setMaxFields(int nFields) {
            super.setMaxFields(nFields);
            return this;
        }
        
        @Override
        public KeyValueSchemaBuilder addField(PDatum datum) {
            super.addField(datum);
            return this;
        }
    }
    
    @Override
    protected int positionVarLength(ImmutableBytesWritable ptr, int position, int nFields, int maxLength) {
        int length = 0;
        while (nFields-- > 0) {
            length = ByteUtil.vintFromBytes(ptr);
            ptr.set(ptr.get(),ptr.getOffset()+length,ptr.getLength());
        }
        return length;
    }
    
    @Override
    protected int getVarLengthBytes(int length) {
        return length + WritableUtils.getVIntSize(length);
    }
    
    @Override
    protected int writeVarLengthField(ImmutableBytesWritable ptr, byte[] b, int offset) {
        int length = ptr.getLength();
        offset += ByteUtil.vintToBytes(b, offset, length);
        System.arraycopy(ptr.get(), ptr.getOffset(), b, offset, length);                        
        offset += length;
        return offset;
    }

    @Override
    public Boolean previous(ImmutableBytesWritable ptr, int position, int minOffset) {
        if (position < 0) {
            return null;
        }
        Field field = this.getField(position);
        if (field.getType().isFixedWidth()) {
            ptr.set(ptr.get(), minOffset, field.getByteSize());
            return true;
        }
        if (position == 0) {
            ptr.set(ptr.get(), minOffset, ptr.getOffset() - minOffset - 1);
            return true;
        }
        field = this.getField(position-1);
        // Field before the one we want to position at is variable length
        // In this case, we can search backwards for our separator byte
        // to determine the length
        if (!field.getType().isFixedWidth()) {
            byte[] buf = ptr.get();
            int offset = ptr.getOffset()-1;
            while (offset > minOffset /* sanity check*/ && buf[offset] != QueryConstants.SEPARATOR_BYTE) {
                offset--;
            }
            if (offset == minOffset) { // shouldn't happen
                ptr.set(buf, minOffset, ptr.getOffset()-minOffset-1);
            } else {
                ptr.set(buf,offset+1,ptr.getOffset()-offset-2);
            }
            return true;
        }
        int i,fixedOffset = field.getByteSize();
        for (i = position-2; i >= 0 && this.getField(i).getType().isFixedWidth(); i--) {
            fixedOffset += this.getField(i).getByteSize();
        }
        // All of the previous fields are fixed width, so we can calculate the offset
        // based on the total fixed offset
        if (i < 0) {
            int length = ptr.getOffset() - 1 - fixedOffset;
            ptr.set(ptr.get(),minOffset+fixedOffset, length);
            return true;
        }
        // Otherwise we're stuck with starting from the minOffset and working all the way forward,
        // because we can't infer the length of the previous position.
        ptr.set(ptr.get(), minOffset, ptr.getOffset()-1-minOffset);
        int maxOffset = this.iterator(ptr);
        for (i = 0; i <= position; i++)  {
            this.next(ptr,i,maxOffset);
        }
        return true;
    }
}
