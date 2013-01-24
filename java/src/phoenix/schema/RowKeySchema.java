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
package phoenix.schema;

import static phoenix.query.QueryConstants.SEPARATOR_BYTE;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import phoenix.query.QueryConstants;

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
public class RowKeySchema extends ValueSchema  {
    public static final RowKeySchema EMPTY_SCHEMA = new RowKeySchema(0,Collections.<Field>emptyList())
    ;
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
        public RowKeySchema build() {
            List<Field> condensedFields = buildFields();
            return new RowKeySchema(this.minNullable, condensedFields);
        }
    }

    @Override
    public Boolean next(ImmutableBytesWritable ptr, int position, ValueBitSet bitSet) {
        // If positioned at SEPARATOR_BYTE, skip it.
        if (ptr.get()[ptr.getOffset()] == QueryConstants.SEPARATOR_BYTE) {
            ptr.set(ptr.get(), ptr.getOffset()+1, ptr.getLength());
        }
        return super.next(ptr,position,bitSet);
    }
    
    @Override
    protected int positionVarLength(ImmutableBytesWritable ptr, Field field, int nFields) {
        byte[] buf = ptr.get();
        int initialOffset = ptr.getOffset();
        int offset = initialOffset;
        int maxOffset = buf.length;
        while (offset < maxOffset && buf[offset] != SEPARATOR_BYTE) {
            offset++;
        }
        ptr.set(buf, offset, 0);
        return offset - initialOffset;
    }
    
    @Override
    protected int writeVarLengthField(ImmutableBytesWritable ptr, byte[] b, int offset) {
        int length = ptr.getLength();
        b = ensureSize(b, offset, offset + length + 1);
        System.arraycopy(ptr.get(), ptr.getOffset(), b, offset, length);
        offset += length + 1;
        b[offset-1] = QueryConstants.SEPARATOR_BYTE;
        return offset;
    }
}
