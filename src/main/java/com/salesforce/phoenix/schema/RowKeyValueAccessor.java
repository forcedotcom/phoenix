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

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.util.ByteUtil;


/**
 * 
 * Class that encapsulates accessing a value stored in the row key.
 *
 * @author jtaylor
 * @since 0.1
 */
public class RowKeyValueAccessor implements Writable   {
    /**
     * Constructor solely for use during deserialization. Should not
     * otherwise be used.
     */
    public RowKeyValueAccessor() {
    }
    
    /**
     * Constructor to compile access to the value in the row key formed from
     * a list of PData.
     * 
     * @param data the list of data that make up the key
     * @param index the zero-based index of the data item to access.
     */
    public RowKeyValueAccessor(List<? extends PDatum> data, int index) {
        this.index = index;
        int[] offsets = new int[data.size()];
        int nOffsets = 0;
        Iterator<? extends PDatum> iterator = data.iterator();
        PDatum datum = iterator.next();
        int pos = 0;
        while (pos < index) {
            int offset = 0;
            if (datum.getDataType().isFixedWidth()) {
                do {
                    offset += datum.getMaxLength();
                    datum = iterator.next();
                    pos++;
                } while (pos < index && datum.getDataType().isFixedWidth());
                offsets[nOffsets++] = offset; // Encode fixed byte offset as positive
            } else {
                do {
                    offset++; // Count the number of variable length columns
                    datum = iterator.next();
                    pos++;
                } while (pos < index && !datum.getDataType().isFixedWidth());
                offsets[nOffsets++] = -offset; // Encode number of variable length columns as negative
            }
        }
        if (nOffsets < offsets.length) {
            this.offsets = Arrays.copyOf(offsets, nOffsets);
        } else {
            this.offsets = offsets;
        }
        // Remember this so that we don't bother looking for the null separator byte in this case
        this.isFixedLength = datum.getDataType().isFixedWidth();
        this.hasSeparator = !isFixedLength && (datum != data.get(data.size()-1));
    }
    
    private int index = -1; // Only available on client side
    private int[] offsets;
    private boolean isFixedLength;
    private boolean hasSeparator;

    public int getIndex() {
        return index;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (hasSeparator ? 1231 : 1237);
        result = prime * result + (isFixedLength ? 1231 : 1237);
        result = prime * result + Arrays.hashCode(offsets);
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        RowKeyValueAccessor other = (RowKeyValueAccessor)obj;
        if (hasSeparator != other.hasSeparator) return false;
        if (isFixedLength != other.isFixedLength) return false;
        if (!Arrays.equals(offsets, other.offsets)) return false;
        return true;
    }

    @Override
    public String toString() {
        return "RowKeyValueAccessor [offsets=" + Arrays.toString(offsets) + ", isFixedLength=" + isFixedLength
                + ", hasSeparator=" + hasSeparator + "]";
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        // Decode hasSeparator and isFixedLength from vint storing offset array length
        int length = WritableUtils.readVInt(input);
        hasSeparator = (length & 0x02) != 0;
        isFixedLength = (length & 0x01) != 0;
        length >>= 2;
        offsets = ByteUtil.deserializeIntArray(input, length);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        // Encode hasSeparator and isFixedLength into vint storing offset array length
        // (since there's plenty of room)
        int length = offsets.length << 2;
        length |= (hasSeparator ? 1 << 1 : 0) | (isFixedLength ? 1 : 0);
        ByteUtil.serializeIntArray(output, offsets, length);
    }
    
    /**
     * Calculate the byte offset in the row key to the start of the PK column value
     * @param keyBuffer the byte array of the row key
     * @param keyOffset the offset in the byte array of where the key begins
     * @return byte offset to the start of the PK column value
     */
    public int getOffset(byte[] keyBuffer, int keyOffset) {
        // Use encoded offsets to navigate through row key buffer
        for (int offset : offsets) {
            if (offset >= 0) { // If offset is non negative, it's a byte offset
                keyOffset += offset;
            } else { // Else, a negative offset is the number of variable length values to skip
                while (offset++ < 0) {
                    // FIXME: keyOffset < keyBuffer.length required because HBase passes bogus keys to filter to position scan (HBASE-6562)
                    while (keyOffset < keyBuffer.length && keyBuffer[keyOffset++] != SEPARATOR_BYTE) {
                    }
                }
            }
        }
        return keyOffset;
    }
    
    /**
     * Calculate the length of the PK column value
     * @param keyBuffer the byte array of the row key
     * @param keyOffset the offset in the byte array of where the key begins
     * @param keyLength the length of the entire row key
     * @return the length of the PK column value
     */
    public int getLength(byte[] keyBuffer, int keyOffset, int keyLength) {
        if (!hasSeparator) {
            return keyLength;
        }
        int offset = keyOffset;
        int maxOffset = keyOffset + keyLength;
        // FIXME: offset < maxOffset required because HBase passes bogus keys to filter to position scan (HBASE-6562)
        while (offset < maxOffset && keyBuffer[offset] != SEPARATOR_BYTE) {
            offset++;
        }
        return offset - keyOffset;
    }
}
