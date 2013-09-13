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

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import com.google.common.base.Preconditions;

/**
 * A ColumnModifier implementation modifies how bytes are stored in a primary key column.</p>  
 * The {@link ColumnModifier#apply apply} method is called when the bytes for a specific column are first written to HBase and again
 * when they are read back.  Phoenix attemps to minimize calls to apply when bytes are read out of HBase.   
 * 
 * @author simontoens
 * @since 1.2
 */
public enum ColumnModifier {
    /**
     * Invert the bytes in the src byte array to support descending ordering of row keys.
     */
    SORT_DESC(1) {
        @Override
        public byte[] apply(byte[] src, int srcOffset, byte[] dest, int dstOffset, int length) {
            Preconditions.checkNotNull(src);            
            Preconditions.checkNotNull(dest);            
            for (int i = 0; i < length; i++) {
                dest[dstOffset+i] = (byte)(src[srcOffset+i] ^ 0xFF);
            }                       
            return dest;
        }

        @Override
        public byte apply(byte b) {
            return (byte)(b ^ 0xFF);
        }

        @Override
        public CompareOp transform(CompareOp op) {
            switch (op) {
                case EQUAL:
                    return op;
                case GREATER:
                    return CompareOp.LESS;
                case GREATER_OR_EQUAL:
                    return CompareOp.LESS_OR_EQUAL;
                case LESS:
                    return CompareOp.GREATER;
                case LESS_OR_EQUAL:
                    return CompareOp.GREATER_OR_EQUAL;
                default:
                    throw new IllegalArgumentException("Unknown operator " + op);
            }
        }
    };
        
    private final int serializationId;
    
    ColumnModifier(int serializationId) {
        this.serializationId = serializationId;
    }
    
    public int getSerializationId() {
        return serializationId;
    }
    /**
     * Returns the ColumnModifier for the specified DDL stmt keyword.
     */
    public static ColumnModifier fromDDLValue(String modifier) {
        if (modifier == null) {
            return null;
        } else if (modifier.equalsIgnoreCase("ASC")) {
            return null;
        } else if (modifier.equalsIgnoreCase("DESC")) {
            return SORT_DESC;
        } else {
            return null;
        }                       
    }

   /**
    * Returns the ColumnModifier for the specified internal value.
    */
    public static ColumnModifier fromSystemValue(int value) {
        for (ColumnModifier mod : ColumnModifier.values()) {
            if (mod.getSerializationId() == value) {
                return mod;
            }
        }
        return null;
    }

    /**
     * Returns an internal value representing the specified ColumnModifier.
     */
    public static int toSystemValue(ColumnModifier columnModifier) {
        if (columnModifier == null) {
            return 0;
        }
        return columnModifier.getSerializationId();
    }

    /**
     * Copies the bytes from source array to destination array and applies the column modifier operation on the bytes
     * starting at the specified offsets.  The column modifier is applied to the number of bytes matching the 
     * specified length.
     * @param src  the source byte array to copy from, cannot be null
     * @param srcOffset the offset into the source byte array at which to begin.
     * @param dest the destination byte array into which to transfer the modified bytes.
     * @param dstOffset the offset into the destination byte array at which to begin
     * @param length the number of bytes for which to apply the modification
     * @return the destination byte array
     */
    public abstract byte[] apply(byte[] src, int srcOffset, byte[] dest, int dstOffset, int length);
    public abstract byte apply(byte b);
    
    public abstract CompareOp transform(CompareOp op);
}
