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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.salesforce.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;


public class ValueBitSetTest {
    private static final int FIXED_WIDTH_CHAR_SIZE = 10;
    private KeyValueSchema generateSchema(int nFields, int nRepeating, final int nNotNull) {
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder();
        for (int i = 0; i < nFields; i++) {
            final int fieldIndex = i;
            for (int j = 0; j < nRepeating; j++) {
                PDatum datum = new PDatum() {

                    @Override
                    public boolean isNullable() {
                        return fieldIndex <= nNotNull;
                    }

                    @Override
                    public PDataType getDataType() {
                        return PDataType.values()[fieldIndex % PDataType.values().length];
                    }

                    @Override
                    public Integer getMaxLength() {
                        return !getDataType().isFixedWidth() ? null : getDataType().getMaxLength() == null ? FIXED_WIDTH_CHAR_SIZE : getDataType().getMaxLength();
                    }
                    
                };
                builder.addField(datum);
            }
        }
        builder.setMinNullable(nNotNull);
        KeyValueSchema schema = builder.build();
        return schema;
    }
    
    private static void setValueBitSet(KeyValueSchema schema, ValueBitSet valueSet) {
        for (int i = 0; i < schema.getFieldCount() - schema.getMinNullable(); i++) {
            if ((i & 1) == 1) {
                valueSet.set(i);
            }
        }
    }
    
    @Test
    public void testNullCount() {
        int nFields = 32;
        int nRepeating = 5;
        int nNotNull = 8;
        KeyValueSchema schema = generateSchema(nFields, nRepeating, nNotNull);
        ValueBitSet valueSet = ValueBitSet.newInstance(schema);
        setValueBitSet(schema, valueSet);
        
        // From beginning, not spanning longs
        assertEquals(5, valueSet.getNullCount(0, 10));
        // From middle, not spanning longs
        assertEquals(5, valueSet.getNullCount(10, 10));
        // From middle, spanning to middle of next long
        assertEquals(10, valueSet.getNullCount(64 - 5, 20));
        // from end, not spanning longs
        assertEquals(5, valueSet.getNullCount(nFields*nRepeating-nNotNull-10, 10));
        // from beginning, spanning long entirely into middle of next long
        assertEquals(64, valueSet.getNullCount(2, 128));
    }
    
    @Test
    public void testSizing() {
        int nFields = 32;
        int nRepeating = 5;
        int nNotNull = 8;
        KeyValueSchema schema = generateSchema(nFields, nRepeating, nNotNull);
        ValueBitSet valueSet = ValueBitSet.newInstance(schema);
        // Since no bits are set, it stores the long array length only
        assertEquals(Bytes.SIZEOF_SHORT, valueSet.getEstimatedLength());
        setValueBitSet(schema, valueSet);
        assertEquals(Bytes.SIZEOF_SHORT + Bytes.SIZEOF_LONG * 3, valueSet.getEstimatedLength());
        
        nFields = 18;
        nRepeating = 1;
        nNotNull = 2;
        schema = generateSchema(nFields, nRepeating, nNotNull);
        valueSet = ValueBitSet.newInstance(schema);
        assertEquals(Bytes.SIZEOF_SHORT, valueSet.getEstimatedLength());
        setValueBitSet(schema, valueSet);
        assertEquals(Bytes.SIZEOF_SHORT, valueSet.getEstimatedLength());
        
        nFields = 19;
        nRepeating = 1;
        nNotNull = 2;
        schema = generateSchema(nFields, nRepeating, nNotNull);
        valueSet = ValueBitSet.newInstance(schema);
        assertEquals(Bytes.SIZEOF_SHORT, valueSet.getEstimatedLength());
        setValueBitSet(schema, valueSet);
        assertEquals(Bytes.SIZEOF_SHORT + Bytes.SIZEOF_LONG, valueSet.getEstimatedLength());
        
        nFields = 19;
        nRepeating = 1;
        nNotNull = 19;
        schema = generateSchema(nFields, nRepeating, nNotNull);
        valueSet = ValueBitSet.newInstance(schema);
        assertEquals(0, valueSet.getEstimatedLength());
    }

}
