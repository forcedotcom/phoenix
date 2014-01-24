/*******************************************************************************
 * Copyright (c) 2014, Salesforce.com, Inc. All rights reserved. Redistribution and use in source and binary forms, with
 * or without modification, are permitted provided that the following conditions are met: Redistributions of source code
 * must retain the above copyright notice, this list of conditions and the following disclaimer. Redistributions in
 * binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution. Neither the name of Salesforce.com nor the names
 * of its contributors may be used to endorse or promote products derived from this software without specific prior
 * written permission. THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.phoenix.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.schema.PArrayDataType;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PhoenixArray;
import com.salesforce.phoenix.schema.tuple.Tuple;

/**
 * Creates an expression for Upsert with Values/Select using ARRAY
 */
public class ArrayConstructorExpression extends BaseCompoundExpression {
    private PDataType baseType;
    private int position = -1;
    private Object[] elements;
    
    
    public ArrayConstructorExpression(List<Expression> children, PDataType baseType) {
        super(children);
        init(baseType);
    }

    private void init(PDataType baseType) {
        this.baseType = baseType;
        elements = new Object[getChildren().size()];
    }
    
    @Override
    public PDataType getDataType() {
        return PDataType.fromTypeId(baseType.getSqlType() + Types.ARRAY);
    }

    @Override
    public void reset() {
        super.reset();
        position = 0;
        Arrays.fill(elements, null);
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        for (int i = position >= 0 ? position : 0; i < elements.length; i++) {
            Expression child = children.get(i);
            if (!child.evaluate(tuple, ptr)) {
                if (tuple != null && !tuple.isImmutable()) {
                    if (position >= 0) position = i;
                    return false;
                }
            } else {
                elements[i] = baseType.toObject(ptr, child.getDataType(), child.getColumnModifier());
            }
        }
        if (position >= 0) position = elements.length;
        PhoenixArray array = PArrayDataType.instantiatePhoenixArray(baseType, elements);
        // FIXME: Need to see if this creation of an array and again back to byte[] can be avoided
        ptr.set(getDataType().toBytes(array));
        return true;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        int baseTypeOrdinal = WritableUtils.readVInt(input);
        init(PDataType.values()[baseTypeOrdinal]);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeVInt(output, baseType.ordinal());
    }

}
