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
package com.salesforce.phoenix.expression;

import java.math.BigDecimal;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;


public class DateSubtractExpression extends SubtractExpression {
    
    public DateSubtractExpression() {
    }

    public DateSubtractExpression(List<Expression> children) {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        long finalResult=0;
        
        for(int i=0;i<children.size();i++) {
            if (!children.get(i).evaluate(tuple, ptr) || ptr.getLength() == 0) {
                return false;
            }
            long value;
            PDataType type = children.get(i).getDataType();
            ColumnModifier columnModifier = children.get(i).getColumnModifier();
            if (type == PDataType.DECIMAL) {
                BigDecimal bd = (BigDecimal)PDataType.DECIMAL.toObject(ptr, columnModifier);
                value = bd.multiply(BD_MILLIS_IN_DAY).longValue();
            } else if (type.isCoercibleTo(PDataType.LONG)) {
                value = type.getCodec().decodeLong(ptr, columnModifier) * QueryConstants.MILLIS_IN_DAY;
            } else if (type.isCoercibleTo(PDataType.DOUBLE)) {
                value = (long)(type.getCodec().decodeDouble(ptr, columnModifier) * QueryConstants.MILLIS_IN_DAY);
            } else {
                value = type.getCodec().decodeLong(ptr, columnModifier);
            }
            if (i == 0) {
                finalResult = value;
            } else {
                finalResult -= value;
            }
        }
        byte[] resultPtr=new byte[getDataType().getByteSize()];
        ptr.set(resultPtr);
        getDataType().getCodec().encodeLong(finalResult, ptr);
        return true;
    }

    @Override
    public final PDataType getDataType() {
        return PDataType.DATE;
    }

}
