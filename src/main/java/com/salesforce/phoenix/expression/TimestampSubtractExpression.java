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
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.DateUtil;
/**
 * 
 * Class to encapsulate subtraction arithmetic for {@link PDataType#TIMESTAMP}.
 *
 * @author samarth.jain
 * @since 2.1.3
 */
public class TimestampSubtractExpression extends SubtractExpression {

    public TimestampSubtractExpression() {
    }

    public TimestampSubtractExpression(List<Expression> children) {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        BigDecimal finalResult = BigDecimal.ZERO;
        
        for(int i=0; i<children.size(); i++) {
            if (!children.get(i).evaluate(tuple, ptr)) {
                return false;
            }
            if (ptr.getLength() == 0) {
                return true;
            }
            BigDecimal value;
            PDataType type = children.get(i).getDataType();
            ColumnModifier columnModifier = children.get(i).getColumnModifier();
            if(type == PDataType.TIMESTAMP) {
                value = (BigDecimal)(PDataType.DECIMAL.toObject(ptr, PDataType.TIMESTAMP, columnModifier));
            } else if (type.isCoercibleTo(PDataType.DECIMAL)) {
                value = (((BigDecimal)PDataType.DECIMAL.toObject(ptr, columnModifier)).multiply(BD_MILLIS_IN_DAY)).setScale(6, RoundingMode.HALF_UP);
            } else if (type.isCoercibleTo(PDataType.DOUBLE)) {
                value = ((BigDecimal.valueOf(type.getCodec().decodeDouble(ptr, columnModifier))).multiply(BD_MILLIS_IN_DAY)).setScale(6, RoundingMode.HALF_UP);
            } else {
                value = BigDecimal.valueOf(type.getCodec().decodeLong(ptr, columnModifier));
            }
            if (i == 0) {
                finalResult = value;
            } else {
                finalResult = finalResult.subtract(value);
            }
        }
        Timestamp ts = DateUtil.getTimestamp(finalResult);
        byte[] resultPtr = new byte[getDataType().getByteSize()];
        PDataType.TIMESTAMP.toBytes(ts, resultPtr, 0);
        ptr.set(resultPtr);
        return true;
    }

    @Override
    public final PDataType getDataType() {
        return PDataType.TIMESTAMP;
    }
}
