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
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.schema.PDataType.DateNative;
import com.salesforce.phoenix.schema.PDataType.LongNative;
import com.salesforce.phoenix.schema.tuple.Tuple;


public class DateAddExpression extends AddExpression {
    static private final BigDecimal BD_MILLIS_IN_DAY = BigDecimal.valueOf(QueryConstants.MILLIS_IN_DAY);
    
    public DateAddExpression() {
    }

    public DateAddExpression(List<Expression> children) {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        DateNative dateNative=DateNative.getInstance();
        long finalResult=0;
        
        for(int i=0;i<children.size();i++) {
            if (!children.get(i).evaluate(tuple, ptr) || ptr.getLength() == 0) {
                return false;
            }
            long value;
            PDataType type = children.get(i).getDataType();
            if (type == PDataType.DECIMAL) {
                BigDecimal bd = (BigDecimal)PDataType.DECIMAL.toObject(ptr);
                value = bd.multiply(BD_MILLIS_IN_DAY).longValue();
            } else if (type.isCoercibleTo(PDataType.LONG)) {
                LongNative longNative = (LongNative)type.getNative();
                value = longNative.toLong(ptr) * QueryConstants.MILLIS_IN_DAY;
            } else {
                value = dateNative.toLong(ptr);
            }
            finalResult += value;
        }
        byte[] resultPtr=new byte[PDataType.LONG.getByteSize()];
        ptr.set(resultPtr);
        dateNative.putLong(finalResult, ptr);
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDataType.DATE;
    }

}
