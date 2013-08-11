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

import com.salesforce.phoenix.exception.ValueTypeIncompatibleException;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.NumberUtil;


public class DecimalDivideExpression extends DivideExpression {

    public DecimalDivideExpression() {
    }

    public DecimalDivideExpression(List<Expression> children) {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        BigDecimal result = null;
        for (int i=0; i<children.size(); i++) {
            Expression childExpr = children.get(i);
            if (!childExpr.evaluate(tuple, ptr)) {
                return false;
            }
            if (ptr.getLength() == 0) {
                return true;
            }
            
            PDataType childType = childExpr.getDataType();
            ColumnModifier childColumnModifier = childExpr.getColumnModifier();
            BigDecimal bd= (BigDecimal)PDataType.DECIMAL.toObject(ptr, childType, childColumnModifier);
            
            if (result == null) {
                result = bd;
            } else {
                result = result.divide(bd, PDataType.DEFAULT_MATH_CONTEXT);
            }
        }
        if (getMaxLength() != null && getScale() != null) {
            result = NumberUtil.setDecimalWidthAndScale(result, getMaxLength(), getScale());
        }
        if (result == null) {
            throw new ValueTypeIncompatibleException(PDataType.DECIMAL, getMaxLength(), getScale());
        }
        ptr.set(PDataType.DECIMAL.toBytes(result));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDataType.DECIMAL;
    }
}
