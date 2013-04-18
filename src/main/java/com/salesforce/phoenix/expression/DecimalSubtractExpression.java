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


/**
 * 
 * Subtract expression implementation
 *
 * @author kmahadik
 * @since 0.1
 */
public class DecimalSubtractExpression extends SubtractExpression {
    private Integer maxLength;
    private Integer scale;

    public DecimalSubtractExpression() {
    }

    public DecimalSubtractExpression(List<Expression> children) {
        super(children);
        for (int i=0; i<children.size(); i++) {
            Expression childExpr = children.get(i);
            if (i == 0) {
                maxLength = childExpr.getMaxLength();
                scale = childExpr.getScale();
            } else if (maxLength != null && scale != null && childExpr.getMaxLength() != null
                    && childExpr.getScale() != null) {
                maxLength = getPrecision(maxLength, childExpr.getMaxLength(), scale, childExpr.getScale());
                scale = getScale(maxLength, childExpr.getMaxLength(), scale, childExpr.getScale());
            }
        }
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
            boolean isDate = childType.isCoercibleTo(PDataType.DATE);
            ColumnModifier childColumnModifier = childExpr.getColumnModifier();
            BigDecimal bd = isDate ?
                    BigDecimal.valueOf(childType.getCodec().decodeLong(ptr, childColumnModifier)) :
                    (BigDecimal)PDataType.DECIMAL.toObject(ptr, childType, childColumnModifier);
            
            if (result == null) {
                result = bd;
            } else {
                result = result.subtract(bd);
                /*
                 * Special case for date subtraction - note that only first two expression may be dates.
                 * We need to convert the date to a unit of "days" because that's what sql expects.
                 */
                if (isDate) {
                    result = result.divide(BD_MILLIS_IN_DAY, PDataType.DEFAULT_MATH_CONTEXT);
                }
            }
        }
        if (maxLength != null && scale != null) {
            result = NumberUtil.setDecimalWidthAndScale(result, maxLength, scale);
        }
        if (result == null) {
            throw new ValueTypeIncompatibleException(PDataType.DECIMAL, maxLength, scale);
        }
        ptr.set(PDataType.DECIMAL.toBytes(result));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDataType.DECIMAL;
    }

    @Override
    public Integer getScale() {
        return scale;
    }

    @Override
    public Integer getMaxLength() {
        return maxLength;
    }
}
