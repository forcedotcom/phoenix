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
package com.salesforce.phoenix.expression.function;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Lists;
import com.salesforce.phoenix.expression.CoerceExpression;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PDataType.PDataCodec;
import com.salesforce.phoenix.schema.tuple.Tuple;

/**
 * 
 * Class encapsulating the CEIL operation on {@link com.salesforce.phoenix.schema.PDataType#TIMESTAMP}
 * This class only supports CEIL {@link TimeUnit#MILLISECOND}. If you want more options of CEIL like 
 * using {@link TimeUnit#HOUR} use {@link CeilDateExpression}
 * 
 * @author samarth.jain
 * @since 3.0.0
 */
public class CeilTimestampExpression extends CeilDateExpression {
    
    public CeilTimestampExpression() {}
    
    private CeilTimestampExpression(List<Expression> children) {
        super(children);
    }
    
    /**
     * Creates a {@link CeilTimestampExpression} that uses {@link TimeUnit#MILLISECOND} 
     * as the time unit for rounding. 
     */
    public static CeilTimestampExpression create(Expression expr, int multiplier) throws SQLException {
        List<Expression> childExprs = Lists.newArrayList(expr, getTimeUnitExpr(TimeUnit.MILLISECOND), getMultiplierExpr(multiplier));
        return new CeilTimestampExpression(childExprs); 
    }
    
    public static Expression create(List<Expression> children) throws SQLException {
        Expression firstChild = children.get(0);
        PDataType firstChildDataType = firstChild.getDataType();
        String timeUnit = (String)((LiteralExpression)children.get(1)).getValue();
        if(TimeUnit.MILLISECOND.toString().equalsIgnoreCase(timeUnit)) {
            return new CeilTimestampExpression(children);
        }
        // Coerce TIMESTAMP to DATE, as the nanos has no affect
        List<Expression> newChildren = Lists.newArrayListWithExpectedSize(children.size());
        newChildren.add(CoerceExpression.create(firstChild, firstChildDataType == PDataType.TIMESTAMP ? PDataType.DATE : PDataType.UNSIGNED_DATE));
        newChildren.addAll(children.subList(1, children.size()));
        return CeilDateExpression.create(newChildren);
    }
    
    /**
     * Creates a {@link CeilTimestampExpression} that uses {@link TimeUnit#MILLISECOND} 
     * as the time unit for rounding. 
     */
    public static CeilTimestampExpression create (Expression expr) throws SQLException {
        return create(expr, 1);
    }

    @Override
    protected PDataCodec getKeyRangeCodec(PDataType columnDataType) {
        return columnDataType == PDataType.TIMESTAMP 
                ? PDataType.DATE.getCodec() 
                : columnDataType == PDataType.UNSIGNED_TIMESTAMP 
                    ? PDataType.UNSIGNED_DATE.getCodec() 
                    : super.getKeyRangeCodec(columnDataType);
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (children.get(0).evaluate(tuple, ptr)) {
            ColumnModifier columnModifier = children.get(0).getColumnModifier();
            PDataType dataType = getDataType();
            int nanos = dataType.getNanos(ptr, columnModifier);
            if (nanos > 0) {
                long millis = dataType.getMillis(ptr, columnModifier); 
                Timestamp roundedTs = new Timestamp(millis + 1);
                byte[] byteValue = dataType.toBytes(roundedTs);
                ptr.set(byteValue);
            }
            return true; // for timestamp we only support rounding up the milliseconds.
        }
        return false;
    }   

}
