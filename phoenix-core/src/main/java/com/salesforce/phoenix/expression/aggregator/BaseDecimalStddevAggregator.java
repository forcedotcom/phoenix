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
package com.salesforce.phoenix.expression.aggregator;

import java.math.*;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;

import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.expression.ColumnExpression;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.*;
import com.salesforce.phoenix.util.BigDecimalUtil.Operation;

/**
 * 
 * @author anoopsjohn
 * @since 1.2.1
 */
public abstract class BaseDecimalStddevAggregator extends DistinctValueWithCountClientAggregator {

    private BigDecimal cachedResult = null;
    private int colPrecision;
    private int colScale;

    public BaseDecimalStddevAggregator(List<Expression> exps, ColumnModifier columnModifier) {
        super(columnModifier);
        ColumnExpression stdDevColExp = (ColumnExpression)exps.get(0);
        this.colPrecision = stdDevColExp.getMaxLength();
        this.colScale = stdDevColExp.getScale();
    }

    @Override
    protected int getBufferLength() {
        return PDataType.DECIMAL.getByteSize();
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (cachedResult == null) {
            BigDecimal ssd = sumSquaredDeviation();
            ssd = ssd.divide(new BigDecimal(getDataPointsCount()), PDataType.DEFAULT_MATH_CONTEXT);
            // Calculate the precision for the stddev result.
            // There are totalCount #Decimal values for which we are calculating the stddev
            // The resultant precision depends on precision and scale of all these values. (See
            // BigDecimalUtil.getResultPrecisionScale)
            // As of now we are not using the actual precision and scale of individual values but just using the table
            // column's max length(precision) and scale for each of the values.
            int resultPrecision = colPrecision;
            for (int i = 1; i < this.totalCount; i++) {
                // Max precision that we can support is 38 See PDataType.MAX_PRECISION
                if (resultPrecision >= PDataType.MAX_PRECISION) break;
                Pair<Integer, Integer> precisionScale = BigDecimalUtil.getResultPrecisionScale(this.colPrecision,
                        this.colScale, this.colPrecision, this.colScale, Operation.OTHERS);
                resultPrecision = precisionScale.getFirst();
            }
            cachedResult = new BigDecimal(Math.sqrt(ssd.doubleValue()), new MathContext(resultPrecision,
                    RoundingMode.HALF_UP));
            cachedResult.setScale(this.colScale, RoundingMode.HALF_UP);
        }
        if (buffer == null) {
            initBuffer();
        }
        buffer = PDataType.DECIMAL.toBytes(cachedResult);
        ptr.set(buffer);
        return true;
    }

    protected abstract long getDataPointsCount();

    private BigDecimal sumSquaredDeviation() {
        BigDecimal m = mean();
        BigDecimal result = BigDecimal.ZERO;
        for (Entry<ImmutableBytesPtr, Integer> entry : valueVsCount.entrySet()) {
            BigDecimal colValue = (BigDecimal)PDataType.DECIMAL.toObject(entry.getKey());
            BigDecimal delta = colValue.subtract(m);
            result = result.add(delta.multiply(delta).multiply(new BigDecimal(entry.getValue())));
        }
        return result;
    }

    private BigDecimal mean() {
        BigDecimal sum = BigDecimal.ZERO;
        for (Entry<ImmutableBytesPtr, Integer> entry : valueVsCount.entrySet()) {
            BigDecimal colValue = (BigDecimal)PDataType.DECIMAL.toObject(entry.getKey());
            sum = sum.add(colValue.multiply(new BigDecimal(entry.getValue())));
        }
        return sum.divide(new BigDecimal(totalCount), PDataType.DEFAULT_MATH_CONTEXT);
    }

    @Override
    public void reset() {
        super.reset();
        this.cachedResult = null;
    }
}
