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

import java.math.BigDecimal;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ImmutableBytesPtr;

/**
 * 
 * @author anoopsjohn
 * @since 1.2.1
 */
public abstract class BaseStddevAggregator extends DistinctValueWithCountClientAggregator {

    protected Expression stdDevColExp;
    private BigDecimal cachedResult = null;

    public BaseStddevAggregator(List<Expression> exps) {
        this.stdDevColExp = exps.get(0);
    }

    @Override
    protected int getBufferLength() {
        return PDataType.DECIMAL.getByteSize();
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (cachedResult == null) {
            double ssd = sumSquaredDeviation();
            double result = Math.sqrt(ssd / getDataPointsCount());
            cachedResult = new BigDecimal(result);
        }
        if (buffer == null) {
            initBuffer();
        }
        buffer = PDataType.DECIMAL.toBytes(cachedResult);
        ptr.set(buffer);
        return true;
    }
    
    protected abstract long getDataPointsCount();
    
    private double sumSquaredDeviation() {
        double m = mean();
        double result = 0.0;
        for (Entry<ImmutableBytesPtr, Integer> entry : valueVsCount.entrySet()) {
            double colValue = (Double)PDataType.DOUBLE.toObject(entry.getKey(), this.stdDevColExp.getDataType());
            double delta = colValue - m;
            result += (delta * delta) * entry.getValue();
        }
        return result;
    }

    private double mean() {
        double sum = 0.0;
        for (Entry<ImmutableBytesPtr, Integer> entry : valueVsCount.entrySet()) {
            double colValue = (Double)PDataType.DOUBLE.toObject(entry.getKey(), this.stdDevColExp.getDataType());
            sum += colValue * entry.getValue();
        }
        return sum / totalCount;
    }
    
    @Override
    public void reset() {
        super.reset();
        this.cachedResult = null;
    }
}
