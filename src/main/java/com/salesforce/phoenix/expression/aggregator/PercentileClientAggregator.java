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

import com.salesforce.phoenix.expression.*;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ImmutableBytesPtr;

/**
 * Client side Aggregator for PERCENTILE_CONT aggregations
 * 
 * @author anoopsjohn
 * @since 1.2.1
 */
public class PercentileClientAggregator extends DistinctValueWithCountClientAggregator {

    private final List<Expression> exps;
    private BigDecimal cachedResult = null;

    public PercentileClientAggregator(List<Expression> exps) {
        this.exps = exps;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (cachedResult == null) {
            ColumnExpression columnExp = (ColumnExpression)exps.get(0);
            // Second exp will be a LiteralExpression of Boolean type indicating whether the ordering to
            // be ASC/DESC
            LiteralExpression isAscendingExpression = (LiteralExpression)exps.get(1);
            boolean isAscending = (Boolean)isAscendingExpression.getValue();

            // Third expression will be LiteralExpression
            LiteralExpression percentileExp = (LiteralExpression)exps.get(2);
            float p = ((Number)percentileExp.getValue()).floatValue();
            Entry<ImmutableBytesPtr, Integer>[] entries = getSortedValueVsCount(isAscending);
            float i = (p * this.totalCount) + 0.5F;
            long k = (long)i;
            float f = i - k;
            ImmutableBytesPtr pi1 = null;
            ImmutableBytesPtr pi2 = null;
            long distinctCountsSum = 0;
            for (Entry<ImmutableBytesPtr, Integer> entry : entries) {
                if (pi1 != null) {
                    pi2 = entry.getKey();
                    break;
                }
                distinctCountsSum += entry.getValue();
                if (distinctCountsSum == k) {
                    pi1 = entry.getKey();
                } else if (distinctCountsSum > k) {
                    pi1 = pi2 = entry.getKey();
                    break;
                }
            }

            double result = 0.0;
            Number n1 = (Number)columnExp.getDataType().toObject(pi1);
            if (pi2 == null || pi1 == pi2) {
                result = n1.doubleValue();
            } else {
                Number n2 = (Number)columnExp.getDataType().toObject(pi2);
                result = (n1.doubleValue() * (1.0F - f)) + (n2.doubleValue() * f);
            }
            this.cachedResult = new BigDecimal(result);
        }
        if (buffer == null) {
            initBuffer();
        }
        buffer = PDataType.DECIMAL.toBytes(this.cachedResult);
        ptr.set(buffer);
        return true;
    }

    @Override
    protected int getBufferLength() {
        return PDataType.DECIMAL.getByteSize();
    }
    
    @Override
    public void reset() {
        super.reset();
        this.cachedResult = null;
    }
}
