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
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.*;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ImmutableBytesPtr;

/**
 * Client side Aggregator for PERCENT_RANK aggregations
 * 
 * @author anoopsjohn
 * @since 1.2.1
 */
public class PercentRankClientAggregator extends DistinctValueWithCountClientAggregator {

    private List<Expression> exps = null;

    public PercentRankClientAggregator(List<Expression> exps) {
        this.exps = exps;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        ColumnExpression columnExp = (ColumnExpression)exps.get(0);
        // Second exp will be a LiteralExpression of Boolean type indicating whether the ordering to
        // be ASC/DESC
        LiteralExpression isAscendingExpression = (LiteralExpression)exps.get(1);
        boolean isAscending = (Boolean)isAscendingExpression.getValue();

        // Third expression will be LiteralExpression
        LiteralExpression valueExp = (LiteralExpression)exps.get(2);
        // To sort the valueVsCount
        NavigableMap<ImmutableBytesPtr, Integer> sortedMap = new TreeMap<ImmutableBytesPtr, Integer>(valueVsCount);
        if (!isAscending) {
            sortedMap = sortedMap.descendingMap();
        }

        long distinctCountsSum = 0;
        for (Entry<ImmutableBytesPtr, Integer> entry : sortedMap.entrySet()) {
            Object value = valueExp.getValue();
            Object colValue = columnExp.getDataType().toObject(entry.getKey());
            int compareResult = columnExp.getDataType().compareTo(colValue, value, valueExp.getDataType());
            boolean done = isAscending ? compareResult > 0 : compareResult <= 0;
            if (done) break;
            distinctCountsSum += entry.getValue();
        }

        float result = (float)distinctCountsSum / totalCount;
        if (buffer == null) {
            initBuffer();
        }
        buffer = PDataType.DECIMAL.toBytes(new BigDecimal(result));
        ptr.set(buffer);
        return true;
    }

    @Override
    protected int getBufferLength() {
        return PDataType.DECIMAL.getByteSize();
    }
}
