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
package com.salesforce.phoenix.iterate;

import static com.salesforce.phoenix.query.QueryConstants.*;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.aggregator.Aggregator;
import com.salesforce.phoenix.expression.aggregator.Aggregators;
import com.salesforce.phoenix.schema.tuple.SingleKeyValueTuple;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.KeyValueUtil;
import com.salesforce.phoenix.util.TupleUtil;



/**
 * 
 * Result scanner that aggregates the row count value for rows with duplicate keys.
 * The rows from the backing result iterator must be in key sorted order.  For example,
 * given the following input:
 *   a  1
 *   a  2
 *   b  1
 *   b  3
 *   c  1
 * the following will be output:
 *   a  3
 *   b  4
 *   c  1
 *
 * @author jtaylor
 * @since 0.1
 */
public class GroupedAggregatingResultIterator implements AggregatingResultIterator {
    private final ImmutableBytesWritable tempPtr = new ImmutableBytesWritable();
    private final PeekingResultIterator resultIterator;
    protected final Aggregators aggregators;
    
    public GroupedAggregatingResultIterator( PeekingResultIterator resultIterator, Aggregators aggregators) {
        if (resultIterator == null) throw new NullPointerException();
        if (aggregators == null) throw new NullPointerException();
        this.resultIterator = resultIterator;
        this.aggregators = aggregators;
    }
    
    @Override
    public Tuple next() throws SQLException {
        Tuple result = resultIterator.next();
        if (result == null) {
            return null;
        }
        Aggregator[] rowAggregators = aggregators.getAggregators();
        aggregators.reset(rowAggregators);
        while (true) {
            aggregators.aggregate(rowAggregators, result);
            Tuple nextResult = resultIterator.peek();
            if (nextResult == null || !TupleUtil.equals(result, nextResult, tempPtr)) {
                break;
            }
            result = resultIterator.next();
        }
        
        byte[] value = aggregators.toBytes(rowAggregators);
        result.getKey(tempPtr);
        return new SingleKeyValueTuple(KeyValueUtil.newKeyValue(tempPtr, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, AGG_TIMESTAMP, value, 0, value.length));
    }
    
    @Override
    public void close() throws SQLException {
        resultIterator.close();
    }
    
    @Override
    public void aggregate(Tuple result) {
        Aggregator[] rowAggregators = aggregators.getAggregators();
        aggregators.reset(rowAggregators);
        aggregators.aggregate(rowAggregators, result);
    }

    @Override
    public void explain(List<String> planSteps) {
        resultIterator.explain(planSteps);
    }
}
