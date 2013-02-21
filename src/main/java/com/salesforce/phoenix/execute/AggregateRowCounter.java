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
package com.salesforce.phoenix.execute;


import java.sql.SQLException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.aggregator.Aggregators;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.schema.PDataType.LongNative;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.TupleUtil;


/**
 * 
 * Implementation of RowCounter for aggregation. Responsible for calculating the number of rows
 * that were scanned over from the Result returned for a aggregate query.
 *
 * @author jtaylor
 * @since 0.1
 */
public class AggregateRowCounter implements RowCounter {
    private final KeyValueSchema schema;
    private final static int ROW_COUNT_AGGREGATOR_INDEX = 0; // Our sorting algorithm guarantees this
    private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    
    /**
     * Construct an AggregateRowCounter that is used to short circuit scans with a rownum limit by
     * calculating the row count from aggregated query results.
     * @param aggregators aggregators
     */
    public AggregateRowCounter(Aggregators aggregators) {
        this.schema = aggregators.getValueSchema();
    }

    /**
     * Calculates the number of rows scanned as a result of calculating this aggregate row.
     * @param result a single row result returned from an aggregate query
     * @return the number of rows scanned
     */
    @Override
    public long calculate(Tuple result) throws SQLException {
        // Cannot use getValue here because aggregation hasn't been performed yet.
        // Instead, we access the raw value from the result set at the correct position.
        // We can use ValueBitSet.EMPTY_VALUE_BITSET, since we know that the row count
        // is not null and thus no null column values will be encountered.
        TupleUtil.getAggregateValue(result, ptr);
        schema.setAccessor(ptr, ROW_COUNT_AGGREGATOR_INDEX, ValueBitSet.EMPTY_VALUE_BITSET);
        return LongNative.getInstance().toLong(ptr.get(), ptr.getOffset(), ptr.getLength());
    }
}
