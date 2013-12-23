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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.function.SingleAggregateFunction;
import com.salesforce.phoenix.schema.KeyValueSchema;
import com.salesforce.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import com.salesforce.phoenix.schema.ValueBitSet;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.SizedUtil;


/**
 * 
 * Represents an ordered list of Aggregators
 *
 * @author jtaylor
 * @since 0.1
 */
abstract public class Aggregators {
    protected final int size;
    protected final KeyValueSchema schema;
    protected final ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    protected final ValueBitSet valueSet;
    protected final Aggregator[] aggregators;
    protected final SingleAggregateFunction[] functions;
    
    public int getSize() {
        return size;
    }
    
    public Aggregators(SingleAggregateFunction[] functions, Aggregator[] aggregators, int minNullableIndex) {
        this.functions = functions;
        this.aggregators = aggregators;
        this.size = calculateSize(aggregators);
        this.schema = newValueSchema(aggregators, minNullableIndex);
        this.valueSet = ValueBitSet.newInstance(schema);
    }
    
    public KeyValueSchema getValueSchema() {
        return schema;
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder(this.getClass().getName() + " [" + functions.length + "]:");
        for (int i = 0; i < functions.length; i++) {
            SingleAggregateFunction function = functions[i];
            buf.append("\t" + i + ") " + function );
        }
        return buf.toString();
    }
    
    /**
     * Return the aggregate functions
     */
    public SingleAggregateFunction[] getFunctions() {
        return functions;
    }
    
    /**
     * Aggregate over aggregators
     * @param result the single row Result from scan iteration
     */
    abstract public void aggregate(Aggregator[] aggregators, Tuple result);

    protected static int calculateSize(Aggregator[] aggregators) {
        
        int size = SizedUtil.ARRAY_SIZE /*aggregators[]*/  + (SizedUtil.POINTER_SIZE  * aggregators.length);
        for (Aggregator aggregator : aggregators) {
            size += aggregator.getSize();
        }
        return size;
    }
    
    /**
     * Get the ValueSchema for the Aggregators
     */
    private static KeyValueSchema newValueSchema(Aggregator[] aggregators, int minNullableIndex) {
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(minNullableIndex);
        for (Aggregator aggregator : aggregators) {
            builder.addField(aggregator);
        }
        return builder.build();
    }

    /**
     * @return byte representation of the ValueSchema
     */
    public byte[] toBytes(Aggregator[] aggregators) {
        return schema.toBytes(aggregators, valueSet, ptr);
    }
    
    public int getAggregatorCount() {
        return aggregators.length;
    }

    public Aggregator[] getAggregators() {
        return aggregators;
    }
    
    abstract public Aggregator[] newAggregators();
    
    public void reset(Aggregator[] aggregators) {
        for (Aggregator aggregator : aggregators) {
            aggregator.reset();
        }
    }
    
    protected Aggregator getAggregator(int position) {
        return aggregators[position];
    }
}
