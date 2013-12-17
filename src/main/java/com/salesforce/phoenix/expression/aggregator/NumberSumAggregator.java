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

import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.SizedUtil;


/**
 * 
 * Aggregator that sums integral number values
 *
 * @author jtaylor
 * @since 0.1
 */
abstract public class NumberSumAggregator extends BaseAggregator {
    private long sum = 0;
    private byte[] buffer;
    
    public NumberSumAggregator(ColumnModifier columnModifier) {
        super(columnModifier);
    }

    public NumberSumAggregator(ColumnModifier columnModifier, ImmutableBytesWritable ptr) {
        this(columnModifier);
        if (ptr != null) {
            sum = PDataType.LONG.getCodec().decodeLong(ptr, columnModifier);
        }
    }

    public long getSum() {
        return sum;
    }
    
    abstract protected PDataType getInputDataType();
    
    private int getBufferLength() {
        return getDataType().getByteSize();
    }
    
    private void initBuffer() {
        buffer = new byte[getBufferLength()];
    }
        
    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        // Get either IntNative or LongNative depending on input type
        long value = getInputDataType().getCodec().decodeLong(ptr, columnModifier);
        sum += value;
        if (buffer == null) {
            initBuffer();
        }
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (buffer == null) {
            if (isNullable()) {
                return false;
            }
            initBuffer();
        }
        ptr.set(buffer);
        getDataType().getCodec().encodeLong(sum, ptr);
        return true;
    }
    
    @Override
    public final PDataType getDataType() {
        return PDataType.LONG;
    }
    
    @Override
    public void reset() {
        sum = 0;
        buffer = null;
        super.reset();
    }

    @Override
    public String toString() {
        return "SUM [sum=" + sum + "]";
    }

    @Override
    public int getSize() {
        return super.getSize() + SizedUtil.LONG_SIZE + SizedUtil.ARRAY_SIZE + getBufferLength();
    }
    
    @Override
    public void init(Aggregator clientAgg) {
        // TODO move into a new constrcutor
        // type safety on clientAgg
        ImmutableBytesWritable ptr = evalClientAggs(clientAgg);
        sum = getDataType().getCodec().decodeLong(ptr, columnModifier);
        if(buffer == null) {
            initBuffer();
        }       
    }
}
