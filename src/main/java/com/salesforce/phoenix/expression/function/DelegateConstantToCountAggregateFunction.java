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

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Base class for non composite aggregation functions that optimize aggregation by
 * delegating to {@link CountAggregateFunction} when the child expression is a 
 * constant.
 *
 * @author jtaylor
 * @since 0.1
 */
abstract public class DelegateConstantToCountAggregateFunction extends SingleAggregateFunction {
    private static final ImmutableBytesWritable ZERO = new ImmutableBytesWritable(PDataType.LONG.toBytes(0L));
    private CountAggregateFunction delegate;
    
    public DelegateConstantToCountAggregateFunction() {
    }
    
    public DelegateConstantToCountAggregateFunction(List<Expression> childExpressions, CountAggregateFunction delegate) {
        super(childExpressions);
        // Using a delegate here causes us to optimize the number of aggregators
        // by sharing the CountAggregator across functions. On the server side,
        // this will always be null, since if it had not been null on the client,
        // the function would not have been transfered over (the delegate would
        // have instead).
        this.delegate = delegate;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (delegate == null) {
            return super.evaluate(tuple, ptr);
        }
        delegate.evaluate(tuple, ptr);
        return PDataType.LONG.compareTo(ptr, ZERO) != 0;
    }


    @Override
    protected SingleAggregateFunction getDelegate() {
        return delegate != null ? delegate : super.getDelegate();
    }

}
