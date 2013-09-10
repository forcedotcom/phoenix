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

import java.util.List;

import com.salesforce.phoenix.expression.function.SingleAggregateFunction;
import com.salesforce.phoenix.schema.ValueBitSet;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.TupleUtil;



/**
 * 
 * Aggregators that execute on the client-side
 *
 * @author jtaylor
 * @since 0.1
 */
public class ClientAggregators extends Aggregators {
    private final ValueBitSet tempValueSet; 
  
    private static Aggregator[] getAggregators(List<SingleAggregateFunction> aggFuncs) {
        Aggregator[] aggregators = new Aggregator[aggFuncs.size()];
        for (int i = 0; i < aggregators.length; i++) {
            aggregators[i] = aggFuncs.get(i).getAggregator();
        }
        return aggregators;
    }
    
    public ClientAggregators(List<SingleAggregateFunction> functions, int minNullableIndex) {
        super(functions.toArray(new SingleAggregateFunction[functions.size()]), getAggregators(functions), minNullableIndex);
        this.tempValueSet = ValueBitSet.newInstance(schema);
    }
    
    @Override
    public void aggregate(Aggregator[] aggregators, Tuple result) {
        TupleUtil.getAggregateValue(result, ptr);
        tempValueSet.clear();
        tempValueSet.or(ptr);

        int i = 0;
        Boolean hasValue;
        int maxOffset = schema.iterator(ptr);
        while ((hasValue=schema.next(ptr, i, maxOffset, tempValueSet)) != null) {
            if (hasValue) {
                aggregators[i].aggregate(result, ptr);
            }
            i++;
        }
    }
    
    @Override
    public Aggregator[] newAggregators() {
        Aggregator[] aggregators = new Aggregator[functions.length];
        for (int i = 0; i < functions.length; i++) {
            aggregators[i] = functions[i].newClientAggregator();
        }
        return aggregators;
    }
    
}
