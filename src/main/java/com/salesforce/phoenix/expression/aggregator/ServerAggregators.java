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

import java.io.*;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.ExpressionType;
import com.salesforce.phoenix.expression.function.SingleAggregateFunction;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Aggregators that execute on the server-side
 *
 * @author jtaylor
 * @since 0.1
 */
public class ServerAggregators extends Aggregators {
    public static final ServerAggregators EMPTY_AGGREGATORS = new ServerAggregators(new SingleAggregateFunction[0], new Aggregator[0], new Expression[0], 0);
    private final Expression[] expressions;
    
    private ServerAggregators(SingleAggregateFunction[] functions, Aggregator[] aggregators, Expression[] expressions, int minNullableIndex) {
        super(functions, aggregators, minNullableIndex);
        if (aggregators.length != expressions.length) {
            throw new IllegalArgumentException("Number of aggregators (" + aggregators.length 
                    + ") must match the number of expressions (" + Arrays.toString(expressions) + ")");
        }
        this.expressions = expressions;
    }
    
    @Override
    public void aggregate(Aggregator[] aggregators, Tuple result) {
        for (int i = 0; i < expressions.length; i++) {
            if (expressions[i].evaluate(result, ptr)) {
                aggregators[i].aggregate(result, ptr);
            }
        }
    }
    
    /**
     * Serialize an Aggregator into a byte array
     * @param aggFuncs list of aggregator to serialize
     * @return serialized byte array respresentation of aggregator
     */
    public static byte[] serialize(List<SingleAggregateFunction> aggFuncs, int minNullableIndex) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            WritableUtils.writeVInt(output, minNullableIndex);
            WritableUtils.writeVInt(output, aggFuncs.size());
            for (int i = 0; i < aggFuncs.size(); i++) {
                SingleAggregateFunction aggFunc = aggFuncs.get(i);
                WritableUtils.writeVInt(output, ExpressionType.valueOf(aggFunc).ordinal());
                aggFunc.write(output);
            }
            return stream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Aggregator[] newAggregators() {
        Aggregator[] aggregators = new Aggregator[functions.length];
        for (int i = 0; i < functions.length; i++) {
            aggregators[i] = functions[i].newServerAggregator();
        }
        return aggregators;
    }
    
    /**
     * Deserialize aggregators from the serialized byte array representation
     * @param b byte array representation of a list of Aggregators
     * @return newly instantiated Aggregators instance
     */
    public static ServerAggregators deserialize(byte[] b) {
        if (b == null) {
            return ServerAggregators.EMPTY_AGGREGATORS;
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(b);
        try {
            DataInputStream input = new DataInputStream(stream);
            int minNullableIndex = WritableUtils.readVInt(input);
            int len = WritableUtils.readVInt(input);
            Aggregator[] aggregators = new Aggregator[len];
            Expression[] expressions = new Expression[len];
            SingleAggregateFunction[] functions = new SingleAggregateFunction[len];
            for (int i = 0; i < aggregators.length; i++) {
                SingleAggregateFunction aggFunc = (SingleAggregateFunction)ExpressionType.values()[WritableUtils.readVInt(input)].newInstance();
                aggFunc.readFields(input);
                functions[i] = aggFunc;
                aggregators[i] = aggFunc.getAggregator();
                expressions[i] = aggFunc.getAggregatorExpression();
            }
            return new ServerAggregators(functions, aggregators,expressions, minNullableIndex);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
