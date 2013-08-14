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

import java.math.BigDecimal;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.expression.aggregator.*;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Built-in function for SUM aggregation function.
 *
 * @author jtaylor
 * @since 0.1
 */
@BuiltInFunction(name=SumAggregateFunction.NAME, nodeClass=SumAggregateParseNode.class, args= {@Argument(allowedTypes={PDataType.DECIMAL})} )
public class SumAggregateFunction extends DelegateConstantToCountAggregateFunction {
    public static final String NAME = "SUM";
    
    public SumAggregateFunction() {
    }
    
    // TODO: remove when not required at built-in func register time
    public SumAggregateFunction(List<Expression> childExpressions){
        super(childExpressions, null);
    }
    
    public SumAggregateFunction(List<Expression> childExpressions, CountAggregateFunction delegate){
        super(childExpressions, delegate);
    }
    
    @Override
    public Aggregator newServerAggregator() {
        final PDataType type = getAggregatorExpression().getDataType();
        ColumnModifier columnModifier = getAggregatorExpression().getColumnModifier();
        switch( type ) {
            case DECIMAL:
                return new DecimalSumAggregator(columnModifier);
            case UNSIGNED_DOUBLE:
            case UNSIGNED_FLOAT:
            case DOUBLE:
            case FLOAT:
                return new DoubleSumAggregator(columnModifier) {
                    @Override
                    protected PDataType getInputDataType() {
                        return type;
                    }
                };
            default:
                return new NumberSumAggregator(columnModifier) {
                    @Override
                    protected PDataType getInputDataType() {
                        return type;
                    }
                };
        }
    }
    
    @Override
    public Aggregator newClientAggregator() {
        switch( getDataType() ) {
            case DECIMAL:
                // On the client, we'll always aggregate over non modified column values,
                // because we always get them back from the server in their non modified
                // form.
                return new DecimalSumAggregator(null);
            case LONG:
                return new LongSumAggregator(null);
            case DOUBLE:
                return new DoubleSumAggregator(null);
            default:
                throw new IllegalStateException("Unexpected SUM type: " + getDataType());
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!super.evaluate(tuple, ptr)) {
            return false;
        }
        if (isConstantExpression()) {
            PDataType type = getDataType();
            Object constantValue = ((LiteralExpression)children.get(0)).getValue();
            if (type == PDataType.DECIMAL) {
                BigDecimal value = ((BigDecimal)constantValue).multiply((BigDecimal)PDataType.DECIMAL.toObject(ptr, PDataType.LONG));
                ptr.set(PDataType.DECIMAL.toBytes(value));
            } else {
                long constantLongValue = ((Number)constantValue).longValue();
                long value = constantLongValue * type.getCodec().decodeLong(ptr, null);
                ptr.set(new byte[type.getByteSize()]);
                type.getCodec().encodeLong(value, ptr);
            }
        }
        return true;
    }

    @Override
    public PDataType getDataType() {
        switch(super.getDataType()) {
        case DECIMAL:
            return PDataType.DECIMAL;
        case UNSIGNED_FLOAT:
        case UNSIGNED_DOUBLE:
        case FLOAT:
        case DOUBLE:
            return PDataType.DOUBLE;
        default:
            return PDataType.LONG;
        }
    }

    @Override
    public String getName() {
        return NAME;
    }
}
