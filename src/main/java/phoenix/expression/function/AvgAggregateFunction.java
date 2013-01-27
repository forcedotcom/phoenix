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
package phoenix.expression.function;

import java.math.BigDecimal;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import phoenix.expression.Expression;
import phoenix.expression.LiteralExpression;
import phoenix.parse.*;
import phoenix.parse.FunctionParseNode.Argument;
import phoenix.parse.FunctionParseNode.BuiltInFunction;
import phoenix.schema.PDataType;
import phoenix.schema.PDataType.LongNative;
import phoenix.schema.tuple.Tuple;

@BuiltInFunction(name=AvgAggregateFunction.NAME, nodeClass=AvgAggregateParseNode.class, args= {@Argument(allowedTypes={PDataType.DECIMAL})} )
public class AvgAggregateFunction extends CompositeAggregateFunction {
    public static final String NAME = "AVG";
    private final CountAggregateFunction countFunc;
    private final SumAggregateFunction sumFunc;

    // TODO: remove when not required at built-in func register time
    public AvgAggregateFunction(List<Expression> children) {
        super(children);
        this.countFunc = null;
        this.sumFunc = null;
    }

    public AvgAggregateFunction(List<Expression> children, CountAggregateFunction countFunc, SumAggregateFunction sumFunc) {
        super(children);
        this.countFunc = countFunc;
        this.sumFunc = sumFunc;
    }

    @Override
    public PDataType getDataType() {
        return PDataType.DECIMAL;
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        LongNative longNative = LongNative.getInstance();
        if (!countFunc.evaluate(tuple, ptr)) {
            return false;
        }
        long count = longNative.toLong(ptr);
        if (count == 0) {
            return false;
        }
        
        // Normal case where a column reference was used as the argumenn to AVG
        if (!countFunc.isConstantExpression()) {
            sumFunc.evaluate(tuple, ptr);
            BigDecimal sum = (BigDecimal)PDataType.DECIMAL.toObject(ptr, sumFunc.getDataType());
            // For the final column projection, we divide the sum by the count, both coerced to BigDecimal.
            // TODO: base the precision on column metadata instead of constant
            BigDecimal avg = sum.divide(BigDecimal.valueOf(count), PDataType.DEFAULT_MATH_CONTEXT);
            ptr.set(PDataType.DECIMAL.toBytes(avg));
            return true;
        }
        Object value = ((LiteralExpression)countFunc.getChildren().get(0)).getValue();
        ptr.set(PDataType.DECIMAL.toBytes(value));
        return true;
    }

    @Override
    public boolean isNullable() {
        return sumFunc != null && sumFunc.isNullable();
    }
    
    @Override
    public String getName() {
        return NAME;
    }
}
