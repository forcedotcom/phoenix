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

import java.util.Arrays;
import java.util.List;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.expression.aggregator.*;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.SchemaUtil;


/**
 * 
 * Built-in function for COUNT(<expression>) aggregate function,
 * for example COUNT(foo), COUNT(1), COUNT(*)
 *
 * @author jtaylor
 * @since 0.1
 */
@BuiltInFunction(name=CountAggregateFunction.NAME, args= {@Argument()} )
public class CountAggregateFunction extends SingleAggregateFunction {
    public static final String NAME = "COUNT";
    public static final List<Expression> STAR = Arrays.<Expression>asList(LiteralExpression.newConstant(1));
    public static final String NORMALIZED_NAME = SchemaUtil.normalizeIdentifier(NAME);
    
    public CountAggregateFunction() {
    }
    
    public CountAggregateFunction(List<Expression> childExpressions) {
        super(childExpressions);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        CountAggregateFunction other = (CountAggregateFunction)obj;
        return (isConstantExpression() && other.isConstantExpression()) || children.equals(other.getChildren());
    }

    @Override
    public int hashCode() {
        return isConstantExpression() ? 0 : super.hashCode();
    }

    /**
     * The COUNT function never returns null
     */
    @Override
    public boolean isNullable() {
        return false;
    }
    
    @Override
    public PDataType getDataType() {
        return PDataType.LONG;
    }

    @Override 
    public Aggregator newClientAggregator() {
        // Since COUNT can never be null, ensure the aggregator is not nullable.
        // This allows COUNT(*) to return 0 with the initial state of ClientAggregators
        // when no rows are returned. 
        return new LongSumAggregator(null) {
            @Override
            public boolean isNullable() {
                return false;
            }
        };
    }
    
    @Override 
    public Aggregator newServerAggregator() {
        return new CountAggregator();
    }
    
    @Override
    public String getName() {
        return NAME;
    }
}
