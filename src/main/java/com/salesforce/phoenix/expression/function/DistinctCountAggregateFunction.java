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

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.aggregator.Aggregator;
import com.salesforce.phoenix.expression.aggregator.DistinctCountServerAggregator;
import com.salesforce.phoenix.expression.aggregator.DistinctCountClientAggregator;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.SchemaUtil;


/**
 * 
 * Built-in function for COUNT(distinct <expression>) aggregate function,
 *
 * @author anoopsjohn
 * @since 1.2.1
 */
@BuiltInFunction(name=DistinctCountAggregateFunction.NAME, args= {@Argument()} )
public class DistinctCountAggregateFunction extends SingleAggregateFunction {
    public static final String NAME = "count_distinct";
    public static final String NORMALIZED_NAME = SchemaUtil.normalizeIdentifier(NAME);

    public DistinctCountAggregateFunction() {
    }
    
    public DistinctCountAggregateFunction(List<Expression> childExpressions) {
        super(childExpressions);
        assert childExpressions.size()==1;
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
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        DistinctCountAggregateFunction other = (DistinctCountAggregateFunction)obj;
        return (isConstantExpression() && other.isConstantExpression()) || children.equals(other.getChildren());
    }

    @Override
    public PDataType getDataType() {
        return PDataType.VARBINARY;
    }

    @Override 
    public Aggregator newClientAggregator() {
        return new DistinctCountClientAggregator();
    }
    
    @Override 
    public Aggregator newServerAggregator() {
        return new DistinctCountServerAggregator(getChildren());
    }
    
    @Override
    public String getName() {
        return NAME;
    }
}
