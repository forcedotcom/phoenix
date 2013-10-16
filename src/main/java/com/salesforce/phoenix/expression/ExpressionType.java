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
package com.salesforce.phoenix.expression;

import java.util.Map;

import com.google.common.collect.Maps;
import com.salesforce.phoenix.expression.function.CoalesceFunction;
import com.salesforce.phoenix.expression.function.CountAggregateFunction;
import com.salesforce.phoenix.expression.function.DistinctCountAggregateFunction;
import com.salesforce.phoenix.expression.function.IndexStateNameFunction;
import com.salesforce.phoenix.expression.function.LTrimFunction;
import com.salesforce.phoenix.expression.function.LengthFunction;
import com.salesforce.phoenix.expression.function.LowerFunction;
import com.salesforce.phoenix.expression.function.MD5Function;
import com.salesforce.phoenix.expression.function.MaxAggregateFunction;
import com.salesforce.phoenix.expression.function.MinAggregateFunction;
import com.salesforce.phoenix.expression.function.PercentRankAggregateFunction;
import com.salesforce.phoenix.expression.function.PercentileContAggregateFunction;
import com.salesforce.phoenix.expression.function.PercentileDiscAggregateFunction;
import com.salesforce.phoenix.expression.function.RTrimFunction;
import com.salesforce.phoenix.expression.function.RegexpReplaceFunction;
import com.salesforce.phoenix.expression.function.RegexpSubstrFunction;
import com.salesforce.phoenix.expression.function.ReverseFunction;
import com.salesforce.phoenix.expression.function.RoundFunction;
import com.salesforce.phoenix.expression.function.SqlTableType;
import com.salesforce.phoenix.expression.function.SqlTypeNameFunction;
import com.salesforce.phoenix.expression.function.StddevPopFunction;
import com.salesforce.phoenix.expression.function.StddevSampFunction;
import com.salesforce.phoenix.expression.function.SubstrFunction;
import com.salesforce.phoenix.expression.function.SumAggregateFunction;
import com.salesforce.phoenix.expression.function.ToCharFunction;
import com.salesforce.phoenix.expression.function.ToDateFunction;
import com.salesforce.phoenix.expression.function.ToNumberFunction;
import com.salesforce.phoenix.expression.function.TrimFunction;
import com.salesforce.phoenix.expression.function.TruncFunction;
import com.salesforce.phoenix.expression.function.UpperFunction;

/**
 * 
 * Enumeration of all Expression types that may be evaluated on the server-side.
 * Used during serialization and deserialization to pass Expression between client
 * and server.
 *
 * @author jtaylor
 * @since 0.1
 */
public enum ExpressionType {
    ReverseFunction(ReverseFunction.class),
    RowKey(RowKeyColumnExpression.class),
    KeyValue(KeyValueColumnExpression.class),
    LiteralValue(LiteralExpression.class),
    RoundFunction(RoundFunction.class),
    TruncFunction(TruncFunction.class),
    ToDateFunction(ToDateFunction.class),
    ToCharFunction(ToCharFunction.class),
    ToNumberFunction(ToNumberFunction.class),
    CoerceFunction(CoerceExpression.class),
    SubstrFunction(SubstrFunction.class),
    AndExpression(AndExpression.class),
    OrExpression(OrExpression.class),
    ComparisonExpression(ComparisonExpression.class),
    CountAggregateFunction(CountAggregateFunction.class),
    SumAggregateFunction(SumAggregateFunction.class),
    MinAggregateFunction(MinAggregateFunction.class),
    MaxAggregateFunction(MaxAggregateFunction.class),
    LikeExpression(LikeExpression.class),
    NotExpression(NotExpression.class),
    CaseExpression(CaseExpression.class),
    InListExpression(InListExpression.class),
    IsNullExpression(IsNullExpression.class),
    LongSubtractExpression(LongSubtractExpression.class),
    DateSubtractExpression(DateSubtractExpression.class),
    DecimalSubtractExpression(DecimalSubtractExpression.class),
    LongAddExpression(LongAddExpression.class),
    DecimalAddExpression(DecimalAddExpression.class),
    DateAddExpression(DateAddExpression.class),
    LongMultiplyExpression(LongMultiplyExpression.class),
    DecimalMultiplyExpression(DecimalMultiplyExpression.class),
    LongDivideExpression(LongDivideExpression.class),
    DecimalDivideExpression(DecimalDivideExpression.class),
    CoalesceFunction(CoalesceFunction.class),
    RegexpReplaceFunction(RegexpReplaceFunction.class),
    SQLTypeNameFunction(SqlTypeNameFunction.class),
    RegexpSubstrFunction(RegexpSubstrFunction.class),
    StringConcatExpression(StringConcatExpression.class),
    LengthFunction(LengthFunction.class),
    LTrimFunction(LTrimFunction.class),
    RTrimFunction(RTrimFunction.class),
    UpperFunction(UpperFunction.class),
    LowerFunction(LowerFunction.class),
    TrimFunction(TrimFunction.class),
    DistinctCountAggregateFunction(DistinctCountAggregateFunction.class),
    PercentileContAggregateFunction(PercentileContAggregateFunction.class),
    PercentRankAggregateFunction(PercentRankAggregateFunction.class),
    StddevPopFunction(StddevPopFunction.class),
    StddevSampFunction(StddevSampFunction.class),
    PercentileDiscAggregateFunction(PercentileDiscAggregateFunction.class),
    DoubleAddExpression(DoubleAddExpression.class),
    DoubleSubtractExpression(DoubleSubtractExpression.class),
    DoubleMultiplyExpression(DoubleMultiplyExpression.class),
    DoubleDivideExpression(DoubleDivideExpression.class),
    RowValueConstructorExpression(RowValueConstructorExpression.class),
    MD5Function(MD5Function.class),
    SqlTableType(SqlTableType.class),
    CeilingDecimalExpression(CeilingDecimalExpression.class),
    CeilingTimestampExpression(CeilingTimestampExpression.class),
    FloorDecimalExpression(FloorDecimalExpression.class),
    FloorTimestampExpression(FloorTimestampExpression.class),
    IndexKeyValue(IndexKeyValueColumnExpression.class),
    IndexStateName(IndexStateNameFunction.class),
    ProjectedColumnExpression(ProjectedColumnExpression.class);
    
    ExpressionType(Class<? extends Expression> clazz) {
        this.clazz = clazz;
    }

    public Class<? extends Expression> getExpressionClass() {
        return clazz;
    }

    private final Class<? extends Expression> clazz;

    private static final Map<Class<? extends Expression>,ExpressionType> classToEnumMap = Maps.newHashMapWithExpectedSize(3);
    static {
        for (ExpressionType type : ExpressionType.values()) {
            classToEnumMap.put(type.clazz, type);
        }
    }

    /**
     * Return the ExpressionType for a given Expression instance
     */
    public static ExpressionType valueOf(Expression expression) {
        ExpressionType type = classToEnumMap.get(expression.getClass());
        if (type == null) { // FIXME: this exception gets swallowed and retries happen
            throw new IllegalArgumentException("No ExpressionType for " + expression.getClass());
        }
        return type;
    }

    /**
     * Instantiates a DataAccessor based on its DataAccessorType
     */
    public Expression newInstance() {
        try {
            return clazz.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
