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
package phoenix.expression;

import java.util.Map;

import phoenix.expression.function.*;

import com.google.common.collect.Maps;

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
    RowKey(RowKeyColumnExpression.class),
    KeyValue(KeyValueColumnExpression.class),
    LiteralValue(LiteralExpression.class),
    RoundFunction(RoundFunction.class),
    TruncFunction(TruncFunction.class),
    ToDateFunction(ToDateFunction.class),
    ToCharFunction(ToCharFunction.class),
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
    RTrimFunction(RTrimFunction.class);

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
