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

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Function used to provide an alternative value when the first argument is null.
 * Usage:
 * COALESCE(expr1,expr2)
 * If expr1 is not null, then it is returned, otherwise expr2 is returned. 
 *
 * TODO: better bind parameter type matching, since arg2 must be coercible
 * to arg1. consider allowing a common base type?
 * @author jtaylor
 * @since 0.1
 */
@BuiltInFunction(name=CoalesceFunction.NAME, args= {
    @Argument(),
    @Argument()} )
public class CoalesceFunction extends ScalarFunction {
    public static final String NAME = "COALESCE";

    public CoalesceFunction() {
    }

    public CoalesceFunction(List<Expression> children) throws SQLException {
        super(children);
        if (!children.get(1).getDataType().isCoercibleTo(children.get(0).getDataType())) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CONVERT_TYPE)
                .setMessage(getName() + " expected " + children.get(0).getDataType() + ", but got " + children.get(1).getDataType())
                .build().buildException();
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (children.get(0).evaluate(tuple, ptr)) {
            return true;
        }
        return children.get(1).evaluate(tuple, ptr);
    }

    @Override
    public PDataType getDataType() {
        return children.get(0).getDataType();
    }

    @Override
    public Integer getMaxLength() {
        Integer maxLength1 = children.get(0).getMaxLength();
        if (maxLength1 != null) {
            Integer maxLength2 = children.get(1).getMaxLength();
            if (maxLength2 != null) {
                return maxLength1 > maxLength2 ? maxLength1 : maxLength2;
            }
        }
        return null;
    }

    @Override
    public boolean isNullable() {
        return children.get(0).isNullable() && children.get(1).isNullable();
    }

    @Override
    public String getName() {
        return NAME;
    }
}
