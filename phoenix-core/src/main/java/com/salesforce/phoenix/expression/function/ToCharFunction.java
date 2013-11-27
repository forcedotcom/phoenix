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

import java.io.*;
import java.sql.SQLException;
import java.text.Format;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Preconditions;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Implementation of the TO_CHAR(&lt;date&gt;/&lt;number&gt;,[&lt;format-string&gt;] built-in function.
 * The first argument must be of type DATE or TIME or TIMESTAMP or DECIMAL or INTEGER, and the second argument must be a constant string. 
 *
 * @author jtaylor
 * @since 0.1
 */
@BuiltInFunction(name=ToCharFunction.NAME, nodeClass=ToCharParseNode.class, args={
    @Argument(allowedTypes={PDataType.TIMESTAMP, PDataType.DECIMAL}),
    @Argument(allowedTypes={PDataType.VARCHAR},isConstant=true,defaultValue="null") } )
public class ToCharFunction extends ScalarFunction {
    public static final String NAME = "TO_CHAR";
    private String formatString;
    private Format formatter;
    private FunctionArgumentType type;
    
    public ToCharFunction() {
    }

    public ToCharFunction(List<Expression> children, FunctionArgumentType type, String formatString, Format formatter) throws SQLException {
        super(children.subList(0, 1));
        Preconditions.checkNotNull(formatString);
        Preconditions.checkNotNull(formatter);
        Preconditions.checkNotNull(type);
        this.type = type;
        this.formatString = formatString;
        this.formatter = formatter;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + formatString.hashCode();
        result = prime * result + getExpression().hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ToCharFunction other = (ToCharFunction)obj;
        if (!getExpression().equals(other.getExpression())) return false;
        if (!formatString.equals(other.formatString)) return false;
        return true;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression expression = getExpression();
        if (!expression.evaluate(tuple, ptr)) {
            return false;
        }
        PDataType type = expression.getDataType();
        Object value = formatter.format(type.toObject(ptr, expression.getColumnModifier()));
        byte[] b = getDataType().toBytes(value);
        ptr.set(b);
        return true;
     }

    @Override
    public PDataType getDataType() {
        return PDataType.VARCHAR;
    }

    @Override
    public boolean isNullable() {
        return getExpression().isNullable();
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        formatString = WritableUtils.readString(input);
        type = WritableUtils.readEnum(input, FunctionArgumentType.class);
        formatter = type.getFormatter(formatString);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeString(output, formatString);
        WritableUtils.writeEnum(output, type);
    }

    private Expression getExpression() {
        return children.get(0);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
