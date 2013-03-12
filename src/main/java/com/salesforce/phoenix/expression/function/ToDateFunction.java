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
import java.text.ParseException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.DateUtil;


/**
 * 
 * Implementation of the TO_DATE(<string>,[<format-string>]) built-in function.
 * The second argument is optional and defaults to the phoenix.query.dateFormat value
 * from the HBase config. If present it must be a constant string.
 *
 * @author jtaylor
 * @since 0.1
 */
@BuiltInFunction(name=ToDateFunction.NAME, nodeClass=ToDateParseNode.class, args= {@Argument(allowedTypes={PDataType.VARCHAR}),@Argument(allowedTypes={PDataType.VARCHAR},isConstant=true,defaultValue="null")} )
public class ToDateFunction extends ScalarFunction {
    public static final String NAME = "TO_DATE";
    private Format dateParser;
    private String dateFormat;

    public ToDateFunction() {
    }

    public ToDateFunction(List<Expression> children, String dateFormat, Format dateParser) throws SQLException {
        super(children.subList(0, 1));
        this.dateFormat = dateFormat;
        this.dateParser = dateParser;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + dateFormat.hashCode();
        result = prime * result + getExpression().hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ToDateFunction other = (ToDateFunction)obj;
        if (!getExpression().equals(other.getExpression())) return false;
        if (!dateFormat.equals(other.dateFormat)) return false;
        return true;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) throws SQLException {
        if (!getExpression().evaluate(tuple, ptr) || ptr.getLength() == 0) {
            return false;
        }
        PDataType type = getExpression().getDataType();
        String dateStr = (String)type.toObject(ptr);
        try {
            Object value = dateParser.parseObject(dateStr);
            byte[] byteValue = getDataType().toBytes(value);
            ptr.set(byteValue);
            return true;
        } catch (ParseException e) {
            throw new IllegalStateException("to_date('" + dateStr + ")' did not match expected date format of '" + dateFormat + "'.");
        }
     }

    @Override
    public PDataType getDataType() {
        return PDataType.DATE;
    }

    @Override
    public boolean isNullable() {
        return getExpression().isNullable();
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        dateFormat = WritableUtils.readString(input);
        dateParser = DateUtil.getDateParser(dateFormat);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeString(output, dateFormat);
    }

    private Expression getExpression() {
        return children.get(0);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
