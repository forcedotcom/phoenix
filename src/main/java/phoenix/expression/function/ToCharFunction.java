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

import java.io.*;
import java.sql.SQLException;
import java.text.Format;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import phoenix.expression.Expression;
import phoenix.parse.FunctionParseNode.Argument;
import phoenix.parse.FunctionParseNode.BuiltInFunction;
import phoenix.parse.*;
import phoenix.schema.PDataType;
import phoenix.schema.tuple.Tuple;
import phoenix.util.DateUtil;

/**
 * 
 * Implementation of the TO_CHAR(<date>,[<format-string>] built-in function.
 * The first argument must be of type DATE or TIME and the second argument must be a constant string. 
 *
 * @author jtaylor
 * @since 0.1
 */
@BuiltInFunction(name=ToCharFunction.NAME, nodeClass=ToCharParseNode.class, args={
    @Argument(allowedTypes={PDataType.DATE,PDataType.TIME}),
    @Argument(allowedTypes={PDataType.VARCHAR},isConstant=true,defaultValue="null") } )
public class ToCharFunction extends ScalarFunction {
    public static final String NAME = "TO_CHAR";
    private Format dateFormatter;
    private String dateFormat;

    public ToCharFunction() {
    }

    public ToCharFunction(List<Expression> children, String dateFormat, Format dateFormatter) throws SQLException {
        super(children.subList(0, 1));
        this.dateFormat = dateFormat;
        this.dateFormatter = dateFormatter;
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
        ToCharFunction other = (ToCharFunction)obj;
        if (!getExpression().equals(other.getExpression())) return false;
        if (!dateFormat.equals(other.dateFormat)) return false;
        return true;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getExpression().evaluate(tuple, ptr)) {
            return false;
        }
        PDataType type = getExpression().getDataType();
        Object value = dateFormatter.format(type.toObject(ptr));
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
        dateFormat = WritableUtils.readString(input);
        dateFormatter = DateUtil.getDateFormatter(dateFormat);
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
