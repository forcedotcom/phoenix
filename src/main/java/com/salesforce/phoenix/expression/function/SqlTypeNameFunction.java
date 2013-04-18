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

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.schema.IllegalDataException;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ByteUtil;


/**
 * 
 * Function used to get the SQL type name from the SQL type integer.
 * Usage:
 * SqlTypeName(12)
 * will return 'VARCHAR' based on {@link java.sql.Types#VARCHAR} being 12
 * 
 * @author jtaylor
 * @since 0.1
 */
@BuiltInFunction(name=SqlTypeNameFunction.NAME, args= {
    @Argument(allowedTypes=PDataType.INTEGER)} )
public class SqlTypeNameFunction extends ScalarFunction {
    public static final String NAME = "SqlTypeName";

    public SqlTypeNameFunction() {
    }
    
    public SqlTypeNameFunction(List<Expression> children) throws SQLException {
        super(children);
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression child = children.get(0);
        if (!child.evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return true;
        }
        int sqlType = child.getDataType().getCodec().decodeInt(ptr, child.getColumnModifier());
        try {
            byte[] sqlTypeNameBytes = PDataType.fromSqlType(sqlType).getSqlTypeNameBytes();
            ptr.set(sqlTypeNameBytes);
        } catch (IllegalDataException e) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
        }
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDataType.VARCHAR;
    }
    
    @Override
    public String getName() {
        return NAME;
    }
}
