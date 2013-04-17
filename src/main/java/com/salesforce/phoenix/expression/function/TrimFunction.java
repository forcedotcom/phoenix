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

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.StringUtil;


/**
 * Implementation of the Trim(<string>) build-in function. It removes from both end of <string>
 * space character and other function bytes in single byte utf8 characters set.
 * 
 * @author zhuang
 * @since 0.1
 */
@BuiltInFunction(name=TrimFunction.NAME, args={
    @Argument(allowedTypes={PDataType.VARCHAR})} )
public class TrimFunction extends ScalarFunction {
    public static final String NAME = "TRIM";

    private Integer byteSize;

    public TrimFunction() { }

    public TrimFunction(List<Expression> children) throws SQLException {
        super(children);
        if (getStringExpression().getDataType().isFixedWidth()) {
            byteSize = getStringExpression().getByteSize();
        }
    }

    private Expression getStringExpression() {
        return children.get(0);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getStringExpression().evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
        }
        byte[] string = ptr.get();
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        
        ColumnModifier columnModifier = getStringExpression().getColumnModifier();
        if (columnModifier != null) {
            string = columnModifier.apply(string, null, offset, length);
        }
        
        try {
            int end = StringUtil.getFirstNonBlankCharIdxFromEnd(string, offset, length);
            if (end == offset - 1) {
                ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
                return true; 
            }
            int head = StringUtil.getFirstNonBlankCharIdxFromStart(string, offset, length);
            ptr.set(string, head, end - head + 1);
            return true;
        } catch (UnsupportedEncodingException e) {
            return false;
        }
    }

    @Override
    public Integer getByteSize() {
        return byteSize;
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
