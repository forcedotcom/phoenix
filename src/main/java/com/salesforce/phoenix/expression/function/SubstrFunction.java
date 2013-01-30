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
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PDataType.LongNative;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.StringUtil;


/**
 * 
 * Implementation of the SUBSTR(<string>,<offset>[,<length>]) built-in function
 * where <offset> is the offset from the start of <string>. A positive offset
 * is treated as 1-based, a zero offset is treated as 0-based, and a negative
 * offset starts from the end of the string working backwards. The optional
 * <length> argument is the number of characters to return. In the absence of the
 * <length> argument, the rest of the string starting from <offset> is returned.
 * If <length> is less than 1, null is returned.
 *
 * @author jtaylor
 * @since 0.1
 */
@BuiltInFunction(name=SubstrFunction.NAME,  args={
    @Argument(allowedTypes={PDataType.VARCHAR}),
    @Argument(allowedTypes={PDataType.LONG}), // These are LONG because negative numbers end up as longs
    @Argument(allowedTypes={PDataType.LONG},defaultValue="null")} )
public class SubstrFunction extends ScalarFunction {
    public static final String NAME = "SUBSTR";
    private boolean hasLengthExpression;
    private boolean isOffsetConstant;
    private boolean isLengthConstant;
    private boolean isFixedWidth;
    private Integer maxLength;

    public SubstrFunction() {
    }

    public SubstrFunction(List<Expression> children) throws SQLException {
        super(children);
        init();
    }

    private void init() {
        isOffsetConstant = getOffsetExpression() instanceof LiteralExpression;
        isLengthConstant = getLengthExpression() instanceof LiteralExpression;
        hasLengthExpression = !isLengthConstant || ((LiteralExpression)getLengthExpression()).getValue() != null;
        isFixedWidth = (getStrExpression().getDataType().isFixedWidth() && isOffsetConstant) || (hasLengthExpression && isLengthConstant);
        if (hasLengthExpression && isLengthConstant) {
            Integer maxLength = ((Number)((LiteralExpression)getLengthExpression()).getValue()).intValue();
            this.maxLength = maxLength >= 0 ? maxLength : 0;
        } else if (isOffsetConstant) {
            Number offsetNumber = (Number)((LiteralExpression)getOffsetExpression()).getValue();
            if (offsetNumber != null) {
                int offset = offsetNumber.intValue();
                if (getStrExpression().getDataType().isFixedWidth()) {
                    if (offset >= 0) {
                        maxLength = getStrExpression().getMaxLength() - offset - (offset == 0 ? 0 : 1);
                    } else {
                        maxLength = -offset;
                    }
                }
            }
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        // TODO: multi-byte characters
        Expression offsetExpression = getOffsetExpression();
        if (!offsetExpression.evaluate(tuple,  ptr)) {
            return false;
        }
        LongNative longNative = (LongNative)offsetExpression.getDataType().getNative();
        int offset = (int)longNative.toLong(ptr);
        
        int length = -1;
        if (hasLengthExpression) {
            Expression lengthExpression = getLengthExpression();
            if (!lengthExpression.evaluate(tuple, ptr)) {
                return false;
            }
            longNative = (LongNative)lengthExpression.getDataType().getNative();
            length = (int)longNative.toLong(ptr);
            if (length <= 0) {
                return false;
            }
        }
        
        if (!getStrExpression().evaluate(tuple, ptr)) {
            return false;
        }

        try {
            int strlen = StringUtil.calculateUTF8Length(ptr.get(), ptr.getOffset(), ptr.getLength());
            
            // Account for 1 versus 0-based offset
            offset = offset - (offset <= 0 ? 0 : 1);
            if (offset < 0) { // Offset < 0 means get from end
                offset = strlen + offset;
            }
            if (offset < 0 || offset >= strlen) {
                return false;
            }
            int maxLength = strlen - offset;
            length = length == -1 ? maxLength : Math.min(length,maxLength);
            
            int byteOffset = StringUtil.getByteLengthForUtf8SubStr(ptr.get(), ptr.getOffset(), offset);
            int byteLength = StringUtil.getByteLengthForUtf8SubStr(ptr.get(), ptr.getOffset() + byteOffset, length);
            ptr.set(ptr.get(), ptr.getOffset() + byteOffset, byteLength);
            return true;
        } catch (UnsupportedEncodingException e) {
            return false;
        }
    }

    @Override
    public PDataType getDataType() {
        return isFixedWidth ? PDataType.CHAR : PDataType.VARCHAR;
    }

    @Override
    public boolean isNullable() {
        return getStrExpression().isNullable() || !isFixedWidth || getOffsetExpression().isNullable();
    }

    @Override
    public Integer getMaxLength() {
        return maxLength;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        init();
    }

    private Expression getStrExpression() {
        return children.get(0);
    }

    private Expression getOffsetExpression() {
        return children.get(1);
    }

    private Expression getLengthExpression() {
        return children.get(2);
    }

    @Override
    public boolean preservesOrder() {
        if (isOffsetConstant) {
            LiteralExpression literal = (LiteralExpression) getOffsetExpression();
            Number offsetNumber = (Number) literal.getValue();
            if (offsetNumber != null) { 
                int offset = offsetNumber.intValue();
                return (offset == 0 || offset == 1) && (!hasLengthExpression || isLengthConstant);
            }
        }
        return false;
    }

    @Override
    public KeyFormationDirective getKeyFormationDirective() {
        return preservesOrder() ? KeyFormationDirective.TRAVERSE_AND_EXTRACT : KeyFormationDirective.UNTRAVERSABLE;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
