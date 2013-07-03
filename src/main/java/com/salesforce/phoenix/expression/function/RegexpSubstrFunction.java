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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ByteUtil;


/**
 * 
 * Implementation of REGEXP_SUBSTR(<source>, <pattern>, <offset>) built-in function,
 * where <offset> is the offset from the start of <string>. Positive offset is treated as 1-based,
 * a zero offset is treated as 0-based, and a negative offset starts from the end of the string 
 * working backwards. The <pattern> is the pattern we would like to search for in the <source> string.
 * The function returns the first occurance of any substring in the <source> string that matches
 * the <pattern> input as a VARCHAR. 
 * 
 * @author zhuang
 * @since 0.1
 */
@BuiltInFunction(name=RegexpSubstrFunction.NAME, args={
    @Argument(allowedTypes={PDataType.VARCHAR}),
    @Argument(allowedTypes={PDataType.VARCHAR}),
    @Argument(allowedTypes={PDataType.LONG}, defaultValue="1")} )
public class RegexpSubstrFunction extends PrefixFunction {
    public static final String NAME = "REGEXP_SUBSTR";

    private Pattern pattern;
    private boolean isOffsetConstant;
    private Integer byteSize;

    public RegexpSubstrFunction() { }

    public RegexpSubstrFunction(List<Expression> children) {
        super(children);
        init();
    }

    private void init() {
        Object patternString = ((LiteralExpression)children.get(1)).getValue();
        if (patternString != null) {
            pattern = Pattern.compile((String)patternString);
        }
        // If the source string has a fixed width, then the max length would be the length 
        // of the source string minus the offset, or the absolute value of the offset if 
        // it's negative. Offset number is a required argument. However, if the source string
        // is not fixed width, the maxLength would be null.
        isOffsetConstant = getOffsetExpression() instanceof LiteralExpression;
        Number offsetNumber = (Number)((LiteralExpression)getOffsetExpression()).getValue();
        if (offsetNumber != null) {
            int offset = offsetNumber.intValue();
            if (getSourceStrExpression().getDataType().isFixedWidth()) {
                if (offset >= 0) {
                    byteSize = getSourceStrExpression().getByteSize() - offset - (offset == 0 ? 0 : 1);
                } else {
                    byteSize = -offset;
                }
            }
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (pattern == null) {
            return false;
        }
        if (!getSourceStrExpression().evaluate(tuple, ptr)) {
            return false;
        }
        String sourceStr = (String)PDataType.VARCHAR.toObject(ptr, getSourceStrExpression().getColumnModifier());
        if (sourceStr == null) {
            return false;
        }

        Expression offsetExpression = getOffsetExpression();
        if (!offsetExpression.evaluate(tuple, ptr)) {
            return false;
        }
        int offset = offsetExpression.getDataType().getCodec().decodeInt(ptr, offsetExpression.getColumnModifier());

        int strlen = sourceStr.length();
        // Account for 1 versus 0-based offset
        offset = offset - (offset <= 0 ? 0 : 1);
        if (offset < 0) { // Offset < 0 means get from end
            offset = strlen + offset;
        }
        if (offset < 0 || offset >= strlen) {
            return false;
        }

        Matcher matcher = pattern.matcher(sourceStr);
        boolean hasSubString = matcher.find(offset);
        if (!hasSubString) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
        }
        String subString = matcher.group();
        ptr.set(PDataType.VARCHAR.toBytes(subString));
        return true;
    }

    @Override
    public Integer getByteSize() {
        return byteSize;
    }

    @Override
    public OrderPreserving preservesOrder() {
        if (isOffsetConstant) {
            LiteralExpression literal = (LiteralExpression) getOffsetExpression();
            Number offsetNumber = (Number) literal.getValue();
            if (offsetNumber != null) { 
                int offset = offsetNumber.intValue();
                if (offset == 0 || offset == 1) {
                    return OrderPreserving.YES_IF_LAST;
                }
            }
        }
        return OrderPreserving.NO;
    }

    @Override
    public int getKeyFormationTraversalIndex() {
        return preservesOrder() == OrderPreserving.NO ? NO_TRAVERSAL : 0;
    }

    private Expression getOffsetExpression() {
        return children.get(2);
    }

    private Expression getSourceStrExpression() {
        return children.get(0);
    }

    @Override
    public PDataType getDataType() {
        // ALways VARCHAR since we do not know in advanced how long the 
        // matched string will be.
        return PDataType.VARCHAR;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
