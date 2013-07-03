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
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
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
public class SubstrFunction extends PrefixFunction {
    public static final String NAME = "SUBSTR";
    private boolean hasLengthExpression;
    private boolean isOffsetConstant;
    private boolean isLengthConstant;
    private boolean isFixedWidth;
    private Integer byteSize;

    public SubstrFunction() {
    }

    public SubstrFunction(List<Expression> children) {
        super(children);
        init();
    }

    private void init() {
        // TODO: when we have ColumnModifier.REVERSE, we'll need to negate offset,
        // since the bytes are reverse and we'll want to work from the end.
        isOffsetConstant = getOffsetExpression() instanceof LiteralExpression;
        isLengthConstant = getLengthExpression() instanceof LiteralExpression;
        hasLengthExpression = !isLengthConstant || ((LiteralExpression)getLengthExpression()).getValue() != null;
        isFixedWidth = getStrExpression().getDataType().isFixedWidth() && ((hasLengthExpression && isLengthConstant) || (!hasLengthExpression && isOffsetConstant));
        if (hasLengthExpression && isLengthConstant) {
            Integer maxLength = ((Number)((LiteralExpression)getLengthExpression()).getValue()).intValue();
            this.byteSize = maxLength >= 0 ? maxLength : 0;
        } else if (isOffsetConstant) {
            Number offsetNumber = (Number)((LiteralExpression)getOffsetExpression()).getValue();
            if (offsetNumber != null) {
                int offset = offsetNumber.intValue();
                if (getStrExpression().getDataType().isFixedWidth()) {
                    if (offset >= 0) {
                        byteSize = getStrExpression().getByteSize() - offset + (offset == 0 ? 0 : 1);
                    } else {
                        byteSize = -offset;
                    }
                }
            }
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression offsetExpression = getOffsetExpression();
        if (!offsetExpression.evaluate(tuple,  ptr)) {
            return false;
        }
        int offset = offsetExpression.getDataType().getCodec().decodeInt(ptr, offsetExpression.getColumnModifier());
        
        int length = -1;
        if (hasLengthExpression) {
            Expression lengthExpression = getLengthExpression();
            if (!lengthExpression.evaluate(tuple, ptr)) {
                return false;
            }
            length = lengthExpression.getDataType().getCodec().decodeInt(ptr, lengthExpression.getColumnModifier());
            if (length <= 0) {
                return false;
            }
        }
        
        if (!getStrExpression().evaluate(tuple, ptr)) {
            return false;
        }
        
        try {
            boolean isCharType = getStrExpression().getDataType() == PDataType.CHAR;
            ColumnModifier columnModifier = getStrExpression().getColumnModifier();
            int strlen = isCharType ? ptr.getLength() : StringUtil.calculateUTF8Length(ptr.get(), ptr.getOffset(), ptr.getLength(), columnModifier);
            
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
            
            int byteOffset = isCharType ? offset : StringUtil.getByteLengthForUtf8SubStr(ptr.get(), ptr.getOffset(), offset, columnModifier);
            int byteLength = isCharType ? length : StringUtil.getByteLengthForUtf8SubStr(ptr.get(), ptr.getOffset() + byteOffset, length, columnModifier);
            ptr.set(ptr.get(), ptr.getOffset() + byteOffset, byteLength);
            return true;
        } catch (UnsupportedEncodingException e) {
            return false;
        }
    }

    @Override
    public PDataType getDataType() {
        // If fixed width, then return child expression type.
        // If not fixed width, then we don't know how big this will be across the board
        return isFixedWidth ? getStrExpression().getDataType() : PDataType.VARCHAR;
    }

    @Override
    public boolean isNullable() {
        return getStrExpression().isNullable() || !isFixedWidth || getOffsetExpression().isNullable();
    }

    @Override
    public Integer getByteSize() {
        return byteSize;
    }
    
    @Override
    public ColumnModifier getColumnModifier() {
        return getStrExpression().getColumnModifier();
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
    public OrderPreserving preservesOrder() {
        if (isOffsetConstant) {
            LiteralExpression literal = (LiteralExpression) getOffsetExpression();
            Number offsetNumber = (Number) literal.getValue();
            if (offsetNumber != null) { 
                int offset = offsetNumber.intValue();
                if ((offset == 0 || offset == 1) && (!hasLengthExpression || isLengthConstant)) {
                    return OrderPreserving.YES_IF_LAST;
                }
            }
        }
        return OrderPreserving.NO;
    }

    @Override
    protected boolean extractNode() {
        return true;
    }

    @Override
    public String getName() {
        return NAME;
    }
    
}
