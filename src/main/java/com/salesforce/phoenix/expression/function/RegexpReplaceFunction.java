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

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Function similar to the regexp_replace function in Postgres, which is used to pattern
 * match a segment of the string. Usage:
 * REGEXP_REPLACE(<source_char>,<pattern>,<replace_string>)
 * source_char is the string in which we want to perform string replacement. pattern is a
 * Java compatible regular expression string, and we replace all the matching part with 
 * replace_string. The first 2 arguments are required and are {@link com.salesforce.phoenix.schema.PDataType#VARCHAR},
 * the replace_string is default to empty string.
 * 
 * The function returns a {@link com.salesforce.phoenix.schema.PDataType#VARCHAR}
 * 
 * @author zhuang
 * @since 0.1
 */
@BuiltInFunction(name=RegexpReplaceFunction.NAME, args= {
    @Argument(allowedTypes={PDataType.VARCHAR}),
    @Argument(allowedTypes={PDataType.VARCHAR}),
    @Argument(allowedTypes={PDataType.VARCHAR},defaultValue="null")} )
public class RegexpReplaceFunction extends ScalarFunction {
    public static final String NAME = "REGEXP_REPLACE";

    private boolean hasReplaceStr;
    private Pattern pattern;
    
    public RegexpReplaceFunction() { }

    // Expect 1 arguments, the pattern. 
    public RegexpReplaceFunction(List<Expression> children) {
        super(children);
        init();
    }

    private void init() {
        hasReplaceStr = ((LiteralExpression)getReplaceStrExpression()).getValue() != null;
        Object patternString = ((LiteralExpression)children.get(1)).getValue();
        if (patternString != null) {
            pattern = Pattern.compile((String)patternString);
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        // Can't parse if there is no replacement pattern.
        if (pattern == null) {
            return false;
        }
        Expression sourceStrExpression = getSourceStrExpression();
        if (!sourceStrExpression.evaluate(tuple, ptr)) {
            return false;
        }
        String sourceStr = (String)PDataType.VARCHAR.toObject(ptr);
        if (sourceStr == null) {
            return false;
        }
        String replaceStr;
        if (hasReplaceStr) {
         Expression replaceStrExpression = this.getReplaceStrExpression();
            if (!replaceStrExpression.evaluate(tuple, ptr)) {
                return false;
            }
            replaceStr = (String)PDataType.VARCHAR.toObject(ptr);
        } else {
            replaceStr = "";
        }
        String replacedStr = pattern.matcher(sourceStr).replaceAll(replaceStr);
        ptr.set(PDataType.VARCHAR.toBytes(replacedStr));
        return true;
    }

    private Expression getSourceStrExpression() {
        return children.get(0);
    }

    private Expression getReplaceStrExpression() {
        return children.get(2);
    }

    @Override
    public PDataType getDataType() {
        return PDataType.VARCHAR;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        init();
    }

    @Override
    public String getName() {
        return NAME;
    }
}
