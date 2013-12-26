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

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.parse.CeilParseNode;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.schema.PDataType;
/**
 * 
 * Base class for built-in CEIL function.
 *
 * @author samarth.jain
 * @since 3.0.0
 */
@BuiltInFunction(name = CeilFunction.NAME,
                 nodeClass = CeilParseNode.class,
                 args = {
                        @Argument(allowedTypes={PDataType.TIMESTAMP, PDataType.DECIMAL}),
                        @Argument(allowedTypes={PDataType.VARCHAR, PDataType.INTEGER}, defaultValue = "null", isConstant=true),
                        @Argument(allowedTypes={PDataType.INTEGER}, defaultValue="1", isConstant=true)
                        } 
                )
public abstract class CeilFunction extends ScalarFunction {
    
    public static final String NAME = "CEIL";
    
    public CeilFunction(List<Expression> children) {
        super(children);
    }
    
    @Override
    public String getName() {
        return NAME;
    }
    
}
