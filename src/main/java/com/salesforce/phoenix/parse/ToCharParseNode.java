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
package com.salesforce.phoenix.parse;

import java.sql.SQLException;
import java.text.Format;
import java.util.List;

import com.salesforce.phoenix.compile.StatementContext;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.expression.function.*;
import com.salesforce.phoenix.schema.PDataType;


public class ToCharParseNode extends FunctionParseNode {

    public ToCharParseNode(String name, List<ParseNode> children, BuiltInFunctionInfo info) {
        super(name, children, info);
    }

    @Override
    public FunctionExpression create(List<Expression> children, StatementContext context) throws SQLException {
        PDataType dataType = children.get(0).getDataType();
        String formatString = (String)((LiteralExpression)children.get(1)).getValue(); // either date or number format string
        Format formatter;
        FunctionArgumentType type;
        if (dataType.isCoercibleTo(PDataType.TIMESTAMP)) {
            if (formatString == null) {
                formatString = context.getDateFormat();
                formatter = context.getDateFormatter();
            } else {
                formatter = FunctionArgumentType.TEMPORAL.getFormatter(formatString);
            }
            type = FunctionArgumentType.TEMPORAL;
        }
        else if (dataType.isCoercibleTo(PDataType.DECIMAL)) {
            if (formatString == null)
                formatString = context.getNumberFormat();
            formatter = FunctionArgumentType.NUMERIC.getFormatter(formatString);
            type = FunctionArgumentType.NUMERIC;
        }
        else {
            throw new SQLException(dataType + " type is unsupported for TO_CHAR().  Numeric and temporal types are supported.");
        }
        return new ToCharFunction(children, type, formatString, formatter);
    }
}
