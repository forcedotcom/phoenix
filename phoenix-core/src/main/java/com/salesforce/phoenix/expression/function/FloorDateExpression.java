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

import com.google.common.collect.Lists;
import com.salesforce.phoenix.expression.CoerceExpression;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.schema.PDataType;

/**
 * 
 * Class encapsulating the FLOOR operation on 
 * a column/literal of type {@link com.salesforce.phoenix.schema.PDataType#DATE}.
 *
 * @author samarth.jain
 * @since 3.0.0
 */
public class FloorDateExpression extends RoundDateExpression {
    
    public FloorDateExpression() {}
    
    private FloorDateExpression(List<Expression> children) {
        super(children);
    }
    
    public static Expression create(List<Expression> children) throws SQLException {
        Expression firstChild = children.get(0);
        PDataType firstChildDataType = firstChild.getDataType();
        if (firstChildDataType == PDataType.TIMESTAMP || firstChildDataType == PDataType.UNSIGNED_TIMESTAMP){
            // Coerce TIMESTAMP to DATE, as the nanos has no affect
            List<Expression> newChildren = Lists.newArrayListWithExpectedSize(children.size());
            newChildren.add(CoerceExpression.create(firstChild, firstChildDataType == PDataType.TIMESTAMP ? PDataType.DATE : PDataType.UNSIGNED_DATE));
            newChildren.addAll(children.subList(1, children.size()));
            children = newChildren;
        }
        return new FloorDateExpression(children);
    }
    
    /**
     * @param timeUnit - unit of time to round up to.
     * Creates a {@link FloorDateExpression} with default multiplier of 1.
     */
    public static Expression create(Expression expr, TimeUnit timeUnit) throws SQLException {
        return create(expr, timeUnit, 1);
    }
    
    /**
     * @param timeUnit - unit of time to round up to
     * @param multiplier - determines the roll up window size.
     * Create a {@link FloorDateExpression}. 
     */
    public static Expression create(Expression expr, TimeUnit timeUnit, int multiplier) throws SQLException {
        Expression timeUnitExpr = getTimeUnitExpr(timeUnit);
        Expression defaultMultiplierExpr = getMultiplierExpr(multiplier);
        List<Expression> expressions = Lists.newArrayList(expr, timeUnitExpr, defaultMultiplierExpr);
        return create(expressions);
    }
   
    @Override
    protected long getRoundUpAmount() {
        return 0;
    }
    
    @Override
    public String getName() {
        return FloorFunction.NAME;
    }
}
