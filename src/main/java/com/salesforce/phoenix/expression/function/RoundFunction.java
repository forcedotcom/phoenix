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
import java.sql.Date;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PDataType.DateNative;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Function used to bucketize date/time values by rounding them to
 * an even increment.  Usage:
 * ROUND(<date/time col ref>,<'day'|'hour'|'minute'|'second'|'millisecond'>,<optional integer multiplier>)
 * The integer multiplier is optional and is used to do rollups to a partial time unit (i.e. 10 minute rollup)
 * The function returns a {@link com.salesforce.phoenix.schema.PDataType#DATE}
 *
 * @author jtaylor
 * @since 0.1
 */
@BuiltInFunction(name=RoundFunction.NAME, args= {
    @Argument(allowedTypes={PDataType.DATE}),
    @Argument(enumeration="TimeUnit"),
    @Argument(allowedTypes={PDataType.INTEGER}, isConstant=true, defaultValue="1")} )
public class RoundFunction extends ScalarFunction {
    public static final String NAME = "ROUND";
    private long divBy;
    
    private static final long[] TIME_UNIT_MS = new long[] {
        24 * 60 * 60 * 1000,
        60 * 60 * 1000,
        60 * 1000,
        1000,
        1
    };

    public RoundFunction() {
    }
    
    public RoundFunction(List<Expression> children) throws SQLException {
        super(children.subList(0, 1));
        Object timeUnitValue = ((LiteralExpression)children.get(1)).getValue();
        Object multiplierValue = ((LiteralExpression)children.get(2)).getValue();
        if (timeUnitValue != null && multiplierValue != null) {
            TimeUnit timeUnit = TimeUnit.valueOf(timeUnitValue.toString().toUpperCase());
            int multiplier = ((Number)multiplierValue).intValue();
            divBy = multiplier * TIME_UNIT_MS[timeUnit.ordinal()];
        }
    }
    
    protected long getRoundUpAmount() {
        return divBy/2;
    }
    
    private long roundTime(long time) {
        long value;
        long halfDivBy = getRoundUpAmount();
        if (time <= Long.MAX_VALUE - halfDivBy) { // If no overflow, add
            value = (time + halfDivBy) / divBy;
        } else { // Else subtract and add one
            value = (time - halfDivBy) / divBy + 1;
        }
        return value * divBy;
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        // If divBy is 0 this means <time unit> or <multiplier> was null
        if (divBy != 0 && children.get(0).evaluate(tuple, ptr)) {
            long time = DateNative.getInstance().toLong(ptr);
            long value = roundTime(time);
            // TODO: use temporary buffer instead and have way for caller to check if copying is necessary
            byte[] byteValue = getDataType().toBytes(new Date(value));
            ptr.set(byteValue);
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        long roundUpAmount = this.getRoundUpAmount();
        result = prime * result + (int)(divBy ^ (divBy >>> 32));
        result = prime * result + (int)(roundUpAmount ^ (roundUpAmount >>> 32));
        result = prime * result + children.get(0).hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        RoundFunction other = (RoundFunction)obj;
        if (divBy != other.divBy) return false;
        if (getRoundUpAmount() != other.getRoundUpAmount()) return false;
        return children.get(0).equals(other.children.get(0));
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        divBy = WritableUtils.readVLong(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeVLong(output, divBy);
    }

    @Override
    public PDataType getDataType() {
        return PDataType.DATE;
    }
    
    @Override
    public Integer getMaxLength() {
        return children.get(0).getMaxLength();
    }

    @Override
    public boolean isNullable() {
        return children.get(0).isNullable() || divBy == 0;
    }
    
    @Override
    public boolean preservesOrder() {
        return true;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
