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
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;

public class RoundDecimalFunction extends RoundFunction {
    
    private int scale;
    
    public RoundDecimalFunction(List<Expression> children) throws SQLException {
        super(children.subList(0, 1));
        int numChildren = children.size();
        if(numChildren == 0 || numChildren > 2) {
            throw new SQLException ("Invalid number of arguments: " + numChildren);
        }
        String scaleValue = (String)((LiteralExpression)children.get(1)).getValue();
        try {
            scale = Integer.parseInt(scaleValue);
        } catch (NumberFormatException e) {
            throw new SQLException("Invalid value: " + scaleValue.toString());
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if(children.get(0).evaluate(tuple, ptr)) {
            BigDecimal value = (BigDecimal)PDataType.DECIMAL.toObject(ptr);
            BigDecimal scaledValue = value.setScale(scale, getRoundingMode());
            ptr.set(getDataType().toBytes(scaledValue));
            return true;
        }
        return false;
    }

    @Override
    public PDataType getDataType() {
        return PDataType.DECIMAL;
    }
    
    protected RoundingMode getRoundingMode() {
        return RoundingMode.HALF_UP;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        scale = WritableUtils.readVInt(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeVInt(output, scale);
    }

}
