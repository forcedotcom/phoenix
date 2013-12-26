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
import java.sql.Date;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.Lists;
import com.salesforce.phoenix.compile.KeyPart;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PDataType.PDataCodec;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ByteUtil;

/**
 * Function used to bucketize date/time values by rounding them to
 * an even increment.  Usage:
 * ROUND(<date/time col ref>,<'day'|'hour'|'minute'|'second'|'millisecond'>,<optional integer multiplier>)
 * The integer multiplier is optional and is used to do rollups to a partial time unit (i.e. 10 minute rollup)
 * The function returns a {@link com.salesforce.phoenix.schema.PDataType#DATE}

 * @author jtaylor, samarth.jain
 * @since 0.1
 */
public class RoundDateExpression extends ScalarFunction {
    
    long divBy;
    
    public static final String NAME = "ROUND";
    
    private static final long[] TIME_UNIT_MS = new long[] {
        24 * 60 * 60 * 1000,
        60 * 60 * 1000,
        60 * 1000,
        1000,
        1
    };
    
    public RoundDateExpression() {}
    
    /**
     * @param timeUnit - unit of time to round up to.
     * Creates a {@link RoundDateExpression} with default multiplier of 1.
     */
    public static Expression create(Expression expr, TimeUnit timeUnit) throws SQLException {
        return create(expr, timeUnit, 1);
    }
    
    /**
     * @param timeUnit - unit of time to round up to
     * @param multiplier - determines the roll up window size.
     * Create a {@link RoundDateExpression}. 
     */
    public static Expression create(Expression expr, TimeUnit timeUnit, int multiplier) throws SQLException {
        Expression timeUnitExpr = getTimeUnitExpr(timeUnit);
        Expression defaultMultiplierExpr = getMultiplierExpr(multiplier);
        List<Expression> expressions = Lists.newArrayList(expr, timeUnitExpr, defaultMultiplierExpr);
        return create(expressions);
    }
    
    public static Expression create(List<Expression> children) throws SQLException {
        return new RoundDateExpression(children);
    }
    
    static Expression getTimeUnitExpr(TimeUnit timeUnit) throws SQLException {
        return LiteralExpression.newConstant(timeUnit.name(), PDataType.VARCHAR);
    }
    
    static Expression getMultiplierExpr(int multiplier) throws SQLException {
        return LiteralExpression.newConstant(multiplier, PDataType.INTEGER);
    }
    
    RoundDateExpression(List<Expression> children) {
        super(children.subList(0, 1));
        int numChildren = children.size();
        if(numChildren < 2 || numChildren > 3) {
            throw new IllegalArgumentException("Wrong number of arguments : " + numChildren);
        }
        Object timeUnitValue = ((LiteralExpression)children.get(1)).getValue();
        Object multiplierValue = numChildren > 2 ? ((LiteralExpression)children.get(2)).getValue() : null;
        int multiplier = multiplierValue == null ? 1 :((Number)multiplierValue).intValue();
        TimeUnit timeUnit = TimeUnit.getTimeUnit(timeUnitValue != null ? timeUnitValue.toString() : null); 
        divBy = multiplier * TIME_UNIT_MS[timeUnit.ordinal()];
    }
    
    
    protected long getRoundUpAmount() {
        return divBy/2;
    }
    
    
    protected long roundTime(long time) {
        long value;
        long roundUpAmount = getRoundUpAmount();
        if (time <= Long.MAX_VALUE - roundUpAmount) { // If no overflow, add
            value = (time + roundUpAmount) / divBy;
        } else { // Else subtract and add one
            value = (time - roundUpAmount) / divBy + 1;
        }
        return value * divBy;
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (children.get(0).evaluate(tuple, ptr)) {
            PDataType dataType = getDataType();
            long time = dataType.getCodec().decodeLong(ptr, children.get(0).getColumnModifier());
            long value = roundTime(time);
            
            Date d = new Date(value);
            byte[] byteValue = dataType.toBytes(d);
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
        RoundDateExpression other = (RoundDateExpression)obj;
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
        return children.get(0).getDataType();
    }
    
    @Override
    public Integer getByteSize() {
        return children.get(0).getByteSize();
    }

    @Override
    public boolean isNullable() {
        return children.get(0).isNullable() || divBy == 0;
    }
    
    protected PDataCodec getKeyRangeCodec(PDataType columnDataType) {
        return columnDataType.getCodec();
    }
    
    /**
     * Form the key range from the key to the key right before or at the
     * next rounded value.
     */
    @Override
    public KeyPart newKeyPart(final KeyPart childPart) {
        return new KeyPart() {
            private final List<Expression> extractNodes = Collections.<Expression>singletonList(RoundDateExpression.this);

            @Override
            public PColumn getColumn() {
                return childPart.getColumn();
            }

            @Override
            public List<Expression> getExtractNodes() {
                return extractNodes;
            }

            @Override
            public KeyRange getKeyRange(CompareOp op, Expression rhs) {
                PDataType type = getColumn().getDataType();
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                rhs.evaluate(null, ptr);
                byte[] key = ByteUtil.copyKeyBytesIfNecessary(ptr);
                // No need to take into account column modifier, because ROUND
                // always forces the value to be in ascending order
                PDataCodec codec = getKeyRangeCodec(type);
                int offset = ByteUtil.isInclusive(op) ? 1 : 0;
                long value = codec.decodeLong(key, 0, null);
                byte[] nextKey = new byte[type.getByteSize()];
                switch (op) {
                case EQUAL:
                    // If the value isn't evenly divisible by the div amount, then it
                    // can't possibly be equal to any rounded value. For example, if you
                    // had ROUND(dateCol,'DAY') = TO_DATE('2013-01-01 23:00:00')
                    // it could never be equal, since date constant isn't at a day
                    // boundary.
                    if (value % divBy != 0) {
                        return KeyRange.EMPTY_RANGE;
                    }
                    codec.encodeLong(value + divBy, nextKey, 0);
                    return type.getKeyRange(key, true, nextKey, false);
                case GREATER:
                case GREATER_OR_EQUAL:
                    codec.encodeLong((value + divBy - offset)/divBy*divBy, nextKey, 0);
                    return type.getKeyRange(nextKey, true, KeyRange.UNBOUND, false);
                case LESS:
                case LESS_OR_EQUAL:
                    codec.encodeLong((value + divBy - (1 -offset))/divBy*divBy, nextKey, 0);
                    return type.getKeyRange(KeyRange.UNBOUND, false, nextKey, false);
                default:
                    return childPart.getKeyRange(op, rhs);
                }
            }
        };
    }


    @Override
    public String getName() {
        return NAME;
    }
    
    @Override
    public OrderPreserving preservesOrder() {
        return OrderPreserving.YES;
    }

    @Override
    public int getKeyFormationTraversalIndex() {
        return 0;
    }
}
