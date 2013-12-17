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
package com.salesforce.phoenix.expression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Test;

import com.salesforce.phoenix.expression.function.CeilDateExpression;
import com.salesforce.phoenix.expression.function.CeilDecimalExpression;
import com.salesforce.phoenix.expression.function.FloorDateExpression;
import com.salesforce.phoenix.expression.function.FloorDecimalExpression;
import com.salesforce.phoenix.expression.function.RoundDateExpression;
import com.salesforce.phoenix.expression.function.RoundDecimalExpression;
import com.salesforce.phoenix.expression.function.TimeUnit;
import com.salesforce.phoenix.schema.IllegalDataException;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.DateUtil;

/**
 * 
 * Unit tests for {@link RoundDecimalExpression}, {@link FloorDecimalExpression}
 * and {@link CeilDecimalExpression}.
 *
 * @author samarth.jain
 * @since 3.0.0
 */
public class RoundFloorCeilExpressionsUnitTests {

    @Test
    public void testRoundDecimalExpression() throws Exception {
        LiteralExpression bd = LiteralExpression.newConstant(1.23898, PDataType.DECIMAL);
        RoundDecimalExpression rde = RoundDecimalExpression.create(bd, 3);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rde.evaluate(null, ptr);
        Object obj = rde.getDataType().toObject(ptr);
        assertTrue(obj instanceof BigDecimal);
        BigDecimal value = (BigDecimal)obj;
        assertEquals(BigDecimal.valueOf(1.239), value);
    }
    
    @Test
    public void testCeilDecimalExpression() throws Exception {
        LiteralExpression bd = LiteralExpression.newConstant(1.23898, PDataType.DECIMAL);
        CeilDecimalExpression rde = CeilDecimalExpression.create(bd, 3);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rde.evaluate(null, ptr);
        Object obj = rde.getDataType().toObject(ptr);
        assertTrue(obj instanceof BigDecimal);
        BigDecimal value = (BigDecimal)obj;
        assertEquals(BigDecimal.valueOf(1.239), value);
    }
    
    @Test
    public void testFloorDecimalExpression() throws Exception {
        LiteralExpression bd = LiteralExpression.newConstant(1.23898, PDataType.DECIMAL);
        FloorDecimalExpression rde = FloorDecimalExpression.create(bd, 3);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rde.evaluate(null, ptr);
        Object obj = rde.getDataType().toObject(ptr);
        assertTrue(obj instanceof BigDecimal);
        BigDecimal value = (BigDecimal)obj;
        assertEquals(BigDecimal.valueOf(1.238), value);
    }
    
    @Test
    public void testRoundDecimalExpressionScaleParamValidation() throws Exception {
        LiteralExpression bd = LiteralExpression.newConstant(1.23898, PDataType.DECIMAL);
        LiteralExpression scale = LiteralExpression.newConstant("3", PDataType.VARCHAR);
        List<Expression> exprs = new ArrayList<Expression>(2);
        exprs.add(bd);
        exprs.add(scale);
        try {
            new RoundDecimalExpression(exprs);
            fail("Evaluation should have failed because only an INTEGER is allowed for second param in a RoundDecimalExpression");
        } catch(IllegalDataException e) {

        }
    }
    
    @Test
    public void testRoundDateExpression() throws Exception {
        LiteralExpression date = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDataType.DATE);
        RoundDateExpression rde = RoundDateExpression.create(date, TimeUnit.DAY);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rde.evaluate(null, ptr);
        Object obj = rde.getDataType().toObject(ptr);
        assertTrue(obj instanceof Date);
        Date value = (Date)obj;
        assertEquals(DateUtil.parseDate("2012-01-02 00:00:00"), value);
    }
    
    @Test
    public void testRoundDateExpressionWithMultiplier() throws Exception {
        Expression date = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDataType.DATE);
        RoundDateExpression rde = RoundDateExpression.create(date, TimeUnit.MINUTE, 10);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rde.evaluate(null, ptr);
        Object obj = rde.getDataType().toObject(ptr);
        assertTrue(obj instanceof Date);
        Date value = (Date)obj;
        assertEquals(DateUtil.parseDate("2012-01-01 14:30:00"), value);
    }
    
    @Test
    public void testCeilDateExpression() throws Exception {
        LiteralExpression date = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDataType.DATE);
        CeilDateExpression rde = CeilDateExpression.create(date, TimeUnit.DAY);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rde.evaluate(null, ptr);
        Object obj = rde.getDataType().toObject(ptr);
        assertTrue(obj instanceof Date);
        Date value = (Date)obj;
        assertEquals(DateUtil.parseDate("2012-01-02 00:00:00"), value);
    }
    
    @Test
    public void testCeilDateExpressionWithMultiplier() throws Exception {
        Expression date = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDataType.DATE);
        CeilDateExpression rde = CeilDateExpression.create(date, TimeUnit.SECOND, 10);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rde.evaluate(null, ptr);
        Object obj = rde.getDataType().toObject(ptr);
        assertTrue(obj instanceof Date);
        Date value = (Date)obj;
        assertEquals(DateUtil.parseDate("2012-01-01 14:25:30"), value);
    }
    
    @Test
    public void testFloorDateExpression() throws Exception {
        LiteralExpression date = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDataType.DATE);
        FloorDateExpression rde = FloorDateExpression.create(date, TimeUnit.DAY);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rde.evaluate(null, ptr);
        Object obj = rde.getDataType().toObject(ptr);
        assertTrue(obj instanceof Date);
        Date value = (Date)obj;
        assertEquals(DateUtil.parseDate("2012-01-01 00:00:00"), value);
    }
    
    @Test
    public void testFloorDateExpressionWithMultiplier() throws Exception {
        Expression date = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDataType.DATE);
        FloorDateExpression rde = FloorDateExpression.create(date, TimeUnit.SECOND, 10);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rde.evaluate(null, ptr);
        Object obj = rde.getDataType().toObject(ptr);
        assertTrue(obj instanceof Date);
        Date value = (Date)obj;
        assertEquals(DateUtil.parseDate("2012-01-01 14:25:20"), value);
    }
    
    /**
     * Tests {@link RoundDateExpression} constructor check which only allows number of arguments between 2 and 3.
     */
    @Test
    public void testRoundDateExpressionValidation_1() throws Exception {
        LiteralExpression date = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDataType.DATE);
        List<Expression> exprs = new ArrayList<Expression>(1);
        exprs.add(date);
        try {
            new RoundDateExpression(exprs);
            fail("Instantiating a RoundDateExpression with only one argument should have failed.");
        } catch(IllegalArgumentException e) {

        }
    }
    
    /**
     * Tests {@link RoundDateExpression} constructor for a valid value of time unit.
     */
    @Test
    public void testRoundDateExpressionValidation_2() throws Exception {
        LiteralExpression date = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDataType.DATE);
        LiteralExpression timeUnit = LiteralExpression.newConstant("millis", PDataType.VARCHAR);
        List<Expression> exprs = new ArrayList<Expression>(1);
        exprs.add(date);
        exprs.add(timeUnit);
        try {
            new RoundDateExpression(exprs);
            fail("Only a valid time unit represented by TimeUnit enum is allowed and millis is invalid.");
        } catch(IllegalArgumentException e) {

        }
    }

}
