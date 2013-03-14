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
package com.salesforce.phoenix.arithmatic;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.*;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Test;

import com.salesforce.phoenix.exception.ValueTypeIncompatibleException;
import com.salesforce.phoenix.expression.*;
import com.salesforce.phoenix.schema.PDataType;


public class ArithmeticOperationTest {

    // Addition
    // result scale should be: max(ls, rs)
    // result precision should be: max(lp - ls, rp - rs) + 1 + max(ls, rs)
    @Test
    public void testDecimalAddition() throws Exception {
        LiteralExpression op1, op2, op3;
        List<Expression> children;
        ImmutableBytesWritable ptr;
        DecimalAddExpression e;
        boolean evaluated;

        op1 = LiteralExpression.newConstant(new BigDecimal("1234567890123456789012345678901"), PDataType.DECIMAL);
        op2 = LiteralExpression.newConstant(new BigDecimal("12345"), PDataType.DECIMAL);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalAddExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("1234567890123456789012345691246"), ptr);

        op1 = LiteralExpression.newConstant(new BigDecimal("12345"), PDataType.DECIMAL);
        op2 = LiteralExpression.newConstant(new BigDecimal("123.45"), PDataType.DECIMAL);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalAddExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("12468.45"), ptr);

        // Exceeds precision.
        op1 = LiteralExpression.newConstant(new BigDecimal("9999999999999999999999999999999"), PDataType.DECIMAL);
        op2 = LiteralExpression.newConstant(new BigDecimal("123"), PDataType.DECIMAL);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalAddExpression(children);
        ptr = new ImmutableBytesWritable();
        try {
            evaluated = e.evaluate(null, ptr);
            fail("Evaluation should have failed");
        } catch (ValueTypeIncompatibleException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("ERROR 206 (22003): The value is outside the range for the data type. DECIMAL(31,0)"));
        }

        // Pass since we roll out imposing precisioin and scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("9999999999999999999999999999999"), PDataType.DECIMAL);
        op2 = LiteralExpression.newConstant(new BigDecimal("123"), PDataType.DECIMAL);
        op3 = LiteralExpression.newConstant(new BigDecimal("-123"), PDataType.DECIMAL);
        children = Arrays.<Expression>asList(op1, op2, op3);
        e = new DecimalAddExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("9999999999999999999999999999999"), ptr);

        // Exceeds scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("1234567890123456789012345678901"), PDataType.DECIMAL);
        op2 = LiteralExpression.newConstant(new BigDecimal("123.45"), PDataType.DECIMAL);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalAddExpression(children);
        ptr = new ImmutableBytesWritable();
        try {
            evaluated = e.evaluate(null, ptr);
            fail("Evaluation should have failed");
        } catch (ValueTypeIncompatibleException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("ERROR 206 (22003): The value is outside the range for the data type. DECIMAL(31,2)"));
        }
    }

    // Subtraction
    // result scale should be: max(ls, rs)
    // result precision should be: max(lp - ls, rp - rs) + 1 + max(ls, rs)
    @Test
    public void testDecimalSubtraction() throws Exception {
        LiteralExpression op1, op2, op3;
        List<Expression> children;
        ImmutableBytesWritable ptr;
        DecimalSubtractExpression e;
        boolean evaluated;

        op1 = LiteralExpression.newConstant(new BigDecimal("1234567890123456789012345678901"), PDataType.DECIMAL);
        op2 = LiteralExpression.newConstant(new BigDecimal("12345"), PDataType.DECIMAL);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalSubtractExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("1234567890123456789012345666556"), ptr);

        op1 = LiteralExpression.newConstant(new BigDecimal("12345"), PDataType.DECIMAL);
        op2 = LiteralExpression.newConstant(new BigDecimal("123.45"), PDataType.DECIMAL);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalSubtractExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("12221.55"), ptr);

        // Excceds precision
        op1 = LiteralExpression.newConstant(new BigDecimal("9999999999999999999999999999999"), PDataType.DECIMAL);
        op2 = LiteralExpression.newConstant(new BigDecimal("-123"), PDataType.DECIMAL);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalSubtractExpression(children);
        ptr = new ImmutableBytesWritable();
        try {
            evaluated = e.evaluate(null, ptr);
            fail("Evaluation should have failed");
        } catch (ValueTypeIncompatibleException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("ERROR 206 (22003): The value is outside the range for the data type. DECIMAL(31,0)"));
        }

        // Pass since we roll up precision and scale imposing.
        op1 = LiteralExpression.newConstant(new BigDecimal("9999999999999999999999999999999"), PDataType.DECIMAL);
        op2 = LiteralExpression.newConstant(new BigDecimal("-123"), PDataType.DECIMAL);
        op3 = LiteralExpression.newConstant(new BigDecimal("123"), PDataType.DECIMAL);
        children = Arrays.<Expression>asList(op1, op2, op3);
        e = new DecimalSubtractExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("9999999999999999999999999999999"), ptr);

        // Exceeds scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("1234567890123456789012345678901"), PDataType.DECIMAL);
        op2 = LiteralExpression.newConstant(new BigDecimal("123.45"), PDataType.DECIMAL);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalSubtractExpression(children);
        ptr = new ImmutableBytesWritable();
        try {
            evaluated = e.evaluate(null, ptr);
            fail("Evaluation should have failed");
        } catch (ValueTypeIncompatibleException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("ERROR 206 (22003): The value is outside the range for the data type. DECIMAL(31,2)"));
        }
    }

    // Multiplication
    // result scale should be: ls + rs
    // result precision should be: lp + rp
    @Test
    public void testDecimalMultiplication() throws Exception {
        LiteralExpression op1, op2;
        List<Expression> children;
        ImmutableBytesWritable ptr;
        DecimalMultiplyExpression e;
        boolean evaluated;

        op1 = LiteralExpression.newConstant(new BigDecimal("12345"), PDataType.DECIMAL);
        op2 = LiteralExpression.newConstant(new BigDecimal("123.45"), PDataType.DECIMAL);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalMultiplyExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("1523990.25"), ptr);

        // Value too big, exceeds precision.
        op1 = LiteralExpression.newConstant(new BigDecimal("1234567890123456789012345678901"), PDataType.DECIMAL);
        op2 = LiteralExpression.newConstant(new BigDecimal("12345"), PDataType.DECIMAL);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalMultiplyExpression(children);
        ptr = new ImmutableBytesWritable();
        try {
            evaluated = e.evaluate(null, ptr);
            fail("Evaluation should have failed");
        } catch (ValueTypeIncompatibleException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("ERROR 206 (22003): The value is outside the range for the data type. DECIMAL(31,0)"));
        }

        // Values exceeds scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("1234567890123456789012345678901"), PDataType.DECIMAL);
        op2 = LiteralExpression.newConstant(new BigDecimal("1.45"), PDataType.DECIMAL);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalMultiplyExpression(children);
        ptr = new ImmutableBytesWritable();
        try {
            evaluated = e.evaluate(null, ptr);
            fail("Evaluation should have failed");
        } catch (ValueTypeIncompatibleException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("ERROR 206 (22003): The value is outside the range for the data type. DECIMAL(31,2)"));
        }
    }

    // Division
    // result scale should be: 31 - lp + ls - rs
    // result precision should be: lp - ls + rp + scale
    @Test
    public void testDecimalDivision() throws Exception {
        LiteralExpression op1, op2;
        List<Expression> children;
        ImmutableBytesWritable ptr;
        DecimalDivideExpression e;
        boolean evaluated;

        // The value should be 1234500.0000...00 because we set to scale to be 24. However, in
        // PhoenixResultSet.getBigDecimal, the case to (BigDecimal) actually cause the scale to be eradicated. As
        // a result, the resulting value does not have the right form.
        op1 = LiteralExpression.newConstant(new BigDecimal("12345"), PDataType.DECIMAL);
        op2 = LiteralExpression.newConstant(new BigDecimal("0.01"), PDataType.DECIMAL);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalDivideExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("1.2345E+6"), ptr);

        // Exceeds precision.
        op1 = LiteralExpression.newConstant(new BigDecimal("1234567890123456789012345678901"), PDataType.DECIMAL);
        op2 = LiteralExpression.newConstant(new BigDecimal("0.01"), PDataType.DECIMAL);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalDivideExpression(children);
        ptr = new ImmutableBytesWritable();
        try {
            evaluated = e.evaluate(null, ptr);
            fail("Evaluation should have failed");
        } catch (ValueTypeIncompatibleException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("ERROR 206 (22003): The value is outside the range for the data type. DECIMAL(31,0)"));
        }
    }

    private static void assertEqualValue(PDataType type, Object value, ImmutableBytesWritable ptr) {
        assertEquals(value, type.toObject(ptr.get()));
    }
}
