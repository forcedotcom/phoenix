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
package com.salesforce.phoenix.schema;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.salesforce.phoenix.util.TestUtil;


public class PDataTypeTest {
    @Test
    public void testLong() {
        Long la = 4L;
        byte[] b = PDataType.LONG.toBytes(la);
        Long lb = (Long)PDataType.LONG.toObject(b);
        assertEquals(la,lb);
        
        Long na = 1L;
        Long nb = -1L;
        byte[] ba = PDataType.LONG.toBytes(na);
        byte[] bb = PDataType.LONG.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
    }
    
    @Test
    public void testInt() {
        Integer na = 4;
        byte[] b = PDataType.INTEGER.toBytes(na);
        Integer nb = (Integer)PDataType.INTEGER.toObject(b);
        assertEquals(na,nb);

        na = 1;
        nb = -1;
        byte[] ba = PDataType.INTEGER.toBytes(na);
        byte[] bb = PDataType.INTEGER.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -1;
        nb = -3;
        ba = PDataType.INTEGER.toBytes(na);
        bb = PDataType.INTEGER.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -3;
        nb = -100000000;
        ba = PDataType.INTEGER.toBytes(na);
        bb = PDataType.INTEGER.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
    }

    @Test
    public void testBigDecimal() {
        byte[] b;
        BigDecimal na, nb;
        
        b = new byte[] {
                (byte)0xc2,0x02,0x10,0x36,0x22,0x22,0x22,0x22,0x22,0x22,0x0f,0x27,0x38,0x1c,0x05,0x40,0x62,0x21,0x54,0x4d,0x4e,0x01,0x14,0x36,0x0d,0x33
        };
        BigDecimal decodedBytes = (BigDecimal)PDataType.DECIMAL.toObject(b);
        assertTrue(decodedBytes.compareTo(BigDecimal.ZERO) > 0);
 
        na = new BigDecimal(new BigInteger("12345678901239998123456789"), 2);
        //[-52, 13, 35, 57, 79, 91, 13, 40, 100, 82, 24, 46, 68, 90]
        b = PDataType.DECIMAL.toBytes(na);
        nb = (BigDecimal)PDataType.DECIMAL.toObject(b);
        TestUtil.assertRoundEquals(na,nb);
        assertTrue(b.length <= PDataType.DECIMAL.estimateByteSize(na));
        
        na = new BigDecimal("115.533333333333331438552704639732837677001953125");
        b = PDataType.DECIMAL.toBytes(na);
        nb = (BigDecimal)PDataType.DECIMAL.toObject(b);
        TestUtil.assertRoundEquals(na,nb);
        assertTrue(b.length <= PDataType.DECIMAL.estimateByteSize(na));

        na = new BigDecimal(2.5);
        b = PDataType.DECIMAL.toBytes(na);
        nb = (BigDecimal)PDataType.DECIMAL.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= PDataType.DECIMAL.estimateByteSize(na));

        // If we don't remove trailing zeros, this fails
        na = new BigDecimal(Double.parseDouble("96.45238095238095"));
        String naStr = na.toString();
        assertTrue(naStr != null);
        b = PDataType.DECIMAL.toBytes(na);
        nb = (BigDecimal)PDataType.DECIMAL.toObject(b);
        TestUtil.assertRoundEquals(na,nb);
        assertTrue(b.length <= PDataType.DECIMAL.estimateByteSize(na));
        
        // If we don't remove trailing zeros, this fails
        na = new BigDecimal(-1000);
        b = PDataType.DECIMAL.toBytes(na);
        nb = (BigDecimal)PDataType.DECIMAL.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= PDataType.DECIMAL.estimateByteSize(na));

        na = TestUtil.computeAverage(11000, 3);
        b = PDataType.DECIMAL.toBytes(na);
        nb = (BigDecimal)PDataType.DECIMAL.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= PDataType.DECIMAL.estimateByteSize(na));

        na = new BigDecimal(new BigInteger("12345678901239999"), 2);
        b = PDataType.DECIMAL.toBytes(na);
        nb = (BigDecimal)PDataType.DECIMAL.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= PDataType.DECIMAL.estimateByteSize(na));
        
        na = new BigDecimal(1);
        nb = new BigDecimal(-1);
        byte[] ba = PDataType.DECIMAL.toBytes(na);
        byte[] bb = PDataType.DECIMAL.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        assertTrue(ba.length <= PDataType.DECIMAL.estimateByteSize(na));
        assertTrue(bb.length <= PDataType.DECIMAL.estimateByteSize(nb));

        na = new BigDecimal(-1);
        nb = new BigDecimal(-2);
        ba = PDataType.DECIMAL.toBytes(na);
        bb = PDataType.DECIMAL.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        assertTrue(ba.length <= PDataType.DECIMAL.estimateByteSize(na));
        assertTrue(bb.length <= PDataType.DECIMAL.estimateByteSize(nb));
        
        na = new BigDecimal(-3);
        nb = new BigDecimal(-1000);
        assertTrue(na.compareTo(nb) > 0);
        ba = PDataType.DECIMAL.toBytes(na);
        bb = PDataType.DECIMAL.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        assertTrue(ba.length <= PDataType.DECIMAL.estimateByteSize(na));
        assertTrue(bb.length <= PDataType.DECIMAL.estimateByteSize(nb));
        
        na = new BigDecimal(BigInteger.valueOf(12345678901239998L), 2);
        nb = new BigDecimal(97);
        assertTrue(na.compareTo(nb) > 0);
        ba = PDataType.DECIMAL.toBytes(na);
        bb = PDataType.DECIMAL.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        assertTrue(ba.length <= PDataType.DECIMAL.estimateByteSize(na));
        assertTrue(bb.length <= PDataType.DECIMAL.estimateByteSize(nb));
        
        List<BigDecimal> values = Arrays.asList(new BigDecimal[] {
            new BigDecimal(-1000),
            new BigDecimal(-100000000),
            new BigDecimal(1000),
            new BigDecimal("-0.001"),
            new BigDecimal("0.001"),
            new BigDecimal(new BigInteger("12345678901239999"), 2),
            new BigDecimal(new BigInteger("12345678901239998"), 2),
            new BigDecimal(new BigInteger("12345678901239998123456789"), 2), // bigger than long
            new BigDecimal(new BigInteger("-1000"),3),
            new BigDecimal(new BigInteger("-1000"),10),
            new BigDecimal(99),
            new BigDecimal(97),
            new BigDecimal(-3)
        });
        
        List<byte[]> byteValues = new ArrayList<byte[]>();
        for (int i = 0; i < values.size(); i++) {
            byteValues.add(PDataType.DECIMAL.toBytes(values.get(i)));
        }
        
        for (int i = 0; i < values.size(); i++) {
            BigDecimal expected = values.get(i);
            BigDecimal actual = (BigDecimal)PDataType.DECIMAL.toObject(byteValues.get(i));
            assertTrue("For " + i + " expected " + expected + " but got " + actual,expected.round(PDataType.DEFAULT_MATH_CONTEXT).compareTo(actual.round(PDataType.DEFAULT_MATH_CONTEXT)) == 0);
            assertTrue(byteValues.get(i).length <= PDataType.DECIMAL.estimateByteSize(expected));
        }

        Collections.sort(values);
        Collections.sort(byteValues, Bytes.BYTES_COMPARATOR);
        
        for (int i = 0; i < values.size(); i++) {
            BigDecimal expected = values.get(i);
            byte[] bytes = PDataType.DECIMAL.toBytes(values.get(i));
            assertNotNull("bytes converted from values should not be null!", bytes);
            BigDecimal actual = (BigDecimal)PDataType.DECIMAL.toObject(byteValues.get(i));
            assertTrue("For " + i + " expected " + expected + " but got " + actual,expected.round(PDataType.DEFAULT_MATH_CONTEXT).compareTo(actual.round(PDataType.DEFAULT_MATH_CONTEXT))==0);
        }
        
    }
    
    @Test
    public void testEmptyString() throws Throwable {
        byte[] b1 = PDataType.VARCHAR.toBytes("");
        byte[] b2 = PDataType.VARCHAR.toBytes(null);
        assert (b1.length == 0 && Bytes.compareTo(b1, b2) == 0);
    }
    
    @Test
    public void testNull() throws Throwable {
        byte[] b = new byte[8];
        for (PDataType type : PDataType.values()) {
            try {
               type.toBytes(null);
               type.toBytes(null, b, 0);
               type.toObject(new byte[0],0,0);
               type.toObject(new byte[0],0,0, type);
            } catch (ConstraintViolationException e) {
                // Fixed width types do not support the concept of a "null" value.
                if (! (type.isFixedWidth() && e.getMessage().contains("may not be null"))) {
                    fail(type + ":" + e);
                }
            }
        }
    }

    @Test
    public void testValueCoersion() throws Exception {
        // Testing coercing integer to other values.
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.LONG));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.LONG, 10));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.LONG, 0));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.LONG, -10));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_INT));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_INT, 10));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_INT, 0));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_INT, -10));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_LONG));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_LONG, 10));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_LONG, 0));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_LONG, -10));
        
        // Testing coercing long to other values.
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.INTEGER));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, Long.MAX_VALUE));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, Integer.MAX_VALUE + 10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, (long)Integer.MAX_VALUE));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, Integer.MAX_VALUE - 10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, 10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, 0L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, -10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, Integer.MIN_VALUE + 10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, (long)Integer.MIN_VALUE));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, Integer.MIN_VALUE - 10L));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, Long.MIN_VALUE));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_INT));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_INT, 10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_INT, 0L));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_INT, -10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_LONG));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_LONG, Long.MAX_VALUE));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_LONG, 10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_LONG, 0L));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_LONG, -10L));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_LONG, Long.MIN_VALUE));
        
        // Testing coercing unsigned_int to other values.
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.INTEGER));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.INTEGER, 10));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.INTEGER, 0));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.LONG));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.LONG, 10));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.LONG, 0));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_LONG));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_LONG, 10));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_LONG, 0));
        
        // Testing coercing unsigned_long to other values.
        assertFalse(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.INTEGER));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.INTEGER, 10L));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.INTEGER, 0L));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.LONG));
        assertFalse(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.UNSIGNED_INT));
    }
}
