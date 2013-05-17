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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
        
        assertEquals(1, PDataType.LONG.compareTo(1L, -1L, PDataType.LONG));
    }

    @Test
    public void testRawLong() {
        Long la = 4L;
        byte[] b = PDataType.RAW_LONG.toBytes(la);
        Long lb = (Long)PDataType.RAW_LONG.toObject(b);
        assertEquals(la,lb);

        Long na = 1L;
        Long nb = -1L;
        byte[] ba = PDataType.RAW_LONG.toBytes(na);
        byte[] bb = PDataType.RAW_LONG.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);

        assertEquals(PDataType.RAW_LONG.compareTo(1L, -1L, PDataType.LONG),1);
    
        byte[] bRAW_LONG_M = PDataType.RAW_LONG.toBytes(-1L);
        byte[] bRAW_LONG_P = PDataType.RAW_LONG.toBytes(1L);
        byte[] bLONG_M = PDataType.LONG.toBytes(-1L);
        byte[] bLONG_P = PDataType.LONG.toBytes(1L);
        assertEquals(0, PDataType.RAW_LONG.compareTo(bRAW_LONG_P, 0, 8, null, bLONG_P, 0, 8, null, PDataType.LONG));
        assertEquals(1, PDataType.RAW_LONG.compareTo(bRAW_LONG_P, 0, 8, null, bLONG_M, 0, 8, null, PDataType.LONG));
        assertEquals(-1, PDataType.RAW_LONG.compareTo(bRAW_LONG_M, 0, 8, null, bLONG_P, 0, 8, null, PDataType.LONG));
        assertEquals(0, PDataType.RAW_LONG.compareTo(bRAW_LONG_M, 0, 8, null, bLONG_M, 0, 8, null, PDataType.LONG));

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

        na = new BigDecimal("1000.5829999999999913");
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


        {
            String[] strs ={
                    "\\xC2\\x03\\x0C\\x10\\x01\\x01\\x01\\x01\\x01\\x019U#\\x13W\\x09\\x09"
                    ,"\\xC2\\x03<,ddddddN\\x1B\\x1B!.9N"
                    ,"\\xC2\\x039"
                    ,"\\xC2\\x03\\x16,\\x01\\x01\\x01\\x01\\x01\\x01E\\x16\\x16\\x03@\\x1EG"
                    ,"\\xC2\\x02d6dddddd\\x15*]\\x0E<1F"
                    ,"\\xC2\\x04 3"
                    ,"\\xC2\\x03$Ldddddd\\x0A\\x06\\x06\\x1ES\\x1C\\x08"
                    ,"\\xC2\\x03\\x1E\\x0A\\x01\\x01\\x01\\x01\\x01\\x01#\\x0B=4 AV"
                    ,"\\xC2\\x02\\\\x04dddddd\\x15*]\\x0E<1F"
                    ,"\\xC2\\x02V\"\\x01\\x01\\x01\\x01\\x01\\x02\\x1A\\x068\\x162&O"
            };
            for (String str : strs) {
                byte[] bytes = Bytes.toBytesBinary(str);
                Object o = PDataType.DECIMAL.toObject(bytes);
                assertNotNull(o);
                //System.out.println(o.getClass() +" " + bytesToHex(bytes)+" " + o+" ");
            }
        }
    }
    public static String bytesToHex(byte[] bytes) {
        final char[] hexArray = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
        char[] hexChars = new char[bytes.length * 2];
        int v;
        for ( int j = 0; j < bytes.length; j++ ) {
            v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
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
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.RAW_LONG));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.RAW_LONG, 10));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.RAW_LONG, 0));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.RAW_LONG, -10));
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
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.RAW_LONG));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.RAW_LONG, 10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.RAW_LONG, 0L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.RAW_LONG, -10L));

        // Testing coercing raw long to other values.
        assertFalse(PDataType.RAW_LONG.isCoercibleTo(PDataType.INTEGER));
        assertFalse(PDataType.RAW_LONG.isCoercibleTo(PDataType.INTEGER, Long.MAX_VALUE));
        assertFalse(PDataType.RAW_LONG.isCoercibleTo(PDataType.INTEGER, Integer.MAX_VALUE + 10L));
        assertTrue(PDataType.RAW_LONG.isCoercibleTo(PDataType.INTEGER, (long)Integer.MAX_VALUE));
        assertTrue(PDataType.RAW_LONG.isCoercibleTo(PDataType.INTEGER, Integer.MAX_VALUE - 10L));
        assertTrue(PDataType.RAW_LONG.isCoercibleTo(PDataType.INTEGER, 10L));
        assertTrue(PDataType.RAW_LONG.isCoercibleTo(PDataType.INTEGER, 0L));
        assertTrue(PDataType.RAW_LONG.isCoercibleTo(PDataType.INTEGER, -10L));
        assertTrue(PDataType.RAW_LONG.isCoercibleTo(PDataType.INTEGER, Integer.MIN_VALUE + 10L));
        assertTrue(PDataType.RAW_LONG.isCoercibleTo(PDataType.INTEGER, (long)Integer.MIN_VALUE));
        assertFalse(PDataType.RAW_LONG.isCoercibleTo(PDataType.INTEGER, Integer.MIN_VALUE - 10L));
        assertFalse(PDataType.RAW_LONG.isCoercibleTo(PDataType.INTEGER, Long.MIN_VALUE));
        assertFalse(PDataType.RAW_LONG.isCoercibleTo(PDataType.UNSIGNED_INT));
        assertTrue(PDataType.RAW_LONG.isCoercibleTo(PDataType.UNSIGNED_INT, 10L));
        assertTrue(PDataType.RAW_LONG.isCoercibleTo(PDataType.UNSIGNED_INT, 0L));
        assertFalse(PDataType.RAW_LONG.isCoercibleTo(PDataType.UNSIGNED_INT, -10L));
        assertTrue(PDataType.RAW_LONG.isCoercibleTo(PDataType.UNSIGNED_LONG));
        assertTrue(PDataType.RAW_LONG.isCoercibleTo(PDataType.UNSIGNED_LONG, Long.MAX_VALUE));
        assertTrue(PDataType.RAW_LONG.isCoercibleTo(PDataType.UNSIGNED_LONG, 10L));
        assertTrue(PDataType.RAW_LONG.isCoercibleTo(PDataType.UNSIGNED_LONG, 0L));
        assertFalse(PDataType.RAW_LONG.isCoercibleTo(PDataType.UNSIGNED_LONG, -10L));
        assertFalse(PDataType.RAW_LONG.isCoercibleTo(PDataType.UNSIGNED_LONG, Long.MIN_VALUE));
        assertTrue(PDataType.RAW_LONG.isCoercibleTo(PDataType.LONG));
        assertTrue(PDataType.RAW_LONG.isCoercibleTo(PDataType.LONG, 10L));
        assertTrue(PDataType.RAW_LONG.isCoercibleTo(PDataType.LONG, 0L));
        assertTrue(PDataType.RAW_LONG.isCoercibleTo(PDataType.LONG, -10L));

        // Testing coercing unsigned_int to other values.
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.INTEGER));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.INTEGER, 10));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.INTEGER, 0));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.LONG));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.LONG, 10));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.LONG, 0));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.RAW_LONG));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.RAW_LONG, 10));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.RAW_LONG, 0));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_LONG));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_LONG, 10));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_LONG, 0));

        // Testing coercing unsigned_long to other values.
        assertFalse(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.INTEGER));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.INTEGER, 10L));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.INTEGER, 0L));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.LONG));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.RAW_LONG));
        assertFalse(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.UNSIGNED_INT));
        
        // Testing coercing Date types
        assertTrue(PDataType.DATE.isCoercibleTo(PDataType.TIMESTAMP));
        assertTrue(PDataType.DATE.isCoercibleTo(PDataType.TIME));
        assertTrue(PDataType.TIMESTAMP.isCoercibleTo(PDataType.DATE));
        assertTrue(PDataType.TIMESTAMP.isCoercibleTo(PDataType.TIME));
        assertTrue(PDataType.TIME.isCoercibleTo(PDataType.TIMESTAMP));
        assertTrue(PDataType.TIME.isCoercibleTo(PDataType.DATE));
    }

    @Test
    public void testGetDeicmalPrecisionAndScaleFromRawBytes() throws Exception {
        // Special case for 0.
        BigDecimal bd = new BigDecimal("0");
        byte[] b = PDataType.DECIMAL.toBytes(bd);
        int[] v = PDataType.getDecimalPrecisionAndScale(b, 0, b.length);
        assertEquals(0, v[0]);
        assertEquals(0, v[1]);

        BigDecimal[] bds = new BigDecimal[] {
                new BigDecimal("1"),
                new BigDecimal("0.11"),
                new BigDecimal("1.1"),
                new BigDecimal("11"),
                new BigDecimal("101"),
                new BigDecimal("10.1"),
                new BigDecimal("1.01"),
                new BigDecimal("0.101"),
                new BigDecimal("1001"),
                new BigDecimal("100.1"),
                new BigDecimal("10.01"),
                new BigDecimal("1.001"),
                new BigDecimal("0.1001"),
                new BigDecimal("10001"),
                new BigDecimal("1000.1"),
                new BigDecimal("100.01"),
                new BigDecimal("10.001"),
                new BigDecimal("1.0001"),
                new BigDecimal("0.10001"),
                new BigDecimal("100000000000000000000000000000"),
                new BigDecimal("1000000000000000000000000000000"),
                new BigDecimal("0.000000000000000000000000000001"),
                new BigDecimal("0.0000000000000000000000000000001"),
                new BigDecimal("111111111111111111111111111111"),
                new BigDecimal("1111111111111111111111111111111"),
                new BigDecimal("0.111111111111111111111111111111"),
                new BigDecimal("0.1111111111111111111111111111111"),
        };

        for (int i=0; i<bds.length; i++) {
            testReadDecimalPrecisionAndScaleFromRawBytes(bds[i]);
            testReadDecimalPrecisionAndScaleFromRawBytes(bds[i].negate());
        }
    }
    
    @Test
    public void testDateConversions() {
        long now = System.currentTimeMillis();
        Date date = new Date(now);
        Time t = new Time(now);
        Timestamp ts = new Timestamp(now);
        
        Object o = PDataType.DATE.toObject(ts, PDataType.TIMESTAMP);
        assertEquals(o.getClass(), java.sql.Date.class);
        o = PDataType.DATE.toObject(t, PDataType.TIME);
        assertEquals(o.getClass(), java.sql.Date.class);
        
        o = PDataType.TIME.toObject(date, PDataType.DATE);
        assertEquals(o.getClass(), java.sql.Time.class);
        o = PDataType.TIME.toObject(ts, PDataType.TIMESTAMP);
        assertEquals(o.getClass(), java.sql.Time.class);
                
        o = PDataType.TIMESTAMP.toObject(date, PDataType.DATE);
        assertEquals(o.getClass(), java.sql.Timestamp.class);
        o = PDataType.TIMESTAMP.toObject(t, PDataType.TIME);
        assertEquals(o.getClass(), java.sql.Timestamp.class); 
    }

    private void testReadDecimalPrecisionAndScaleFromRawBytes(BigDecimal bd) {
        byte[] b = PDataType.DECIMAL.toBytes(bd);
        int[] v = PDataType.getDecimalPrecisionAndScale(b, 0, b.length);
        assertEquals(bd.toString(), bd.precision(), v[0]);
        assertEquals(bd.toString(), bd.scale(), v[1]);
    }
}
