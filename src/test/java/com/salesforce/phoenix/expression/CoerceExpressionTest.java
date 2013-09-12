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

import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Test;

import com.salesforce.phoenix.schema.PDataType;

/**
 * Test class for unit-testing {@link CoerceExpression}
 * 
 * @author samarth.jain
 * @since 0.1
 * 
 */
public class CoerceExpressionTest {
    
	private static final HashMap<Class, Object> map = new HashMap<Class, Object>();
	
	static {
		map.put(String.class, "a");
		map.put(Long.class, 1l);	
		map.put(Integer.class, 1);
		map.put(Short.class, 1);
		map.put(Byte.class, 1);
		map.put(Float.class, 1.00f);
		map.put(Double.class, 1.00d);
		map.put(BigDecimal.class, BigDecimal.ONE);
		map.put(Timestamp.class, new Timestamp(0));
		map.put(Time.class, new Time(0));
		map.put(Date.class, new Date(0));
		map.put(Boolean.class, Boolean.TRUE);
		map.put(byte[].class, new byte[]{-128, 0, 0, 1});
	}
	
	@Test
    public void testCoerceExpressionSupportsCoercingIntToDecimal() throws Exception {
        LiteralExpression v = LiteralExpression.newConstant(1, PDataType.INTEGER);
        CoerceExpression e = new CoerceExpression(v, PDataType.DECIMAL);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        e.evaluate(null, ptr);
        Object obj = e.getDataType().toObject(ptr);
        assertTrue(obj instanceof BigDecimal);
        BigDecimal value = (BigDecimal)obj;
        assertTrue(value.equals(BigDecimal.valueOf(1)));
    }
	
	@Test
    public void testCoerceExpressionSupportsCoercingCharToVarchar() throws Exception {
        LiteralExpression v = LiteralExpression.newConstant("a", PDataType.CHAR);
        CoerceExpression e = new CoerceExpression(v, PDataType.VARCHAR);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        e.evaluate(null, ptr);
        Object obj = e.getDataType().toObject(ptr);
        assertTrue(obj instanceof String);
        String value = (String)obj;
        assertTrue(value.equals("a"));
    }
	
	@Test
    public void testCoerceExpressionSupportsCoercingIntToLong() throws Exception {
        LiteralExpression v = LiteralExpression.newConstant(1, PDataType.INTEGER);
        CoerceExpression e = new CoerceExpression(v, PDataType.LONG);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        e.evaluate(null, ptr);
        Object obj = e.getDataType().toObject(ptr);
        assertTrue(obj instanceof Long);
        Long value = (Long)obj;
        assertTrue(value.equals(Long.valueOf(1)));
    }
	
	@Test
	public void testCoerceExpressionSupportsCoercingAllPDataTypesToVarBinary() throws Exception {
		for(PDataType p : PDataType.values()) {
			LiteralExpression v = LiteralExpression.newConstant(map.get(p.getJavaClass()), p);
			CoerceExpression e = new CoerceExpression(v, PDataType.VARBINARY);
			ImmutableBytesWritable ptr = new ImmutableBytesWritable();
			e.evaluate(null, ptr);
			Object obj = e.getDataType().toObject(ptr);
			assertTrue("Coercing to VARBINARY failed for PDataType " + p, obj instanceof byte[]);
		}
	}
	

	@Test
    public void testCoerceExpressionSupportsCoercingAllPDataTypesToBinary() throws Exception {
		for(PDataType p : PDataType.values()) {
			LiteralExpression v = LiteralExpression.newConstant(map.get(p.getJavaClass()), p);
			CoerceExpression e = new CoerceExpression(v, PDataType.BINARY);
			ImmutableBytesWritable ptr = new ImmutableBytesWritable();
			e.evaluate(null, ptr);
			Object obj = e.getDataType().toObject(ptr);
			assertTrue("Coercing to BINARY failed for PDataType " + p, obj instanceof byte[]);
		}
    }
	
}
