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
import static org.junit.Assert.assertNull;

import java.io.*;

import org.junit.Test;

import com.salesforce.phoenix.schema.*;

public class ColumnExpressionTest {

    @Test
    public void testSerialization() throws Exception {
        int maxLen = 30;
        int scale = 5;
        PColumn column = new PColumnImpl(new PNameImpl("c1"), new PNameImpl("f1"), PDataType.DECIMAL, maxLen, scale,
                true, 20, null);
        ColumnExpression colExp = new KeyValueColumnExpression(column);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dOut = new DataOutputStream(baos);
        colExp.write(dOut);
        dOut.flush();

        ColumnExpression colExp2 = new KeyValueColumnExpression();
        byte[] bytes = baos.toByteArray();
        DataInputStream dIn = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length));
        colExp2.readFields(dIn);
        assertEquals(maxLen, colExp2.getMaxLength().intValue());
        assertEquals(scale, colExp2.getScale().intValue());
        assertEquals(PDataType.DECIMAL, colExp2.getDataType());
    }

    @Test
    public void testSerializationWithNullScale() throws Exception {
        int maxLen = 30;
        PColumn column = new PColumnImpl(new PNameImpl("c1"), new PNameImpl("f1"), PDataType.BINARY, maxLen, null,
                true, 20, null);
        ColumnExpression colExp = new KeyValueColumnExpression(column);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dOut = new DataOutputStream(baos);
        colExp.write(dOut);
        dOut.flush();

        ColumnExpression colExp2 = new KeyValueColumnExpression();
        byte[] bytes = baos.toByteArray();
        DataInputStream dIn = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length));
        colExp2.readFields(dIn);
        assertEquals(maxLen, colExp2.getMaxLength().intValue());
        assertNull(colExp2.getScale());
        assertEquals(PDataType.BINARY, colExp2.getDataType());
    }

    @Test
    public void testSerializationWithNullMaxLength() throws Exception {
        int scale = 5;
        PColumn column = new PColumnImpl(new PNameImpl("c1"), new PNameImpl("f1"), PDataType.VARCHAR, null, scale,
                true, 20, null);
        ColumnExpression colExp = new KeyValueColumnExpression(column);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dOut = new DataOutputStream(baos);
        colExp.write(dOut);
        dOut.flush();

        ColumnExpression colExp2 = new KeyValueColumnExpression();
        byte[] bytes = baos.toByteArray();
        DataInputStream dIn = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length));
        colExp2.readFields(dIn);
        assertNull(colExp2.getMaxLength());
        assertEquals(scale, colExp2.getScale().intValue());
        assertEquals(PDataType.VARCHAR, colExp2.getDataType());
    }

    @Test
    public void testSerializationWithNullScaleAndMaxLength() throws Exception {
        PColumn column = new PColumnImpl(new PNameImpl("c1"), new PNameImpl("f1"), PDataType.DECIMAL, null, null, true,
                20, null);
        ColumnExpression colExp = new KeyValueColumnExpression(column);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dOut = new DataOutputStream(baos);
        colExp.write(dOut);
        dOut.flush();

        ColumnExpression colExp2 = new KeyValueColumnExpression();
        byte[] bytes = baos.toByteArray();
        DataInputStream dIn = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length));
        colExp2.readFields(dIn);
        assertNull(colExp2.getMaxLength());
        assertNull(colExp2.getScale());
    }
}
