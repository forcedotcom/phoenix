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
package com.salesforce.phoenix.util;

import static org.junit.Assert.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.ScanKeyOverflowException;


public class ByteUtilTest {

    @Test
    public void testSplitBytes() {
        byte[] startRow = Bytes.toBytes("EA");
        byte[] stopRow = Bytes.toBytes("EZ");
        byte[][] splitPoints = Bytes.split(startRow, stopRow, 10);
        for (byte[] splitPoint : splitPoints) {
            assertTrue(Bytes.toStringBinary(splitPoint), Bytes.compareTo(startRow, splitPoint) <= 0);
            assertTrue(Bytes.toStringBinary(splitPoint), Bytes.compareTo(stopRow, splitPoint) >= 0);
        }
    }
    
    @Test
    public void testVIntToBytes() {
        for (int i = -10000; i <= 10000; i++) {
            byte[] vint = Bytes.vintToBytes(i);
            int vintSize = vint.length;
            byte[] vint2 = new byte[vint.length];
            assertEquals(vintSize, ByteUtil.vintToBytes(vint2, 0, i));
            assertTrue(Bytes.BYTES_COMPARATOR.compare(vint,vint2) == 0);
        }
    }
    
    @Test
    public void testNextKey() {
        byte[] key = new byte[] {1};
        assertEquals((byte)2, ByteUtil.nextKey(key)[0]); 
        key = new byte[] {1, (byte)255};
        byte[] nextKey = ByteUtil.nextKey(key);
        byte[] expectedKey = new byte[] {2,(byte)0};
        assertTrue(Bytes.compareTo(expectedKey, nextKey) == 0); 
        key = ByteUtil.concat(Bytes.toBytes("00D300000000XHP"), PDataType.INTEGER.toBytes(Integer.MAX_VALUE));
        nextKey = ByteUtil.nextKey(key);
        expectedKey = ByteUtil.concat(Bytes.toBytes("00D300000000XHQ"), PDataType.INTEGER.toBytes(Integer.MIN_VALUE));
        assertTrue(Bytes.compareTo(expectedKey, nextKey) == 0);
        
        try {
            key = new byte[] {(byte)255};
            ByteUtil.nextKey(key);
            fail();
        } catch (ScanKeyOverflowException e) {
            assertTrue(e.getMessage().contains("Overflow trying to get next key"));
        }
    }
}
