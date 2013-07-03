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


/**
 * Utilities for computing an object's size.  All size measurements are in bytes.
 * Note, all of the sizes here, but especially OBJECT_SIZE and ARRAY_SIZE are estimates and will
 * depend on the JVM itself (and which JVM, 64bit vs. 32bit, etc).
 * The current values are based on:
 * Java HotSpot(TM) 64-Bit Server VM/14.2-b01
 * 
 * (Uncomment out and run the main w/ appropriate object to test)
 * Also, see this link:
 * https://sites.google.com/a/salesforce.com/development/Home/old-wiki-home-page/i-wish-i-knew#TOC-How-to-measure-the-size-of-a-Java-O
 * For another way to measure.
 */
public class SizedUtil {
    public static final int POINTER_SIZE = 8; // 64 bit jvm.
    public static final int OBJECT_SIZE = 16; // measured, see class comment.
    public static final int ARRAY_SIZE = 24; // measured, see class comment.
    public static final int CHAR_SIZE = 2;
    public static final int INT_SIZE = 4;
    public static final int LONG_SIZE = 8;
    
    public static final int MAP_ENTRY_SIZE = OBJECT_SIZE + 3 * POINTER_SIZE + INT_SIZE;
    public static final int IMMUTABLE_BYTES_WRITABLE_SIZE = OBJECT_SIZE + INT_SIZE * 2 + ARRAY_SIZE;
    public static final int IMMUTABLE_BYTES_PTR_SIZE = IMMUTABLE_BYTES_WRITABLE_SIZE + INT_SIZE;// Extra is an int field which caches hashcode.
    public static final int KEY_VALUE_SIZE = 2 * INT_SIZE + LONG_SIZE + 2 * ARRAY_SIZE;
    public static final int RESULT_SIZE = OBJECT_SIZE +  3 * POINTER_SIZE + IMMUTABLE_BYTES_WRITABLE_SIZE;
    public static final int INT_OBJECT_SIZE = INT_SIZE + OBJECT_SIZE;
    public static final int LONG_OBJECT_SIZE = LONG_SIZE + OBJECT_SIZE;
    public static final int BIG_DECIMAL_SIZE = 
        OBJECT_SIZE + 2 * INT_SIZE + LONG_SIZE + 2 * POINTER_SIZE +
        OBJECT_SIZE /* BigInteger */ + 5 * INT_SIZE + ARRAY_SIZE /*mag[]*/ + 2 * INT_SIZE /* est mag[2] */;

    private SizedUtil() {
    }
    
    public static int sizeOfMap(int nRows, int keySize, int valueSize) {
        return nRows * (
                SizedUtil.MAP_ENTRY_SIZE + // entry
                keySize + // key size
                valueSize); // value size
    }
}
