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
package com.salesforce.phoenix.filter;

import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.expression.Expression;


/**
 * 
 * Filter that evaluates WHERE clause expression, used in the case where there
 * are references to multiple column qualifiers over multiple column families.
 *
 * @author jtaylor
 * @since 0.1
 */
public class MultiCFCQKeyValueComparisonFilter extends MultiKeyValueComparisonFilter {
    private final ImmutablePairBytesPtr ptr = new ImmutablePairBytesPtr();
    private TreeSet<byte[]> cfSet;
    
    public MultiCFCQKeyValueComparisonFilter() {
    }

    public MultiCFCQKeyValueComparisonFilter(Expression expression) {
        super(expression);
    }

    @Override
    protected void init() {
        cfSet = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        super.init();
    }
    
    @Override
    protected Object setColumnKey(byte[] cf, int cfOffset, int cfLength,
            byte[] cq, int cqOffset, int cqLength) {
        ptr.set(cf, cfOffset, cfLength, cq, cqOffset, cqLength);
        return ptr;
    }

    @Override
    protected Object newColumnKey(byte[] cf, int cfOffset, int cfLength, 
            byte[] cq, int cqOffset, int cqLength) {

        byte[] cfKey;
        if (cfOffset == 0 && cf.length == cfLength) {
            cfKey = cf;
        } else {
            // Copy bytes here, but figure cf names are typically a few bytes at most,
            // so this will be better than creating an ImmutableBytesPtr
            cfKey = new byte[cfLength];
            System.arraycopy(cf, cfOffset, cfKey, 0, cfLength);
        }
        cfSet.add(cfKey);
        return new ImmutablePairBytesPtr(cf, cfOffset, cfLength, cq, cqOffset, cqLength);
    }

    private static class ImmutablePairBytesPtr {
        private byte[] bytes1;
        private int offset1;
        private int length1;
        private byte[] bytes2;
        private int offset2;
        private int length2;
        private int hashCode;
        
        private ImmutablePairBytesPtr() {
        }

        private ImmutablePairBytesPtr(byte[] bytes1, int offset1, int length1, byte[] bytes2, int offset2, int length2) {
            set(bytes1, offset1, length1, bytes2, offset2, length2);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        public void set(byte[] bytes1, int offset1, int length1, byte[] bytes2, int offset2, int length2) {
            this.bytes1 = bytes1;
            this.offset1 = offset1;
            this.length1 = length1;
            this.bytes2 = bytes2;
            this.offset2 = offset2;
            this.length2 = length2;
            int hash = 1;
            for (int i = offset1; i < offset1 + length1; i++)
                hash = (31 * hash) + bytes1[i];
            for (int i = offset2; i < offset2 + length2; i++)
                hash = (31 * hash) + bytes2[i];
            hashCode = hash;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            ImmutablePairBytesPtr that = (ImmutablePairBytesPtr)obj;
            if (this.hashCode != that.hashCode) return false;
            if (Bytes.compareTo(this.bytes2, this.offset2, this.length2, that.bytes2, that.offset2, that.length2) != 0) return false;
            if (Bytes.compareTo(this.bytes1, this.offset1, this.length1, that.bytes1, that.offset1, that.length1) != 0) return false;
            return true;
        }
    }

    
    @Override
    public boolean isFamilyEssential(byte[] name) {
        // Only the column families involved in the expression are essential.
        // The others are for columns projected in the select expression.
        return cfSet.contains(name);
    }
}
