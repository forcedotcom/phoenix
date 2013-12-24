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

import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.expression.Expression;

/**
 *
 * Filter that evaluates WHERE clause expression, used in the case where there
 * are references to multiple column qualifiers over a single column family.
 *
 * @author jtaylor
 * @since 0.1
 */
public class MultiCQKeyValueComparisonFilter extends MultiKeyValueComparisonFilter {
    private ImmutableBytesPtr ptr = new ImmutableBytesPtr();
    private byte[] cf;

    public MultiCQKeyValueComparisonFilter() {
    }

    public MultiCQKeyValueComparisonFilter(Expression expression) {
        super(expression);
    }

    @Override
    protected Object setColumnKey(byte[] cf, int cfOffset, int cfLength, byte[] cq, int cqOffset,
            int cqLength) {
        ptr.set(cq, cqOffset, cqLength);
        return ptr;
    }

    @Override
    protected Object newColumnKey(byte[] cf, int cfOffset, int cfLength, byte[] cq, int cqOffset,
            int cqLength) {
        if (cfOffset == 0 && cf.length == cfLength) {
            this.cf = cf;
        } else {
            this.cf = new byte[cfLength];
            System.arraycopy(cf, cfOffset, this.cf, 0, cfLength);
        }
        return new ImmutableBytesPtr(cq, cqOffset, cqLength);
    }


    @SuppressWarnings("all") // suppressing missing @Override since this doesn't exist for HBase 0.94.4
    public boolean isFamilyEssential(byte[] name) {
        return Bytes.compareTo(cf, name) == 0;
    }
}
