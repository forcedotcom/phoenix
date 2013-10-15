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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 *
 * Filter for use when expressions only reference row key columns
 *
 * @author jtaylor
 * @since 0.1
 */
public class RowKeyComparisonFilter extends BooleanExpressionFilter {
    private static final Logger logger = LoggerFactory.getLogger(RowKeyComparisonFilter.class);

    private boolean keepRow = false;
    private RowKeyTuple inputTuple = new RowKeyTuple();
    private byte[] essentialCF;

    public RowKeyComparisonFilter() {
    }

    public RowKeyComparisonFilter(Expression expression, byte[] essentialCF) {
        super(expression);
        this.essentialCF = essentialCF;
    }

    @Override
    public void reset() {
        this.keepRow = false;
        super.reset();
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue v) {
        if(this.keepRow) {
            return ReturnCode.INCLUDE;
        }
        return ReturnCode.NEXT_ROW;
    }

    private final class RowKeyTuple implements Tuple {
        private byte[] buf;
        private int offset;
        private int length;

        public void setKey(byte[] buf, int offset, int length) {
            this.buf = buf;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public void getKey(ImmutableBytesWritable ptr) {
            ptr.set(buf, offset, length);
        }

        @Override
        public KeyValue getValue(byte[] cf, byte[] cq) {
            return null;
        }

        @Override
        public boolean isImmutable() {
            return true;
        }

        @Override
        public String toString() {
            return Bytes.toStringBinary(buf, offset, length);
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public KeyValue getValue(int index) {
            throw new IndexOutOfBoundsException(Integer.toString(index));
        }
    }

    @Override
    public boolean filterRowKey(final byte[] data, int offset, int length) {
        inputTuple.setKey(data, offset, length);
        this.keepRow = Boolean.TRUE.equals(evaluate(inputTuple));
        if (logger.isDebugEnabled()) {
            logger.debug("RowKeyComparisonFilter: " + (this.keepRow ? "KEEP" : "FILTER")  + " row " + inputTuple);
        }
        return !this.keepRow;
    }

    @Override
    public boolean filterRow() {
        return !this.keepRow;
    }

    @SuppressWarnings("all") // suppressing missing @Override since this doesn't exist for HBase 0.94.4
    public boolean isFamilyEssential(byte[] name) {
        // We only need our "guaranteed to have a key value" column family,
        // which we pass in and serialize through. In the case of a VIEW where
        // we don't have this, we have to say that all families are essential.
        return this.essentialCF.length == 0 ? true : Bytes.compareTo(this.essentialCF, name) == 0;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        this.essentialCF = WritableUtils.readCompressedByteArray(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeCompressedByteArray(output, this.essentialCF);
    }
}
