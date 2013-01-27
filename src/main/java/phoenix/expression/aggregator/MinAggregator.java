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
package phoenix.expression.aggregator;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import phoenix.schema.tuple.Tuple;
import phoenix.util.ByteUtil;
import phoenix.util.SizedUtil;

/**
 * Aggregator that finds the min of values. Inverse of {@link MaxAggregator}.
 *
 * @author syyang
 * @since 0.1
 */
abstract public class MinAggregator extends BaseAggregator {
    /** Used to store the accumulate the results of the MIN function */
    protected final ImmutableBytesWritable value = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);

    @Override
    public void reset() {
        value.set(ByteUtil.EMPTY_BYTE_ARRAY);
        super.reset();
    }

    @Override
    public int getSize() {
        return super.getSize() + /*value*/ SizedUtil.IMMUTABLE_BYTES_WRITABLE_SIZE;
    }

    /**
     * Compares two bytes writables, and returns true if the first one should be
     * kept, and false otherwise. For the MIN function, this method will return
     * true if the first bytes writable is less than the second.
     * 
     * @param ibw1 the first bytes writable
     * @param ibw2 the second bytes writable
     * @return true if the first bytes writable should be kept
     */
    protected boolean keepFirst(ImmutableBytesWritable ibw1, ImmutableBytesWritable ibw2) {
        return 0 >= getDataType().compareTo(ibw1, ibw2);
    }

    private boolean isNull() {
        return value.get() == ByteUtil.EMPTY_BYTE_ARRAY;
    }
    
    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (isNull()) {
            value.set(ptr.get(), ptr.getOffset(), ptr.getLength());
        } else {
            if (!keepFirst(value, ptr)) {
                // replace the value with the new value
                value.set(ptr.get(), ptr.getOffset(), ptr.getLength());
            }
        }
    }
    
    @Override
    public String toString() {
        return "MIN [value=" + Bytes.toStringBinary(value.get(),value.getOffset(),value.getLength()) + "]";
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (isNull()) {
            return false;
        }
        ptr.set(value.get(), value.getOffset(), value.getLength());
        return true;
    }
}
