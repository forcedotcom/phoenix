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
package com.salesforce.hbase.index.util;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class ImmutableBytesPtr extends ImmutableBytesWritable {
    private int hashCode;
    
    public ImmutableBytesPtr() {
    }

    public ImmutableBytesPtr(byte[] bytes) {
        super(bytes);
        hashCode = super.hashCode();
    }

    public ImmutableBytesPtr(ImmutableBytesWritable ibw) {
        super(ibw);
        hashCode = super.hashCode();
    }

    public ImmutableBytesPtr(byte[] bytes, int offset, int length) {
        super(bytes, offset, length);
        hashCode = super.hashCode();
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ImmutableBytesPtr that = (ImmutableBytesPtr)obj;
        if (this.hashCode != that.hashCode) return false;
        if (Bytes.compareTo(this.get(), this.getOffset(), this.getLength(), that.get(), that.getOffset(), that.getLength()) != 0) return false;
        return true;
    }

    public void set(ImmutableBytesWritable ptr) {
        set(ptr.get(),ptr.getOffset(),ptr.getLength());
      }

    /**
     * @param b Use passed bytes as backing array for this instance.
     */
    @Override
    public void set(final byte [] b) {
      super.set(b);
      hashCode = super.hashCode();
    }

    /**
     * @param b Use passed bytes as backing array for this instance.
     * @param offset
     * @param length
     */
    @Override
    public void set(final byte [] b, final int offset, final int length) {
        super.set(b,offset,length);
        hashCode = super.hashCode();
    }

    /**
     * @return the backing byte array, copying only if necessary
     */
    public byte[] copyBytesIfNecessary() {
        if (this.getOffset() == 0 && this.getLength() == this.get().length) {
            return this.get();
        }
        return this.copyBytes();
    }

}
