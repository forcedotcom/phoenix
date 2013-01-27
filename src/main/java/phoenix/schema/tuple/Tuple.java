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
package phoenix.schema.tuple;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * 
 * Interface representing an ordered list of KeyValues returned as the
 * result of a query. Each tuple represents a row (i.e. all its KeyValues
 * will have the same key), and each KeyValue represents a column value.
 *
 * @author jtaylor
 * @since 0.1
 */
public interface Tuple {
    /**
     * @return Number of KeyValues contained by the Tuple.
     */
    public int size();
    
    /**
     * Determines whether or not the Tuple is immutable (the typical case)
     * or will potentially have additional KeyValues added to it (the case
     * during filter evaluation when we see one KeyValue at a time).
     * @return true if Tuple is immutable and false otherwise.
     */
    public boolean isImmutable();
    
    /**
     * Get the row key for the Tuple
     * @param ptr the bytes pointer that will be updated to point to
     * the key buffer.
     */
    public void getKey(ImmutableBytesWritable ptr);
    
    /**
     * Get the KeyValue at the given index.
     * @param index the zero-based KeyValue index between 0 and {@link #size()} exclusive
     * @return the KeyValue at the given index
     * @throws IndexOutOfBoundsException if an invalid index is used
     */
    public KeyValue getValue(int index);
    
    /***
     * Get the KeyValue contained by the Tuple with the given family and
     * qualifier name.
     * @param family the column family of the KeyValue being retrieved
     * @param qualifier the column qualify of the KeyValue being retrieved
     * @return the KeyValue with the given family and qualifier name or
     * null if not found.
     */
    public KeyValue getValue(byte [] family, byte [] qualifier);
}
