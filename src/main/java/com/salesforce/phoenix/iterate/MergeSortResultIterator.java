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
package com.salesforce.phoenix.iterate;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.SQLCloseables;


/**
 * 
 * Base class for a ResultIterator that does a merge sort on the list of iterators
 * provided.
 *
 * @author jtaylor
 * @since 1.2
 */
public abstract class MergeSortResultIterator implements PeekingResultIterator {
    protected final ResultIterators resultIterators;
    protected final ImmutableBytesWritable tempPtr = new ImmutableBytesWritable();
    private List<PeekingResultIterator> iterators;
    
    public MergeSortResultIterator(ResultIterators iterators) {
        this.resultIterators = iterators;
    }
    
    private List<PeekingResultIterator> getIterators() throws SQLException {
        if (iterators == null) {
            iterators = resultIterators.getIterators();
        }
        return iterators;
    }
    
    @Override
    public void close() throws SQLException {
        if (iterators != null) {
            SQLCloseables.closeAll(iterators);
        }
    }

    abstract protected int compare(Tuple t1, Tuple t2);
    
    private PeekingResultIterator minIterator() throws SQLException {
        List<PeekingResultIterator> iterators = getIterators();
        Tuple minResult = null;
        PeekingResultIterator minIterator = EMPTY_ITERATOR;
        for (int i = iterators.size()-1; i >= 0; i--) {
            PeekingResultIterator iterator = iterators.get(i);
            Tuple r = iterator.peek();
            if (r != null) {
                if (minResult == null || compare(r, minResult) < 0) {
                    minResult = r;
                    minIterator = iterator;
                }
                continue;
            }
            iterator.close();
            iterators.remove(i);
        }
        return minIterator;
    }
    
    @Override
    public Tuple peek() throws SQLException {
        PeekingResultIterator iterator = minIterator();
        return iterator.peek();
    }

    @Override
    public Tuple next() throws SQLException {
        PeekingResultIterator iterator = minIterator();
        return iterator.next();
    }
}
