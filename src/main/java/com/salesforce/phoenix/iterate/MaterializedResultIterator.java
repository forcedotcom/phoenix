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
import java.util.*;

import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Fully materialized result iterator backed by the result list provided.
 * No copy is made of the backing results collection.
 *
 * @author jtaylor
 * @since 0.1
 */
public class MaterializedResultIterator implements PeekingResultIterator {
    private final PeekingCollectionIterator iterator;
    
    public MaterializedResultIterator(Collection<Tuple> results) {
        iterator = new PeekingCollectionIterator(results);
    }
    
    @Override
    public void close() {
    }

    @Override
    public Tuple next() throws SQLException {
        return iterator.nextOrNull();
    }

    @Override
    public Tuple peek() throws SQLException {
        return iterator.peek();
    }

    private static class PeekingCollectionIterator implements Iterator<Tuple> {
        private final Iterator<Tuple> iterator;
        private int remaining;
        private Tuple current;            
        
        private PeekingCollectionIterator(Collection<Tuple> results) {
            iterator = results.iterator();
            remaining = results.size();
            advance();
        }
        
        private Tuple advance() {
            if (iterator.hasNext()) {
                current = iterator.next();
            } else {
                current = null;
            }
            return current;
        }
        
        @Override
        public boolean hasNext() {
            return current != null;
        }

        @Override
        public Tuple next() {
            Tuple next = nextOrNull();
            if (next == null) {
                throw new NoSuchElementException();
            }
            return next;
        }

        public Tuple nextOrNull() {
            if (current == null) {
                return null;
            }
            Tuple next = current;
            remaining--;
            advance();
            return next;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
        public Tuple peek() {
            return current;
        }

    }

    @Override
    public void explain(List<String> planSteps) {
    }
}
