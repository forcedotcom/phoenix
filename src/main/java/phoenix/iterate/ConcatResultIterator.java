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
package phoenix.iterate;

import java.sql.SQLException;
import java.util.List;

import phoenix.schema.tuple.Tuple;

/**
 * 
 * Result iterator that concatenates a list of other iterators.
 *
 * @author jtaylor
 * @since 0.1
 */
public class ConcatResultIterator implements PeekingResultIterator {
    private final ResultIterators resultIterators;
    private List<PeekingResultIterator> iterators;
    private int index;
    
    public ConcatResultIterator(ResultIterators iterators) {
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
            for (;index < iterators.size(); index++) {
                PeekingResultIterator iterator = iterators.get(index);
                iterator.close();
            }
        }
    }


    @Override
    public void explain(List<String> planSteps) {
        resultIterators.explain(planSteps);
    }

    private PeekingResultIterator currentIterator() throws SQLException {
        List<PeekingResultIterator> iterators = getIterators();
        while (index < iterators.size()) {
            PeekingResultIterator iterator = iterators.get(index);
            Tuple r = iterator.peek();
            if (r != null) {
                return iterator;
            }
            iterator.close();
            index++;
        }
        return EMPTY_ITERATOR;
    }
    
    @Override
    public Tuple peek() throws SQLException {
        return currentIterator().peek();
    }

    @Override
    public Tuple next() throws SQLException {
        return currentIterator().next();
    }

}
