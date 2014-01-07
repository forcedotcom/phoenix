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

import java.util.List;

import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.TupleUtil;


/**
 * 
 * ResultIterator that does a merge sort on the list of iterators provided,
 * returning the rows in row key ascending order. The iterators provided
 * must be in row key ascending order.
 *
 * @author jtaylor
 * @since 0.1
 */
public class MergeSortRowKeyResultIterator extends MergeSortResultIterator {
    private final int keyOffset;
    private final int factor;
    
    public MergeSortRowKeyResultIterator(ResultIterators iterators) {
        this(iterators, 0, false);
    }
    
    public MergeSortRowKeyResultIterator(ResultIterators iterators, int keyOffset, boolean isReverse) {
        super(iterators);
        this.keyOffset = keyOffset;
        this.factor = isReverse ? -1 : 1;
    }
   
    @Override
    protected int compare(Tuple t1, Tuple t2) {
        return factor * TupleUtil.compare(t1, t2, tempPtr, keyOffset);
    }

    @Override
    public void explain(List<String> planSteps) {
        resultIterators.explain(planSteps);
        planSteps.add("CLIENT MERGE SORT");
    }
}
