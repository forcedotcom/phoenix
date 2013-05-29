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
package com.salesforce.phoenix.execute;

/**
 * Store values for column Mutation.
 * there are two type of values could be saved in a single MutationValue instance.
 * putValue for put mutaion, and incValue for a HTable.increment.
 * Upon mutation, putValue should be applied before incValue.
 * 
 * @author Raymond
 */

public class MutationValue {
    private byte[] putValue = null;
    private long incValue = 0;

    public MutationValue() {
        this.putValue = null;
        this.incValue = 0;
    }

    public MutationValue(byte[] putValue) {
        this.putValue = putValue;
        this.incValue = 0;
    }

    public MutationValue(byte[] putValue, long incValue) {
        this.putValue = putValue;
        this.incValue = incValue;
    }

    public byte[] getPutValue() {
        return putValue;
    }

    public long getIncValue() {
        return incValue;
    }

    public void setPutValue(byte[] value) {
        putValue = value;
    }

    public void setIncValue(long value) {
        incValue = value;
    }

    public boolean hasPutValue() {
        return !(putValue == null || putValue.length == 0);
    }
    
    public boolean hasIncValue() {
        return incValue != 0;
    }

    public boolean isEmpty() {
        return ((putValue == null || putValue.length == 0) &&
                (incValue == 0));
    }

    public void merge(MutationValue m) {
        
        if (m == null)
            return;

        // When merging new MutationValue, new putValue always reset current putValue and incValue at the same time.
        // While if only incValue is available in new MutaionValue, then it should accumulate with current one.
        if (m.hasPutValue()) {
            putValue = m.getPutValue();
            incValue = 0;
        }
        
        incValue += m.getIncValue();
    }
}
