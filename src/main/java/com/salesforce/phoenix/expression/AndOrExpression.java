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
package com.salesforce.phoenix.expression;

import java.util.BitSet;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Abstract expression implementation for compound AND and OR expressions
 *
 * @author jtaylor
 * @since 0.1
 */
public abstract class AndOrExpression extends BaseCompoundExpression {
    // Remember evaluation of child expression for partial evaluation
    private BitSet partialEvalState;
   
    public AndOrExpression() {
    }
    
    public AndOrExpression(List<Expression> children) {
        super(children);
    }
    
    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Boolean.valueOf(this.getStopValue()).hashCode();
    }

    @Override
    public PDataType getDataType() {
        return PDataType.BOOLEAN;
    }

    @Override
    public void reset() {
        if (partialEvalState == null) {
            partialEvalState = new BitSet(children.size());
        } else {
            partialEvalState.clear();
        }
        super.reset();
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        boolean isNull = false;
        boolean stopValue = getStopValue();
        for (int i = 0; i < children.size(); i++) {
            Expression child = children.get(i);
            // If partial state is available, then use that to know we've already evaluated this
            // child expression and do not need to do so again.
            if (partialEvalState == null || !partialEvalState.get(i)) {
                // Call through to child evaluate method matching parent call to allow child to optimize
                // evaluate versus getValue code path.
                if (child.evaluate(tuple, ptr)) {
                    // Short circuit if we see our stop value
                    if (PDataType.BOOLEAN.toObject(ptr, child.getDataType()).equals(Boolean.valueOf(stopValue))) {
                        return true;
                    } else if (partialEvalState != null) {
                        partialEvalState.set(i);
                    }
                } else {
                    isNull = true;
                }
            }
        }
        if (isNull) {
            return false;
        }
        return true;
    }

    protected abstract boolean getStopValue();
}
