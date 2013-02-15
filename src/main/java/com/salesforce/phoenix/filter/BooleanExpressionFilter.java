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

import java.io.*;

import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.ExpressionType;
import com.salesforce.phoenix.schema.IllegalDataException;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Base class for filters that use a boolean expression as
 * their means of evaluation.
 *
 * @author jtaylor
 * @since 0.1
 */
abstract public class BooleanExpressionFilter extends FilterBase {

    protected Expression expression;
    protected boolean evaluateOnCompletion;
    private ImmutableBytesWritable tempPtr = new ImmutableBytesWritable();
    
    public BooleanExpressionFilter() {
    }

    public BooleanExpressionFilter(Expression expression) {
        this.expression = expression;
    }

    protected void setEvaluateOnCompletion(boolean evaluateOnCompletion) {
        this.evaluateOnCompletion = evaluateOnCompletion;
    }
    
    protected boolean evaluateOnCompletion() {
        return evaluateOnCompletion;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + expression.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        BooleanExpressionFilter other = (BooleanExpressionFilter)obj;
        if (!expression.equals(other.expression)) return false;
        return true;
    }

    @Override
    public String toString() {
        return expression.toString();
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="NP_BOOLEAN_RETURN_NULL",
            justification="Returns null by design.")
    protected Boolean evaluate(Tuple input) {
        try {
            if (!expression.evaluate(input, tempPtr)) {
                return null;
            }
        } catch (IllegalDataException e) {
            return Boolean.FALSE;
        }
        return (Boolean)expression.getDataType().toObject(tempPtr);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        expression = ExpressionType.values()[WritableUtils.readVInt(input)].newInstance();
        expression.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        WritableUtils.writeVInt(output, ExpressionType.valueOf(expression).ordinal());
        expression.write(output);
    }
}
