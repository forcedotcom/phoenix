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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.ImmutableList;
import com.salesforce.phoenix.expression.visitor.ExpressionVisitor;


public abstract class BaseCompoundExpression extends BaseExpression {
    protected List<Expression> children;
    private boolean isNullable;
   
    public BaseCompoundExpression() {
    }
    
    public BaseCompoundExpression(List<? extends Expression> children) {
        this.children = ImmutableList.copyOf(children);
        for (int i = 0; i < children.size(); i++) {
            Expression child = children.get(i);
            if (child.isNullable()) {
                isNullable = true;
            }
        }
    }
    
    @Override
    public List<Expression> getChildren() {
        return children;
    }
    
    @Override
    public boolean isNullable() {
        return isNullable;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + children.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        BaseCompoundExpression other = (BaseCompoundExpression)obj;
        if (!children.equals(other.children)) return false;
        return true;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        int len = WritableUtils.readVInt(input);
        List<Expression>children = new ArrayList<Expression>(len);
        for (int i = 0; i < len; i++) {
            Expression child = ExpressionType.values()[WritableUtils.readVInt(input)].newInstance();
            child.readFields(input);
            isNullable |= child.isNullable();
            children.add(child);
        }
        this.children = ImmutableList.copyOf(children);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        WritableUtils.writeVInt(output, children.size());
        for (int i = 0; i < children.size(); i++) {
            Expression child = children.get(i);
            WritableUtils.writeVInt(output, ExpressionType.valueOf(child).ordinal());
            child.write(output);
        }
    }

    @Override
    public void reset() {
        for (int i = 0; i < children.size(); i++) {
            children.get(i).reset();
        }
    }
    
    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        List<T> l = acceptChildren(visitor, visitor.visitEnter(this));
        T t = visitor.visitLeave(this, l);
        if (t == null) {
            t = visitor.defaultReturn(this, l);
        }
        return t;
    }
    
    @Override
    public String toString() {
        return this.getClass().getName() + " [children=" + children + "]";
    }
}
