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
package phoenix.expression;

import java.io.*;
import java.util.List;

import org.apache.hadoop.io.WritableUtils;

import phoenix.expression.visitor.ExpressionVisitor;

import com.google.common.collect.ImmutableList;


/**
 * 
 * Base class for expressions which have a single child expression
 *
 * @author jtaylor
 * @since 0.1
 */
public abstract class BaseSingleExpression extends BaseExpression {

    protected List<Expression> children;
    
    public BaseSingleExpression() {
    }

    public BaseSingleExpression(Expression expression) {
        this.children = ImmutableList.of(expression);
    }

    @Override
    public List<Expression> getChildren() {
        return children;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        Expression expression = ExpressionType.values()[WritableUtils.readVInt(input)].newInstance();
        expression.readFields(input);
        children = ImmutableList.of(expression);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        WritableUtils.writeVInt(output, ExpressionType.valueOf(children.get(0)).ordinal());
        children.get(0).write(output);
    }

    @Override
    public boolean isNullable() {
        return children.get(0).isNullable();
    }

    @Override
    public void reset() {
        children.get(0).reset();
    }

    final <T> void acceptChild(ExpressionVisitor<T> visitor)  {
        children.get(0).accept(visitor);
    }

    public Expression getChild() {
        return children.get(0);
    }
}
