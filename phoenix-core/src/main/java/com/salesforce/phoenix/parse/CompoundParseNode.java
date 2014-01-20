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
package com.salesforce.phoenix.parse;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;



/**
 * 
 * Abstract node representing an expression node that has children
 *
 * @author jtaylor
 * @since 0.1
 */
public abstract class CompoundParseNode extends ParseNode {
    private final List<ParseNode> children;
    private final boolean isStateless;
    
    CompoundParseNode(List<ParseNode> children) {
        this.children = Collections.unmodifiableList(children);
        boolean isStateless = true;
        for (ParseNode child : children) {
            isStateless &= child.isStateless();
            if (!isStateless) {
                break;
            }
        }
        this.isStateless = isStateless;
    }
    
    @Override
    public boolean isStateless() {
        return isStateless;
    }
    
    @Override
    public final List<ParseNode> getChildren() {
        return children;
    }


    final <T> List<T> acceptChildren(ParseNodeVisitor<T> visitor) throws SQLException {
        List<T> l = visitor.newElementList(children.size());        
        for (int i = 0; i < children.size(); i++) {
            T e = children.get(i).accept(visitor);
            visitor.addElement(l, e);
        }
        return l;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + children.toString();
    }
}
