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
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;



/**
 * 
 * Base class for parse node visitors.
 *
 * @author jtaylor
 * @since 0.1
 */
public abstract class BaseParseNodeVisitor<E> implements ParseNodeVisitor<E> {

    /**
     * Fall through visitEnter method. Anything coming through
     * here means that a more specific method wasn't found
     * and thus this CompoundNode is not yet supported.
     */
    @Override
    public boolean visitEnter(CompoundParseNode expressionNode) throws SQLException {
        throw new SQLFeatureNotSupportedException(expressionNode.toString());
    }

    @Override
    public E visitLeave(CompoundParseNode expressionNode, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(expressionNode.toString());
    }

    /**
     * Fall through visit method. Anything coming through
     * here means that a more specific method wasn't found
     * and thus this Node is not yet supported.
     */
    @Override
    public E visit(ParseNode expressionNode) throws SQLException {
        throw new SQLFeatureNotSupportedException(expressionNode.toString());
    }
    
    @Override
    public List<E> newElementList(int size) {
        return null;
    }
    
    @Override
    public void addElement(List<E> l, E element) {
    }
}
