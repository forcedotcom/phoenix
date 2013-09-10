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
import java.util.List;


/**
 * 
 * Visitor that traverses into no parse nodes
 *
 * @author jtaylor
 * @since 0.1
 */
public abstract class TraverseNoParseNodeVisitor<T> extends BaseParseNodeVisitor<T> {
    @Override
    public boolean visitEnter(AndParseNode node) throws SQLException {
        return false;
    }

    @Override
    public boolean visitEnter(OrParseNode node) throws SQLException {
        return false;
    }

    @Override
    public boolean visitEnter(FunctionParseNode node) throws SQLException {
        return false;
    }

    @Override
    public boolean visitEnter(ComparisonParseNode node) throws SQLException {
        return false;
    }
    
    @Override
    public boolean visitEnter(CaseParseNode node) throws SQLException {
        return false;
    }

    @Override
    public boolean visitEnter(LikeParseNode node) throws SQLException {
        return false;
    }
    
    @Override
    public boolean visitEnter(BetweenParseNode node) throws SQLException {
        return false;
    }
    
    @Override
    public T visitLeave(LikeParseNode node, List<T> l) throws SQLException {
        return null;
    }
    
    @Override
    public boolean visitEnter(NotParseNode node) throws SQLException {
        return false;
    }
    
    @Override
    public T visitLeave(NotParseNode node, List<T> l) throws SQLException {
        return null;
    }
    
    @Override
    public boolean visitEnter(InListParseNode node) throws SQLException {
        return false;
    }
    
    @Override
    public T visitLeave(InListParseNode node, List<T> l) throws SQLException {
        return null;
    }
    
    @Override
    public boolean visitEnter(IsNullParseNode node) throws SQLException {
        return false;
    }
    
    @Override
    public T visitLeave(IsNullParseNode node, List<T> l) throws SQLException {
        return null;
    }
    
    @Override
    public T visit(ColumnParseNode node) throws SQLException {
        return null;
    }

    @Override
    public T visit(LiteralParseNode node) throws SQLException {
        return null;
    }

    @Override
    public T visit(BindParseNode node) throws SQLException {
        return null;
    }

    @Override
    public T visit(WildcardParseNode node) throws SQLException {
        return null;
    }
    
    @Override
    public T visit(FamilyParseNode node) throws SQLException {
        return null;
    }

    @Override
    public T visitLeave(AndParseNode node, List<T> l) throws SQLException {
        return null;
    }

    @Override
    public T visitLeave(OrParseNode node, List<T> l) throws SQLException {
        return null;
    }

    @Override
    public T visitLeave(FunctionParseNode node, List<T> l) throws SQLException {
        return null;
    }

    @Override
    public T visitLeave(ComparisonParseNode node, List<T> l) throws SQLException {
        return null;
    }
    
    @Override
    public T visitLeave(CaseParseNode node, List<T> l) throws SQLException {
        return null;
    }

    @Override
    public boolean visitEnter(MultiplyParseNode node) throws SQLException {
        return false;
    }

    @Override
    public T visitLeave(MultiplyParseNode node, List<T> l) throws SQLException {
        return null;
    }
    
    @Override
    public boolean visitEnter(SubtractParseNode node) throws SQLException {
        return false;
    }

    @Override
    public T visitLeave(SubtractParseNode node, List<T> l) throws SQLException {
        return null;
    }
    
    @Override
    public boolean visitEnter(AddParseNode node) throws SQLException {
        return false;
    }

    @Override
    public T visitLeave(AddParseNode node, List<T> l) throws SQLException {
        return null;
    }
    
    @Override
    public boolean visitEnter(DivideParseNode node) throws SQLException {
        return false;
    }

    @Override
    public T visitLeave(DivideParseNode node, List<T> l) throws SQLException {
        return null;
    }
    @Override
    public boolean visitEnter(StringConcatParseNode node) throws SQLException {
        return false;
    }

    @Override
    public T visitLeave(StringConcatParseNode node, List<T> l) throws SQLException {
        return null;
    }
    
    @Override
    public T visitLeave(BetweenParseNode node, List<T> l) throws SQLException {
        return null;
    }
}
