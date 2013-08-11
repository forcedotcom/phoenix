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
 * Visitor for ParseNode in the node tree. Uses composite
 * visitor pattern with enter/leave calls for any
 * compound expression node. Only supported SQL constructs
 * have visit methods.  Unsupported constructs fall through
 * to {@link #visitEnter(CompoundParseNode)} for
 * compound parse nodes and {@link #visit(ParseNode)}
 * for terminal parse nodes.
 * 
 * @author jtaylor
 * @since 0.1
 */
public interface ParseNodeVisitor<E> {
    public List<E> newElementList(int size);
    public void addElement(List<E> a, E element);
    
    public boolean visitEnter(LikeParseNode node) throws SQLException;
    public E visitLeave(LikeParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(AndParseNode node) throws SQLException;
    public E visitLeave(AndParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(OrParseNode node) throws SQLException;
    public E visitLeave(OrParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(FunctionParseNode node) throws SQLException;
    public E visitLeave(FunctionParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(ComparisonParseNode node) throws SQLException;
    public E visitLeave(ComparisonParseNode node, List<E> l) throws SQLException;

    public boolean visitEnter(CaseParseNode node) throws SQLException;
    public E visitLeave(CaseParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(CompoundParseNode node) throws SQLException;
    public E visitLeave(CompoundParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(AddParseNode node) throws SQLException;
    public E visitLeave(AddParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(MultiplyParseNode node) throws SQLException;
    public E visitLeave(MultiplyParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(DivideParseNode node) throws SQLException;
    public E visitLeave(DivideParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(SubtractParseNode node) throws SQLException;
    public E visitLeave(SubtractParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(NotParseNode node) throws SQLException;
    public E visitLeave(NotParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(InListParseNode node) throws SQLException;
    public E visitLeave(InListParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(IsNullParseNode node) throws SQLException;
    public E visitLeave(IsNullParseNode node, List<E> l) throws SQLException;
    
    public E visit(ColumnParseNode node) throws SQLException;
    public E visit(LiteralParseNode node) throws SQLException;
    public E visit(BindParseNode node) throws SQLException;
    public E visit(WildcardParseNode node) throws SQLException;  
    public E visit(FamilyParseNode node) throws SQLException;  
    public E visit(ParseNode node) throws SQLException;  
    
    public boolean visitEnter(StringConcatParseNode node) throws SQLException;
    public E visitLeave(StringConcatParseNode node, List<E> l) throws SQLException;
	
    public boolean visitEnter(BetweenParseNode node) throws SQLException;
    public E visitLeave(BetweenParseNode node, List<E> l) throws SQLException;
}
