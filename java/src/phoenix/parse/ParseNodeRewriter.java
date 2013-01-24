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
package phoenix.parse;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * Base class for visitors that rewrite the expression node hierarchy
 *
 * @author jtaylor
 * @since 0.1
 */
public class ParseNodeRewriter extends TraverseAllParseNodeVisitor<ParseNode> {

    @Override
    public ParseNode visitLeave(AndParseNode node, List<ParseNode> l) throws SQLException {
        return node;
    }
    @Override
    public ParseNode visitLeave(SubtractParseNode node, List<ParseNode> l) throws SQLException {
        return node;
    }
    @Override
    public ParseNode visitLeave(OrParseNode node, List<ParseNode> l) throws SQLException {
        return node;
    }

    @Override
    public ParseNode visitLeave(FunctionParseNode node, List<ParseNode> l) throws SQLException {
        return node;
    }

    @Override
    public ParseNode visitLeave(ComparisonParseNode node, List<ParseNode> l) throws SQLException {
        return node;
    }

    @Override
    public ParseNode visitLeave(CaseParseNode node, List<ParseNode> l) throws SQLException {
        return node;
    }

    @Override
    public ParseNode visitLeave(LikeParseNode node, List<ParseNode> l) throws SQLException {
        return node;
    }
    
    @Override
    public ParseNode visitLeave(NotParseNode node, List<ParseNode> l) throws SQLException {
        return node;
    }
    
    @Override
    public ParseNode visitLeave(InListParseNode node, List<ParseNode> l) throws SQLException {
        return node;
    }
    
    @Override
    public ParseNode visitLeave(IsNullParseNode node, List<ParseNode> l) throws SQLException {
        return node;
    }
    
    @Override
    public ParseNode visit(ColumnParseNode node) throws SQLException {
        return node;
    }

    @Override
    public ParseNode visit(LiteralParseNode node) throws SQLException {
        return node;
    }

    @Override
    public ParseNode visit(BindParseNode node) throws SQLException {
        return node;
    }
    
    @Override
    public ParseNode visit(WildcardParseNode node) throws SQLException {
        return node;
    }
    
    @Override
    public List<ParseNode> newElementList(int size) {
        return new ArrayList<ParseNode>(size);
    }
    
    @Override
    public void addElement(List<ParseNode> l, ParseNode element) {
        if (element != null) {
            l.add(element);
        }
    }
    @Override
    public ParseNode visitLeave(AddParseNode node, List<ParseNode> l) throws SQLException {
        return node;
    }
    @Override
    public ParseNode visitLeave(MultiplyParseNode node, List<ParseNode> l) throws SQLException {
        return node;
    }
    @Override
    public ParseNode visitLeave(DivideParseNode node, List<ParseNode> l) throws SQLException {
        return node;
    }

    @Override
    public ParseNode visitLeave(StringConcatParseNode node, List<ParseNode> l) throws SQLException {
        return node;
    }

}
