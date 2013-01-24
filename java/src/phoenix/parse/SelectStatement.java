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

import java.util.Collections;
import java.util.List;

/**
 * 
 * Top level node representing a SQL statement
 *
 * @author jtaylor
 * @since 0.1
 */
public class SelectStatement implements SQLStatement {
    private final List<TableNode> fromTable;
    private final HintNode hint;
    private final List<AliasedParseNode> select;
    private final ParseNode where;
    private final List<ParseNode> groupBy;
    private final ParseNode having;
    private final List<OrderByNode> orderBy;
    private final LimitNode limit;
    private final int bindCount;
    
    protected SelectStatement(List<TableNode> from, HintNode hint, List<AliasedParseNode> select, ParseNode where, List<ParseNode> groupBy, ParseNode having, List<OrderByNode> orderBy, LimitNode limit, int bindCount) {
        this.fromTable = Collections.unmodifiableList(from);
        this.hint = hint;
        this.select = Collections.unmodifiableList(select);
        this.where = where;
        this.groupBy = Collections.unmodifiableList(groupBy);
        this.having = having;
        this.orderBy = Collections.unmodifiableList(orderBy);
        this.limit = limit;
        this.bindCount = bindCount;
    }
    
    public LimitNode getLimit() {
        return limit;
    }
    
    @Override
    public int getBindCount() {
        return bindCount;
    }
    
    public List<TableNode> getFrom() {
        return fromTable;
    }
    
    public HintNode getHint() {
        return hint;
    }
    
    public List<AliasedParseNode> getSelect() {
        return select;
    }
    /**
     * Gets the where condition, or null if none.
     */
    public ParseNode getWhere() {
        return where;
    }
    
    /**
     * Gets the group-by, containing at least 1 element, or null, if none.
     */
    public List<ParseNode> getGroupBy() {
        return groupBy;
    }
    
    public ParseNode getHaving() {
        return having;
    }
    
    /**
     * Gets the order-by, containing at least 1 element, or null, if none.
     */
    public List<OrderByNode> getOrderBy() {
        return orderBy;
    }
}
