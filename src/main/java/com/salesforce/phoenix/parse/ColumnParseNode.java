/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc. All rights reserved. Redistribution and use in source and binary forms, with
 * or without modification, are permitted provided that the following conditions are met: Redistributions of source code
 * must retain the above copyright notice, this list of conditions and the following disclaimer. Redistributions in
 * binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution. Neither the name of Salesforce.com nor the names
 * of its contributors may be used to endorse or promote products derived from this software without specific prior
 * written permission. THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.phoenix.parse;

import java.sql.SQLException;

import com.salesforce.phoenix.query.QueryConstants;

/**
 * Node representing a reference to a column in a SQL expression
 * 
 * @author jtaylor
 * @since 0.1
 */
public class ColumnParseNode extends NamedParseNode {
    private final TableName tableName;
    private final String fullName;
    private final String alias;

    public ColumnParseNode(TableName tableName, String name, String alias) {
        // Upper case here so our Maps can depend on this (and we don't have to upper case and create a string on every
        // lookup
        super(name);
        this.alias = alias;
        this.tableName = tableName;
        fullName = tableName == null ? getName() : tableName.toString() + QueryConstants.NAME_SEPARATOR + getName();
    }

    public ColumnParseNode(TableName tableName, String name) {
        this(tableName, name, null);
    }
    
    @Override
    public <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException {
        return visitor.visit(this);
    }

    public String getTableName() {
        return tableName == null ? null : tableName.getTableName();
    }

    public String getSchemaName() {
        return tableName == null ? null : tableName.getSchemaName();
    }

    public String getFullName() {
        return fullName;
    }

    @Override
    public String getAlias() {
        return alias;
    }

    @Override
    public String toString() {
        return fullName;
    }

    @Override
    public int hashCode() {
        return fullName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ColumnParseNode other = (ColumnParseNode)obj;
        return fullName.equals(other.fullName);
    }
}
