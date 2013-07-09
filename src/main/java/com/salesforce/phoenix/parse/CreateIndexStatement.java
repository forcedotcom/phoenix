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

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.ListMultimap;


public class CreateIndexStatement extends SingleTableSQLStatement {
    private final TableName indexTableName;
    private final PrimaryKeyConstraint indexConstraint;
    private final List<ColumnName> includeColumns;
    private final List<ParseNode> splitNodes;
    private final ListMultimap<String,Pair<String,Object>> props;
    private final boolean ifNotExists;

    public CreateIndexStatement(NamedNode indexTableName, NamedTableNode dataTable, 
            PrimaryKeyConstraint indexConstraint, List<ColumnName> includeColumns, List<ParseNode> splits,
            ListMultimap<String,Pair<String,Object>> props, boolean ifNotExists, int bindCount) {
        super(dataTable, bindCount);
        this.indexTableName = new TableName(dataTable.getName().getSchemaName(),indexTableName.getName());
        this.indexConstraint = indexConstraint == null ? PrimaryKeyConstraint.EMPTY : indexConstraint;
        this.includeColumns = includeColumns == null ? Collections.<ColumnName>emptyList() : includeColumns;
        this.splitNodes = splits == null ? Collections.<ParseNode>emptyList() : splits;
        this.props = props;
        this.ifNotExists = ifNotExists;
    }

    public PrimaryKeyConstraint getIndexConstraint() {
        return indexConstraint;
    }

    public List<ColumnName> getIncludeColumns() {
        return includeColumns;
    }

    public TableName getIndexTableName() {
        return indexTableName;
    }

    public List<ParseNode> getSplitNodes() {
        return splitNodes;
    }

    public ListMultimap<String,Pair<String,Object>> getProps() {
        return props;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

}
