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

import java.util.*;

import phoenix.jdbc.PhoenixDatabaseMetaData;
import phoenix.schema.PTableType;

import com.google.common.collect.ImmutableList;

public class CreateTableStatement implements SQLStatement {
    private final TableName tableName;
    private final PTableType tableType;
    private final List<ColumnDef> pkColumns;
    private final List<ColumnFamilyDef> columnFamilies;
    private final List<ParseNode> splitNodes;
    private final int bindCount;
    private final Map<String,Object> props;
    private final boolean isView;
    private final boolean ifNotExists;
    
    protected CreateTableStatement(TableName tableName, Map<String,Object> props, List<ColumnDef> pkColumns, List<ColumnFamilyDef> columnFamilies, List<ParseNode> splitNodes, boolean isView, boolean ifNotExists, int bindCount) {
        this.tableName = tableName;
        this.props = props == null ? Collections.<String,Object>emptyMap() : props;
        this.tableType = PhoenixDatabaseMetaData.TYPE_SCHEMA.equals(tableName.getSchemaName()) ? PTableType.SYSTEM : isView ? PTableType.VIEW : PTableType.USER;
        this.pkColumns = ImmutableList.copyOf(pkColumns);
        this.columnFamilies = ImmutableList.copyOf(columnFamilies);
        this.splitNodes = splitNodes == null ? Collections.<ParseNode>emptyList() : ImmutableList.copyOf(splitNodes);
        this.bindCount = bindCount;
        this.isView = isView;
        this.ifNotExists = ifNotExists;
    }
    
    @Override
    public int getBindCount() {
        return bindCount;
    }

    public TableName getTableName() {
        return tableName;
    }

    public List<ColumnDef> getPkColumns() {
        return pkColumns;
    }

    public List<ColumnFamilyDef> getColumnFamilies() {
        return columnFamilies;
    }

    public List<ParseNode> getSplitNodes() {
        return splitNodes;
    }

    public PTableType getTableType() {
        return tableType;
    }

    public Map<String,Object> getProps() {
        return props;
    }

    public boolean isView() {
        return isView;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

}
