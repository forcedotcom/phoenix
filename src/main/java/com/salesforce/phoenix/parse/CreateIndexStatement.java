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
import com.salesforce.phoenix.schema.PTableType;


public class CreateIndexStatement extends CreateTableStatement implements SQLStatement {

    private final String dataTableName;
    private final TableName tableName;
    private final List<ParseNode> includeColumns;

    public CreateIndexStatement(NamedNode indexName, TableName tableName, PrimaryKeyConstraint pkConstraint, List<ParseNode> includeColumns,
            ListMultimap<String,Pair<String,Object>> props, int bindCount) {
        super(tableName, props, Collections.<ColumnDef>emptyList(), pkConstraint, null, false, false, bindCount);
        this.includeColumns = includeColumns;
        this.dataTableName = tableName.getTableName();
        this.tableName = new TableName(tableName.getSchemaName(), indexName.getName());
    }

    public List<ParseNode> getIncludeColumns() {
        return includeColumns;
    }

    @Override
    public TableName getTableName() {
        return tableName;
    }

    public String getDataTableName() {
        return dataTableName;
    }

    @Override
    public PTableType getTableType() {
        return PTableType.INDEX;
    }
}
