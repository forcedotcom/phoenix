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
package com.salesforce.phoenix.schema;

import java.sql.SQLException;
import java.util.*;

import com.google.common.collect.*;
import com.salesforce.phoenix.query.QueryConstants;

public class PMetaDataImpl implements PMetaData {
    public static final PMetaData EMPTY_META_DATA = new PMetaDataImpl(Collections.<String,PSchema>emptyMap());
    private final Map<String,PSchema> metaData;
    
    public PMetaDataImpl(Map<String,PSchema> metaData) {
        this.metaData = ImmutableMap.copyOf(metaData);
    }
    
    public PMetaDataImpl(PSchema schema) {
        this.metaData = ImmutableMap.of(schema.getName(), schema);
    }
    
    @Override
    public PSchema getSchemaOrNull(String name) {
        return metaData.get(name);
    }

    @Override
    public PSchema getSchema(String name) throws SchemaNotFoundException {
        name = name == null ? QueryConstants.NULL_SCHEMA_NAME : name;
        PSchema schema = metaData.get(name);
        if (schema == null) {
            throw new SchemaNotFoundException(name);
        }
        return schema;
    }

    @Override
    public Map<String,PSchema> getSchemas() {
        return metaData;
    }


    @Override
    public PMetaData addTable(String schemaName, PTable table) throws SQLException {
        Map<String,PTable> tables;
        Map<String,PSchema> schemas = new HashMap<String,PSchema>(metaData);
        schemaName = schemaName == null ? QueryConstants.NULL_SCHEMA_NAME : schemaName;
        PSchema schema = schemas.get(schemaName);
        if (schema == null) {
            tables = Maps.newHashMapWithExpectedSize(5);
        } else {
            tables = Maps.newHashMap(schema.getTables());
        }
        PTable oldTable = tables.put(table.getName().getString(), table);
        if (table.getDataTableName() != null) { // Upsert new index table into parent data table list
            String parentTableName = table.getDataTableName().getString();
            PTable parentTable = tables.get(parentTableName);
            List<PTable> oldIndexes = parentTable.getIndexes();
            List<PTable> newIndexes = Lists.newArrayListWithExpectedSize(oldIndexes.size() + 1);
            newIndexes.addAll(oldIndexes);
            if (oldTable != null) {
                newIndexes.remove(oldTable);
            }
            newIndexes.add(table);
            tables.put(parentTableName, PTableImpl.makePTable(parentTable, table.getTimeStamp(), newIndexes));
        }
        for (PTable index : table.getIndexes()) {
            tables.put(index.getName().getString(), index);
        }
        schema = new PSchemaImpl(schemaName, tables);
        schemas.put(schemaName, schema);
        return new PMetaDataImpl(schemas);
    }

    @Override
    public PMetaData addColumn(String schemaName, String tableName, List<PColumn> newColumns, long tableTimeStamp, long tableSeqNum, boolean isImmutableRows) throws SQLException {
        Map<String,PSchema> schemas = new HashMap<String,PSchema>(metaData);
        PSchema schema = getSchema(schemaName);
        PTable table = schema.getTable(tableName);
        List<PColumn> columns = Lists.newArrayListWithExpectedSize(table.getColumns().size() + 1);
        columns.addAll(table.getColumns());
        columns.addAll(newColumns);
        Map<String,PTable> tables = Maps.newHashMap(schema.getTables());
        PTable newTable = PTableImpl.makePTable(table, tableTimeStamp, tableSeqNum, columns, isImmutableRows);
        tables.put(tableName, newTable);
        schema = new PSchemaImpl(schemaName, tables);
        schemas.put(schema.getName(), schema);
        return new PMetaDataImpl(schemas);
    }

    @Override
    public PMetaData removeTable(String schemaName, String tableName) throws SQLException {
        PSchema schema;
        try {
            schema = getSchema(schemaName);
        } catch (SchemaNotFoundException e) {
            throw new TableNotFoundException(schemaName, tableName);
        }
        Map<String,PTable> tables;
        Map<String,PSchema> schemas = new HashMap<String,PSchema>(metaData);
        tables = Maps.newHashMap(schema.getTables());
        PTable table;
        if ((table=tables.remove(tableName)) == null) {
            throw new TableNotFoundException(schemaName, tableName);
        } else {
            for (PTable index : table.getIndexes()) {
                if (tables.remove(index.getName().getString()) == null) {
                    throw new TableNotFoundException(schemaName, index.getName().getString());
                }
            }
        }
        schema = new PSchemaImpl(schema.getName(), tables);
        schemas.put(schema.getName(), schema);
        return new PMetaDataImpl(schemas);
    }
    
    @Override
    public PMetaData removeColumn(String schemaName, String tableName, String familyName, String columnName, long tableTimeStamp, long tableSeqNum) throws SQLException {
        PSchema schema = getSchema(schemaName);
        Map<String,PSchema> schemas = new HashMap<String,PSchema>(metaData);
        PTable table = schema.getTable(tableName);
        PColumn column;
        if (familyName == null) {
            column = table.getPKColumn(columnName);
        } else {
            column = table.getColumnFamily(familyName).getColumn(columnName);
        }
        int position = column.getPosition();
        List<PColumn> oldColumns = table.getColumns();
        List<PColumn> columns = Lists.newArrayListWithExpectedSize(table.getColumns().size() - 1);
        columns.addAll(oldColumns.subList(0, position));
        // Update position of columns that follow removed column
        for (int i = position+1; i < oldColumns.size(); i++) {
            PColumn oldColumn = oldColumns.get(i);
            PColumn newColumn = new PColumnImpl(oldColumn.getName(), oldColumn.getFamilyName(), oldColumn.getDataType(), oldColumn.getMaxLength(), oldColumn.getScale(), oldColumn.isNullable(), i-1, oldColumn.getColumnModifier());
            columns.add(newColumn);
        }
        
        Map<String,PTable> tables = Maps.newHashMap(schema.getTables());
        PTable newTable = PTableImpl.makePTable(table, tableTimeStamp, tableSeqNum, columns);
        tables.put(tableName, newTable);
        schema = new PSchemaImpl(schemaName, tables);
        schemas.put(schema.getName(), schema);
        return new PMetaDataImpl(schemas);
    }
}
