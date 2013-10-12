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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class PMetaDataImpl implements PMetaData {
    public static final PMetaData EMPTY_META_DATA = new PMetaDataImpl(Collections.<String,PTable>emptyMap());
    private final Map<String,PTable> metaData;
    
    public PMetaDataImpl(Map<String,PTable> metaData) {
        this.metaData = ImmutableMap.copyOf(metaData);
    }
    
    @Override
    public PTable getTable(String name) throws TableNotFoundException {
        PTable table = metaData.get(name);
        if (table == null) {
            throw new TableNotFoundException(name);
        }
        return table;
    }

    @Override
    public Map<String,PTable> getTables() {
        return metaData;
    }


    @Override
    public PMetaData addTable(PTable table) throws SQLException {
        Map<String,PTable> tables = Maps.newHashMap(metaData);
        PTable oldTable = tables.put(table.getName().getString(), table);
        if (table.getParentName() != null) { // Upsert new index table into parent data table list
            String parentName = table.getParentName().getString();
            PTable parentTable = tables.get(parentName);
            List<PTable> oldIndexes = parentTable.getIndexes();
            List<PTable> newIndexes = Lists.newArrayListWithExpectedSize(oldIndexes.size() + 1);
            newIndexes.addAll(oldIndexes);
            if (oldTable != null) {
                newIndexes.remove(oldTable);
            }
            newIndexes.add(table);
            tables.put(parentName, PTableImpl.makePTable(parentTable, table.getTimeStamp(), newIndexes));
        }
        for (PTable index : table.getIndexes()) {
            tables.put(index.getName().getString(), index);
        }
        return new PMetaDataImpl(tables);
    }

    @Override
    public PMetaData addColumn(String tableName, List<PColumn> columnsToAdd, long tableTimeStamp, long tableSeqNum, boolean isImmutableRows) throws SQLException {
        PTable table = getTable(tableName);
        Map<String,PTable> tables = Maps.newHashMap(metaData);
        List<PColumn> oldColumns = PTableImpl.getColumnsToClone(table);
        List<PColumn> newColumns;
        if (columnsToAdd.isEmpty()) {
            newColumns = oldColumns;
        } else {
            newColumns = Lists.newArrayListWithExpectedSize(oldColumns.size() + columnsToAdd.size());
            newColumns.addAll(oldColumns);
            newColumns.addAll(columnsToAdd);
        }
        PTable newTable = PTableImpl.makePTable(table, tableTimeStamp, tableSeqNum, newColumns, isImmutableRows);
        tables.put(tableName, newTable);
        return new PMetaDataImpl(tables);
    }

    @Override
    public PMetaData removeTable(String tableName) throws SQLException {
        PTable table;
        Map<String,PTable> tables = Maps.newHashMap(metaData);
        if ((table=tables.remove(tableName)) == null) {
            throw new TableNotFoundException(tableName);
        } else {
            for (PTable index : table.getIndexes()) {
                if (tables.remove(index.getName().getString()) == null) {
                    throw new TableNotFoundException(index.getName().getString());
                }
            }
        }
        return new PMetaDataImpl(tables);
    }
    
    @Override
    public PMetaData removeColumn(String tableName, String familyName, String columnName, long tableTimeStamp, long tableSeqNum) throws SQLException {
        PTable table = getTable(tableName);
        Map<String,PTable> tables = Maps.newHashMap(metaData);
        PColumn column;
        if (familyName == null) {
            column = table.getPKColumn(columnName);
        } else {
            column = table.getColumnFamily(familyName).getColumn(columnName);
        }
        int positionOffset = 0;
        int position = column.getPosition();
        List<PColumn> oldColumns = table.getColumns();
        if (table.getBucketNum() != null) {
            position--;
            positionOffset = 1;
            oldColumns = oldColumns.subList(positionOffset, oldColumns.size());
        }
        List<PColumn> columns = Lists.newArrayListWithExpectedSize(oldColumns.size() - 1);
        columns.addAll(oldColumns.subList(0, position));
        // Update position of columns that follow removed column
        for (int i = position+1; i < oldColumns.size(); i++) {
            PColumn oldColumn = oldColumns.get(i);
            PColumn newColumn = new PColumnImpl(oldColumn.getName(), oldColumn.getFamilyName(), oldColumn.getDataType(), oldColumn.getMaxLength(), oldColumn.getScale(), oldColumn.isNullable(), i-1+positionOffset, oldColumn.getColumnModifier());
            columns.add(newColumn);
        }
        
        PTable newTable = PTableImpl.makePTable(table, tableTimeStamp, tableSeqNum, columns);
        tables.put(tableName, newTable);
        return new PMetaDataImpl(tables);
    }
}
