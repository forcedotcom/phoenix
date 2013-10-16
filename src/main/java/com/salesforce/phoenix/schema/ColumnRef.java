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

import org.apache.http.annotation.Immutable;

import com.salesforce.phoenix.expression.ColumnExpression;
import com.salesforce.phoenix.expression.IndexKeyValueColumnExpression;
import com.salesforce.phoenix.expression.KeyValueColumnExpression;
import com.salesforce.phoenix.expression.ProjectedColumnExpression;
import com.salesforce.phoenix.expression.RowKeyColumnExpression;
import com.salesforce.phoenix.util.SchemaUtil;


/**
 * 
 * Class that represents a reference to a PColumn in a PTable
 *
 * @author jtaylor
 * @since 0.1
 */
@Immutable
public final class ColumnRef {
    private final TableRef tableRef;
    private final int columnPosition;
    private final int pkSlotPosition;
    
    public ColumnRef(ColumnRef columnRef, long timeStamp) {
        this.tableRef = new TableRef(columnRef.tableRef, timeStamp);
        this.columnPosition = columnRef.columnPosition;
        this.pkSlotPosition = columnRef.pkSlotPosition;
    }

    public ColumnRef(TableRef tableRef, int columnPosition) {
        if (tableRef == null) {
            throw new NullPointerException();
        }
        if (columnPosition < 0 || columnPosition >= tableRef.getTable().getColumns().size()) {
            throw new IllegalArgumentException("Column position of " + columnPosition + " must be between 0 and " + tableRef.getTable().getColumns().size() + " for table " + tableRef.getTable().getName().getString());
        }
        this.tableRef = tableRef;
        this.columnPosition = columnPosition;
        PColumn column = getColumn();
        int i = -1;
        if (SchemaUtil.isPKColumn(column)) {
            for (PColumn pkColumn : tableRef.getTable().getPKColumns()) {
                i++;
                if (pkColumn == column) {
                    break;
                }
            }
        }
        pkSlotPosition = i;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + columnPosition;
        result = prime * result + tableRef.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ColumnRef other = (ColumnRef)obj;
        if (columnPosition != other.columnPosition) return false;
        if (!tableRef.equals(other.tableRef)) return false;
        return true;
    }

    public ColumnExpression newColumnExpression() throws SQLException {
        if (SchemaUtil.isPKColumn(this.getColumn())) {            
            return new RowKeyColumnExpression(getColumn(), new RowKeyValueAccessor(this.getTable().getPKColumns(), pkSlotPosition));
        }
        
        if (tableRef.getTable().getType() == PTableType.INDEX) {
            return new IndexKeyValueColumnExpression(getColumn());
        }
        
        if (tableRef.getTable().getType() == PTableType.JOIN) {
        	return new ProjectedColumnExpression(getColumn(), tableRef.getTable());
        }
        
        return new KeyValueColumnExpression(getColumn());
    }

    public int getColumnPosition() {
        return columnPosition;
    }
    
    public int getPKSlotPosition() {
        return pkSlotPosition;
    }
    
    public PColumn getColumn() {
        return tableRef.getTable().getColumns().get(columnPosition);
    }

    public PTable getTable() {
        return tableRef.getTable();
    }
    
    public TableRef getTableRef() {
        return tableRef;
    }
}
