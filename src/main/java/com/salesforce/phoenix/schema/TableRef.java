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

import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.util.SchemaUtil;


public final class TableRef {
    private final PTable table;
    private final String alias;
    private final PSchema schema;
    private final byte[] tableName;
    private final long timeStamp;
    private final boolean hasDynamicCols;

    public TableRef(String alias, PTable table, PSchema schema, long timeStamp, boolean hasDynamicCols) {
        this.alias = alias;
        this.table = table;
        this.schema = schema;
        this.tableName = SchemaUtil.getTableName(Bytes.toBytes(schema.getName()), table.getName().getBytes());
        this.timeStamp = timeStamp;
        this.hasDynamicCols = hasDynamicCols;
    }

    public PTable getTable() {
        return table;
    }

    public String getTableAlias() {
        return alias;
    }

    public PSchema getSchema() {
        return schema;
    }

    public byte[] getTableName() {
        return tableName;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.schema.getName().hashCode();
        result = prime * result + this.table.getName().getString().hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        TableRef other = (TableRef)obj;
        if (!schema.getName().equals(other.schema.getName())) return false;
        if (!table.getName().getString().equals(other.table.getName().getString())) return false;
        return true;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public boolean hasDynamicCols() {
        return hasDynamicCols;
    }

}
