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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.*;

public class PColumnFamilyImpl implements PColumnFamily {
    private final PName name;
    private final List<PColumn> columns;
    private final Map<String, PColumn> columnByString;
    private final Map<byte[], PColumn> columnByBytes;
    
    public PColumnFamilyImpl(PName name, List<PColumn> columns) {
        this.name = name;
        this.columns = ImmutableList.copyOf(columns);
        ImmutableMap.Builder<String, PColumn> columnByStringBuilder = ImmutableMap.builder();
        ImmutableSortedMap.Builder<byte[], PColumn> columnByBytesBuilder = ImmutableSortedMap.orderedBy(Bytes.BYTES_COMPARATOR);
        for (PColumn column : columns) {
            columnByBytesBuilder.put(column.getName().getBytes(), column);
            columnByStringBuilder.put(column.getName().getString(), column);
        }
        this.columnByBytes = columnByBytesBuilder.build();
        this.columnByString = columnByStringBuilder.build();
    }
    
    @Override
    public PName getName() {
        return name;
    }

    @Override
    public List<PColumn> getColumns() {
        return columns;
    }

    @Override
    public PColumn getColumn(byte[] qualifier) throws ColumnNotFoundException  {
        PColumn column = columnByBytes.get(qualifier);
        if (column == null) {
            throw new ColumnNotFoundException(Bytes.toString(qualifier));
        }
        return column;
    }
    
    @Override
    public PColumn getColumn(String name) throws ColumnNotFoundException  {
        PColumn column = columnByString.get(name);
        if (column == null) {
            throw new ColumnNotFoundException(name);
        }
        return column;
    }
}
