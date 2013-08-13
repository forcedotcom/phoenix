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
package com.salesforce.phoenix.util;

import java.sql.SQLException;
import java.util.*;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.*;

public class IndexUtil {
    private static final String INDEX_COLUMN_NAME_SEP = ":";
    private static final byte[] INDEX_COLUMN_NAME_SEP_BYTES = Bytes.toBytes(INDEX_COLUMN_NAME_SEP);

    private IndexUtil() {
    }

    // Since we cannot have nullable fixed length in a row key
    // we need to translate to variable length.
    public static PDataType getIndexColumnDataType(PColumn dataColumn) throws SQLException {
        return getIndexColumnDataType(dataColumn.isNullable(),dataColumn.getDataType(),dataColumn.getName().getString());
    }
    
    // Since we cannot have nullable fixed length in a row key
    // we need to translate to variable length.
    public static PDataType getIndexColumnDataType(boolean isNullable, PDataType dataType, String columnName) throws SQLException {
        if (!isNullable || !dataType.isFixedWidth()) {
            return dataType;
        }
        // for INT, BIGINT
        if (dataType.isCoercibleTo(PDataType.DECIMAL)) {
            return PDataType.DECIMAL;
        }
        // for CHAR
        if (dataType.isCoercibleTo(PDataType.VARCHAR)) {
            return PDataType.VARCHAR;
        }
        // Should not happen;
        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_INDEX_COLUMN_ON_TYPE).setColumnName(columnName)
            .setMessage("Type="+dataType).build().buildException();
    }
    
    public static String getIndexColumnName(String dataColumnFamilyName, String dataColumnName) {
        return dataColumnFamilyName == null ? dataColumnName : dataColumnFamilyName + INDEX_COLUMN_NAME_SEP + dataColumnName;
    }
    
    public static byte[] getIndexColumnName(byte[] dataColumnFamilyName, byte[] dataColumnName) {
        return dataColumnFamilyName == null ? dataColumnName : ByteUtil.concat(dataColumnFamilyName, INDEX_COLUMN_NAME_SEP_BYTES, dataColumnName);
    }
    
    public static String getIndexColumnName(PColumn dataColumn) {
        String dataColumnFamilyName = SchemaUtil.isPKColumn(dataColumn) ? null : dataColumn.getFamilyName().getString();
        return getIndexColumnName(dataColumnFamilyName, dataColumn.getName().getString());
    }

    private static void coerceDataValueToIndexValue(PColumn dataColumn, PColumn indexColumn, ImmutableBytesWritable ptr) {
        PDataType dataType = dataColumn.getDataType();
        // TODO: push to RowKeySchema? 
        ColumnModifier dataModifier = dataColumn.getColumnModifier();
        PDataType indexType = indexColumn.getDataType();
        ColumnModifier indexModifier = indexColumn.getColumnModifier();
        // We know ordinal position will match pk position, because you cannot
        // alter an index table.
        indexType.coerceBytes(ptr, dataType, dataModifier, indexModifier);
    }
    
    public static List<Mutation> generateIndexData(PTable indexTable, PTable dataTable, Mutation dataMutation, ImmutableBytesWritable ptr) throws SQLException {
        byte[] dataRowKey = dataMutation.getRow();
        int maxOffset = dataRowKey.length;
        RowKeySchema dataRowKeySchema = dataTable.getRowKeySchema();
        List<PColumn> dataPKColumns = dataTable.getPKColumns();
        int i = 0;
        int indexOffset = 0;
        ptr.set(dataRowKey);
        Boolean hasValue = dataRowKeySchema.first(ptr, i, ValueBitSet.EMPTY_VALUE_BITSET);
        if (dataTable.getBucketNum() != null) { // Skip salt column
            hasValue=dataRowKeySchema.next(ptr, ++i, maxOffset, ValueBitSet.EMPTY_VALUE_BITSET);
        }
        List<PColumn> indexPKColumns = indexTable.getPKColumns();
        List<PColumn> indexColumns = indexTable.getColumns();
        int nIndexColumns = indexPKColumns.size();
        int maxIndexValues = indexColumns.size() - nIndexColumns - indexOffset;
        BitSet indexValuesSet = new BitSet(maxIndexValues);
        byte[][] indexValues = new byte[indexColumns.size() - indexOffset][];
        while (hasValue != null) {
            if (hasValue) {
                PColumn dataColumn = dataPKColumns.get(i);
                PColumn indexColumn = indexTable.getColumn(dataColumn.getName().getString());
                coerceDataValueToIndexValue(dataColumn, indexColumn, ptr);
                indexValues[indexColumn.getPosition()-indexOffset] = ptr.copyBytes();
            }
            hasValue=dataRowKeySchema.next(ptr, ++i, maxOffset, ValueBitSet.EMPTY_VALUE_BITSET);
        }
        PRow row;
        long ts = MetaDataUtil.getClientTimeStamp(dataMutation);
        if (dataMutation instanceof Delete && dataMutation.getFamilyMap().values().isEmpty()) {
            indexTable.newKey(ptr, indexValues);
            row = indexTable.newRow(ts, ptr);
            row.delete();
        } else {
            for (Map.Entry<byte[],List<KeyValue>> entry : dataMutation.getFamilyMap().entrySet()) {
                PColumnFamily family = dataTable.getColumnFamily(entry.getKey());
                for (KeyValue kv : entry.getValue()) {
                    byte[] cq = kv.getQualifier();
                    if (Bytes.compareTo(QueryConstants.EMPTY_COLUMN_BYTES, cq) != 0) {
                        try {
                            PColumn dataColumn = family.getColumn(cq);
                            PColumn indexColumn = indexTable.getColumn(getIndexColumnName(family.getName().getString(), dataColumn.getName().getString()));
                            ptr.set(kv.getBuffer(),kv.getValueOffset(),kv.getValueLength());
                            coerceDataValueToIndexValue(dataColumn, indexColumn, ptr);
                            indexValues[indexColumn.getPosition()-indexOffset] = ptr.copyBytes();
                            if (!SchemaUtil.isPKColumn(indexColumn)) {
                                indexValuesSet.set(indexColumn.getPosition()-nIndexColumns-indexOffset);
                            }
                        } catch (ColumnNotFoundException e) {
                            // Ignore as this means that the data column isn't in the index
                        }
                    }
                }
            }
            indexTable.newKey(ptr, indexValues);
            row = indexTable.newRow(ts, ptr);
            int pos = 0;
            while ((pos = indexValuesSet.nextSetBit(pos)) >= 0) {
                int index = nIndexColumns + indexOffset + pos++;
                PColumn indexColumn = indexColumns.get(index);
                row.setValue(indexColumn, indexValues[index]);
            }
        }
        return row.toRowMutations();
    }

    public static List<Mutation> generateIndexData(PTable table, PTable index, List<Mutation> dataMutations) throws SQLException {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        List<Mutation> indexMutations = Lists.newArrayListWithExpectedSize(dataMutations.size());
        for (Mutation dataMutation : dataMutations) {
            indexMutations.addAll(generateIndexData(index, table, dataMutation, ptr));
        }
        return indexMutations;
    }

}
