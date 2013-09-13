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

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;
import com.salesforce.hbase.index.ValueGetter;
import com.salesforce.hbase.index.covered.update.ColumnReference;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.index.IndexMaintainer;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.ColumnFamilyNotFoundException;
import com.salesforce.phoenix.schema.ColumnNotFoundException;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PColumnFamily;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PTable;

public class IndexUtil {
    public static final String INDEX_COLUMN_NAME_SEP = ":";
    public static final byte[] INDEX_COLUMN_NAME_SEP_BYTES = Bytes.toBytes(INDEX_COLUMN_NAME_SEP);

    private IndexUtil() {
    }

    // Since we cannot have nullable fixed length in a row key
    // we need to translate to variable length.
    public static PDataType getIndexColumnDataType(PColumn dataColumn) throws SQLException {
        PDataType type = getIndexColumnDataType(dataColumn.isNullable(),dataColumn.getDataType());
        if (type == null) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_INDEX_COLUMN_ON_TYPE).setColumnName(dataColumn.getName().getString())
            .setMessage("Type="+dataColumn.getDataType()).build().buildException();
        }
        return type;
    }
    
    // Since we cannot have nullable fixed length in a row key
    // we need to translate to variable length.
    public static PDataType getIndexColumnDataType(boolean isNullable, PDataType dataType) {
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
        return null;
    }
    
    public static String getIndexColumnName(String dataColumnFamilyName, String dataColumnName) {
        return (dataColumnFamilyName == null ? "" : dataColumnFamilyName) + INDEX_COLUMN_NAME_SEP + dataColumnName;
    }
    
    public static byte[] getIndexColumnName(byte[] dataColumnFamilyName, byte[] dataColumnName) {
        return ByteUtil.concat(dataColumnFamilyName == null ?  ByteUtil.EMPTY_BYTE_ARRAY : dataColumnFamilyName, INDEX_COLUMN_NAME_SEP_BYTES, dataColumnName);
    }
    
    public static String getIndexColumnName(PColumn dataColumn) {
        String dataColumnFamilyName = SchemaUtil.isPKColumn(dataColumn) ? null : dataColumn.getFamilyName().getString();
        return getIndexColumnName(dataColumnFamilyName, dataColumn.getName().getString());
    }

    public static PColumn getDataColumn(PTable dataTable, String indexColumnName) {
        int pos = indexColumnName.indexOf(INDEX_COLUMN_NAME_SEP);
        if (pos < 0) {
            throw new IllegalArgumentException("Could not find expected '" + INDEX_COLUMN_NAME_SEP +  "' separator in index column name of \"" + indexColumnName + "\"");
        }
        if (pos == 0) {
            try {
                return dataTable.getPKColumn(indexColumnName.substring(1));
            } catch (ColumnNotFoundException e) {
                throw new IllegalArgumentException("Could not find PK column \"" +  indexColumnName.substring(pos+1) + "\" in index column name of \"" + indexColumnName + "\"", e);
            }
        }
        PColumnFamily family;
        try {
            family = dataTable.getColumnFamily(indexColumnName.substring(0, pos));
        } catch (ColumnFamilyNotFoundException e) {
            throw new IllegalArgumentException("Could not find column family \"" +  indexColumnName.substring(0, pos) + "\" in index column name of \"" + indexColumnName + "\"", e);
        }
        try {
            return family.getColumn(indexColumnName.substring(pos+1));
        } catch (ColumnNotFoundException e) {
            throw new IllegalArgumentException("Could not find column \"" +  indexColumnName.substring(pos+1) + "\" in index column name of \"" + indexColumnName + "\"", e);
        }
    }

    @SuppressWarnings("deprecation")
    public static List<Mutation> generateIndexData(PTable table, PTable index, List<Mutation> dataMutations, ImmutableBytesWritable ptr) throws SQLException {
        IndexMaintainer maintainer = index.getIndexMaintainer(table);
        List<Mutation> indexMutations = Lists.newArrayListWithExpectedSize(dataMutations.size());
        for (final Mutation dataMutation : dataMutations) {
            // Ignore deletes
            if (dataMutation instanceof Put) {
                // TODO: is this more efficient than looking in our mutation map
                // using the key plus finding the PColumn?
                ValueGetter valueGetter = new ValueGetter() {
    
                    @Override
                    public byte[] getLatestValue(ColumnReference ref) {
                        Map<byte [], List<KeyValue>> familyMap = dataMutation.getFamilyMap();
                        byte[] family = ref.getFamily();
                        List<KeyValue> kvs = familyMap.get(family);
                        if (kvs == null) {
                            return null;
                        }
                        byte[] qualifier = ref.getQualifier();
                        for (KeyValue kv : kvs) {
                            if (Bytes.compareTo(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(), family, 0, family.length) == 0 &&
                                Bytes.compareTo(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(), qualifier, 0, qualifier.length) == 0) {
                                return kv.getValue();
                            }
                        }
                        return null;
                    }
                    
                };
                // TODO: we could only handle a delete if maintainer.getIndexColumns().isEmpty(),
                // since the Delete marker will have no key values
                assert(dataMutation instanceof Put);
                long ts = MetaDataUtil.getClientTimeStamp(dataMutation);
                ptr.set(dataMutation.getRow());
                byte[] indexRowKey = maintainer.buildRowKey(valueGetter, ptr);
                Put put = new Put(indexRowKey);
                for (ColumnReference ref : maintainer.getCoverededColumns()) {
                    try{
                        byte[] value = valueGetter.getLatestValue(ref);
                        if (value != null) {
                            KeyValue kv = KeyValueUtil.newKeyValue(put.getRow(), ref.getFamily(), ref.getQualifier(), ts, value);
                            try {
                                put.add(kv);
                            } catch (IOException e) {
                                throw new SQLException(e); // Impossible
                            }
                        }
                    }catch(IOException e){
                      throw new RuntimeException("Inmemory ValueGetter threw exception!",e);
                    }
                }
                put.add(maintainer.getEmptyKeyValueFamily(), QueryConstants.EMPTY_COLUMN_BYTES, ts, ByteUtil.EMPTY_BYTE_ARRAY);
                put.setWriteToWAL(false);
               
                indexMutations.add(put);
            }
        }
        return indexMutations;
    }
}
