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

import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_TABLE_NAME_BYTES;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.Lists;
import com.salesforce.phoenix.coprocessor.MetaDataProtocol;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData;
import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.AmbiguousColumnException;
import com.salesforce.phoenix.schema.ColumnFamilyNotFoundException;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.ColumnNotFoundException;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PColumnFamily;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PDatum;
import com.salesforce.phoenix.schema.PMetaData;
import com.salesforce.phoenix.schema.PName;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.RowKeySchema;
import com.salesforce.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import com.salesforce.phoenix.schema.SaltingUtil;



/**
 * 
 * Static class for various schema-related utilities
 *
 * @author jtaylor
 * @since 0.1
 */
public class SchemaUtil {
    private static final int VAR_LENGTH_ESTIMATE = 10;
    
    public static final DataBlockEncoding DEFAULT_DATA_BLOCK_ENCODING = DataBlockEncoding.FAST_DIFF;
    public static final RowKeySchema VAR_BINARY_SCHEMA = new RowKeySchemaBuilder(1).addField(new PDatum() {
    
        @Override
        public boolean isNullable() {
            return false;
        }
    
        @Override
        public PDataType getDataType() {
            return PDataType.VARBINARY;
        }
    
        @Override
        public Integer getByteSize() {
            return null;
        }
    
        @Override
        public Integer getMaxLength() {
            return null;
        }
    
        @Override
        public Integer getScale() {
            return null;
        }
    
        @Override
        public ColumnModifier getColumnModifier() {
            return null;
        }
        
    }, false, null).build();
    
    /**
     * May not be instantiated
     */
    private SchemaUtil() {
    }

    /**
     * Join srcRow KeyValues to dstRow KeyValues.  For single-column mode, in the case of multiple
     * column families (which can happen for our conceptual PK to PK joins), we always use the
     * column family from the first dstRow.  TODO: we'll likely need a different coprocessor to
     * handle the PK join case.
     * @param srcRow the source list of KeyValues to join with dstRow KeyValues.  The key will be
     * changed upon joining to match the dstRow key.  The list may not be empty and is left unchanged.
     * @param dstRow the list of KeyValues to which the srcRows are joined.  The list is modified in
     * place and may not be empty.
     */
    public static void joinColumns(List<KeyValue> srcRow, List<KeyValue> dstRow) {
        assert(!dstRow.isEmpty());
        KeyValue dst = dstRow.get(0);
        byte[] dstBuf = dst.getBuffer();
        int dstKeyOffset = dst.getRowOffset();
        int dstKeyLength = dst.getRowLength();
        // Combine columns from both rows
        // The key for the cached KeyValues are modified to match the other key.
        for (KeyValue srcValue : srcRow) {
            byte[] srcBuf = srcValue.getBuffer();
            byte type = srcValue.getType();
            KeyValue dstValue = new KeyValue(dstBuf, dstKeyOffset, dstKeyLength,
                srcBuf, srcValue.getFamilyOffset(), srcValue.getFamilyLength(),
                srcBuf, srcValue.getQualifierOffset(), srcValue.getQualifierLength(),
                HConstants.LATEST_TIMESTAMP, KeyValue.Type.codeToType(type),
                srcBuf, srcValue.getValueOffset(), srcValue.getValueLength());
            dstRow.add(dstValue);
        }
        // Put KeyValues in proper sort order
        // TODO: our tests need this, but otherwise would this be required?
        Collections.sort(dstRow, KeyValue.COMPARATOR);
   }
    
    /**
     * Get the column value of a row.
     * @param result the Result return from iterating through scanner results.
     * @param fam the column family 
     * @param col the column qualifier
     * @param pos the column position
     * @param value updated in place to the bytes representing the column value
     * @return true if the column exists and value was set and false otherwise.
     */
    public static boolean getValue(Result result, byte[] fam, byte[] col, int pos, ImmutableBytesWritable value) {
        KeyValue keyValue = ResultUtil.getColumnLatest(result, fam, col);
        if (keyValue != null) {
            value.set(keyValue.getBuffer(),keyValue.getValueOffset(),keyValue.getValueLength());
            return true;
        }
        return false;
    }

    /**
     * Concatenate two PColumn arrays
     * @param first first array
     * @param second second array
     * @return new combined array
     */
    public static PColumn[] concat(PColumn[] first, PColumn[] second) {
        PColumn[] result = new PColumn[first.length + second.length];
        System.arraycopy(first, 0, result, 0, first.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }
    
    public static boolean isPKColumn(PColumn column) {
        return column.getFamilyName() == null;
    }
    
    /**
     * Estimate the max key length in bytes of the PK for a given table
     * @param table the table
     * @return the max PK length
     */
    public static int estimateKeyLength(PTable table) {
        int maxKeyLength = 0;
        // Calculate the max length of a key (each part must currently be of a fixed width)
        int i = 0;
        List<PColumn> columns = table.getPKColumns();
        while (i < columns.size()) {
            PColumn keyColumn = columns.get(i++);
            Integer byteSize = keyColumn.getByteSize();
            maxKeyLength += (byteSize == null) ? VAR_LENGTH_ESTIMATE : byteSize;
        }
        return maxKeyLength;
    }

    /**
     * Normalize an identifier. If name is surrounded by double quotes,
     * it is used as-is, otherwise the name is upper caased.
     * @param name the parsed identifier
     * @return the normalized identifier
     */
    public static String normalizeIdentifier(String name) {
        if (name == null) {
            return name;
        }
        if (isCaseSensitive(name)) {
            // Don't upper case if in quotes
            return name.substring(1, name.length()-1);
        }
        return name.toUpperCase();
    }

    public static boolean isCaseSensitive(String name) {
        return name.length() > 0 && name.charAt(0)=='"';
    }
    
    public static <T> List<T> concat(List<T> l1, List<T> l2) {
        int size1 = l1.size();
        if (size1 == 0) {
            return l2;
        }
        int size2 = l2.size();
        if (size2 == 0) {
            return l1;
        }
        List<T> l3 = new ArrayList<T>(size1 + size2);
        l3.addAll(l1);
        l3.addAll(l2);
        return l3;
    }

    public static byte[] getSequenceKey(byte[] schemaName, byte[] sequenceName) {
        return ByteUtil.concat(schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : schemaName, QueryConstants.SEPARATOR_BYTE_ARRAY, sequenceName);
    }

    public static byte[] getSequenceKey(String schemaName, String sequenceName) {
        return getSequenceKey(Bytes.toBytes(schemaName), Bytes.toBytes(sequenceName));
    }

    /**
     * Get the key used in the Phoenix metadata row for a table definition
     * @param schemaName
     * @param tableName
     */
    public static byte[] getTableKey(byte[] tenantId, byte[] schemaName, byte[] tableName) {
        return ByteUtil.concat(tenantId, QueryConstants.SEPARATOR_BYTE_ARRAY, schemaName, QueryConstants.SEPARATOR_BYTE_ARRAY, tableName);
    }

    public static byte[] getTableKey(byte[] tenantIdBytes, String schemaName, String tableName) {
        return ByteUtil.concat(tenantIdBytes, QueryConstants.SEPARATOR_BYTE_ARRAY, schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(schemaName), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(tableName));
    }

    public static String getTableName(String schemaName, String tableName) {
        return getName(schemaName,tableName);
    }

    private static String getName(String optionalQualifier, String name) {
        if (optionalQualifier == null || optionalQualifier.isEmpty()) {
            return name;
        }
        return optionalQualifier + QueryConstants.NAME_SEPARATOR + name;
    }

    public static String getTableName(byte[] schemaName, byte[] tableName) {
        return getName(schemaName, tableName);
    }

    public static String getColumnDisplayName(byte[] cf, byte[] cq) {
        return getName(cf == null || cf.length == 0 || Bytes.compareTo(cf, QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES) == 0 ? ByteUtil.EMPTY_BYTE_ARRAY : cf, cq);
    }

    public static String getColumnDisplayName(String cf, String cq) {
        return getName(cf == null || cf.isEmpty() || QueryConstants.DEFAULT_COLUMN_FAMILY.equals(cf) ? null : cf, cq);
    }

    public static String getMetaDataEntityName(String schemaName, String tableName, String familyName, String columnName) {
        if ((schemaName == null || schemaName.isEmpty()) && (tableName == null || tableName.isEmpty())) {
            return getName(familyName, columnName);
        }
        if ((familyName == null || familyName.isEmpty()) && (columnName == null || columnName.isEmpty())) {
            return getName(schemaName, tableName);
        }
        return getName(getName(schemaName, tableName), getName(familyName, columnName));
    }

    public static String getColumnName(String familyName, String columnName) {
        return getName(familyName, columnName);
    }

    public static byte[] getTableNameAsBytes(String schemaName, String tableName) {
        if (schemaName == null || schemaName.length() == 0) {
            return StringUtil.toBytes(tableName);
        }
        return getTableNameAsBytes(StringUtil.toBytes(schemaName),StringUtil.toBytes(tableName));
    }

    public static byte[] getTableNameAsBytes(byte[] schemaName, byte[] tableName) {
        return getNameAsBytes(schemaName, tableName);
    }

    private static byte[] getNameAsBytes(byte[] nameOne, byte[] nameTwo) {
        if (nameOne == null || nameOne.length == 0) {
            return nameTwo;
        } else if ((nameTwo == null || nameTwo.length == 0)) {
            return nameOne;
        } else {
            return ByteUtil.concat(nameOne, QueryConstants.NAME_SEPARATOR_BYTES, nameTwo);
        }
    }

    public static String getName(byte[] nameOne, byte[] nameTwo) {
        return Bytes.toString(getNameAsBytes(nameOne,nameTwo));
    }

    public static int getVarCharLength(byte[] buf, int keyOffset, int maxLength) {
        return getVarCharLength(buf, keyOffset, maxLength, 1);
    }

    public static int getVarCharLength(byte[] buf, int keyOffset, int maxLength, int skipCount) {
        int length = 0;
        for (int i=0; i<skipCount; i++) {
            while (length < maxLength && buf[keyOffset+length] != QueryConstants.SEPARATOR_BYTE) {
                length++;
            }
            if (i != skipCount-1) { // skip over the separator if it's not the last one.
                length++;
            }
        }
        return length;
    }

    public static int getVarChars(byte[] rowKey, byte[][] rowKeyMetadata) {
        return getVarChars(rowKey, 0, rowKey.length, 0, rowKeyMetadata);
    }
    
    public static int getVarChars(byte[] rowKey, int colMetaDataLength, byte[][] colMetaData) {
        return getVarChars(rowKey, 0, rowKey.length, 0, colMetaDataLength, colMetaData);
    }
    
    public static int getVarChars(byte[] rowKey, int keyOffset, int keyLength, int colMetaDataOffset, byte[][] colMetaData) {
        return getVarChars(rowKey, keyOffset, keyLength, colMetaDataOffset, colMetaData.length, colMetaData);
    }
    
    public static int getVarChars(byte[] rowKey, int keyOffset, int keyLength, int colMetaDataOffset, int colMetaDataLength, byte[][] colMetaData) {
        int i, offset = keyOffset;
        for (i = colMetaDataOffset; i < colMetaDataLength && keyLength > 0; i++) {
            int length = getVarCharLength(rowKey, offset, keyLength);
            byte[] b = new byte[length];
            System.arraycopy(rowKey, offset, b, 0, length);
            offset += length + 1;
            keyLength -= length + 1;
            colMetaData[i] = b;
        }
        return i;
    }
    
    public static String findExistingColumn(PTable table, List<PColumn> columns) {
        for (PColumn column : columns) {
            PName familyName = column.getFamilyName();
            if (familyName == null) {
                try {
                    return table.getPKColumn(column.getName().getString()).getName().getString();
                } catch (ColumnNotFoundException e) {
                    continue;
                }
            } else {
                try {
                    return table.getColumnFamily(familyName.getString()).getColumn(column.getName().getString()).getName().getString();
                } catch (ColumnFamilyNotFoundException e) {
                    continue; // Shouldn't happen
                } catch (ColumnNotFoundException e) {
                    continue;
                }
            }
        }
        return null;
    }

    public static String toString(byte[][] values) {
        if (values == null) {
            return "null";
        }
        StringBuilder buf = new StringBuilder("[");
        for (byte[] value : values) {
            buf.append(Bytes.toStringBinary(value));
            buf.append(',');
        }
        buf.setCharAt(buf.length()-1, ']');
        return buf.toString();
    }

    public static String toString(PDataType type, byte[] value) {
        boolean isString = type.isCoercibleTo(PDataType.VARCHAR);
        return isString ? ("'" + type.toObject(value).toString() + "'") : type.toObject(value).toString();
    }

    public static byte[] getEmptyColumnFamily(List<PColumnFamily> families) {
        return families.isEmpty() ? QueryConstants.EMPTY_COLUMN_BYTES : families.get(0).getName().getBytes();
    }

    public static boolean isMetaTable(byte[] tableName) {
        return Bytes.compareTo(tableName, TYPE_TABLE_NAME_BYTES) == 0;
    }
    
    public static boolean isSequenceTable(byte[] tableName) {
        return Bytes.compareTo(tableName, PhoenixDatabaseMetaData.SEQUENCE_TABLE_NAME_BYTES) == 0;
    }

    public static boolean isMetaTable(byte[] schemaName, byte[] tableName) {
        return Bytes.compareTo(schemaName, PhoenixDatabaseMetaData.TYPE_SCHEMA_BYTES) == 0 && Bytes.compareTo(tableName, PhoenixDatabaseMetaData.TYPE_TABLE_BYTES) == 0;
    }
    
    public static boolean isMetaTable(String schemaName, String tableName) {
        return PhoenixDatabaseMetaData.TYPE_SCHEMA.equals(schemaName) && PhoenixDatabaseMetaData.TYPE_TABLE.equals(tableName);
    }

    // Given the splits and the rowKeySchema, find out the keys that 
    public static byte[][] processSplits(byte[][] splits, LinkedHashSet<PColumn> pkColumns, Integer saltBucketNum, boolean defaultRowKeyOrder) throws SQLException {
        // FIXME: shouldn't this return if splits.length == 0?
        if (splits == null) return null;
        // We do not accept user specified splits if the table is salted and we specify defaultRowKeyOrder. In this case,
        // throw an exception.
        if (splits.length > 0 && saltBucketNum != null && defaultRowKeyOrder) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_SPLITS_ON_SALTED_TABLE).build().buildException();
        }
        // If the splits are not specified and table is salted, pre-split the table. 
        if (splits.length == 0 && saltBucketNum != null) {
            splits = SaltingUtil.getSalteByteSplitPoints(saltBucketNum);
        }
        byte[][] newSplits = new byte[splits.length][];
        for (int i=0; i<splits.length; i++) {
            newSplits[i] = processSplit(splits[i], pkColumns); 
        }
        return newSplits;
    }

    // Go through each slot in the schema and try match it with the split byte array. If the split
    // does not confer to the schema, extends its length to match the schema.
    private static byte[] processSplit(byte[] split, LinkedHashSet<PColumn> pkColumns) {
        int pos = 0, offset = 0, maxOffset = split.length;
        Iterator<PColumn> iterator = pkColumns.iterator();
        while (pos < pkColumns.size()) {
            PColumn column = iterator.next();
            if (column.getDataType().isFixedWidth()) { // Fixed width
                int length = column.getByteSize();
                if (maxOffset - offset < length) {
                    // The split truncates the field. Fill in the rest of the part and any fields that
                    // are missing after this field.
                    int fillInLength = length - (maxOffset - offset);
                    fillInLength += estimatePartLength(pos + 1, iterator);
                    return ByteUtil.fillKey(split, split.length + fillInLength);
                }
                // Account for this field, move to next position;
                offset += length;
                pos++;
            } else { // Variable length
                // If we are the last slot, then we are done. Nothing needs to be filled in.
                if (pos == pkColumns.size() - 1) {
                    break;
                }
                while (offset < maxOffset && split[offset] != QueryConstants.SEPARATOR_BYTE) {
                    offset++;
                }
                if (offset == maxOffset) {
                    // The var-length field does not end with a separator and it's not the last field.
                    int fillInLength = 1; // SEPARATOR byte for the current var-length slot.
                    fillInLength += estimatePartLength(pos + 1, iterator);
                    return ByteUtil.fillKey(split, split.length + fillInLength);
                }
                // Move to the next position;
                offset += 1; // skip separator;
                pos++;
            }
        }
        return split;
    }

    // Estimate the key length after pos slot for schema.
    private static int estimatePartLength(int pos, Iterator<PColumn> iterator) {
        int length = 0;
        while (iterator.hasNext()) {
            PColumn column = iterator.next();
            if (column.getDataType().isFixedWidth()) {
                length += column.getByteSize();
            } else {
                length += 1; // SEPARATOR byte.
            }
        }
        return length;
    }
    
    public static final String UPGRADE_TO_2_0 = "UpgradeTo20";
    public static final Integer SYSTEM_TABLE_NULLABLE_VAR_LENGTH_COLUMNS = 3;
    public static final String UPGRADE_TO_2_1 = "UpgradeTo21";
    public static final String UPGRADE_TO_3_0 = "UpgradeTo30";

    public static boolean isUpgradeTo2Necessary(ConnectionQueryServices connServices) throws SQLException {
        HTableInterface htable = connServices.getTable(PhoenixDatabaseMetaData.TYPE_TABLE_NAME_BYTES);
        try {
            return (htable.getTableDescriptor().getValue(SchemaUtil.UPGRADE_TO_2_0) == null);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }
    
    public static void upgradeTo2IfNecessary(HRegion region, int nColumns) throws IOException {
        Scan scan = new Scan();
        scan.setRaw(true);
        scan.setMaxVersions(MetaDataProtocol.DEFAULT_MAX_META_DATA_VERSIONS);
        RegionScanner scanner = region.getScanner(scan);
        int batchSizeBytes = 100 * 1024; // 100K chunks
        int sizeBytes = 0;
        List<Pair<Mutation,Integer>> mutations =  Lists.newArrayListWithExpectedSize(10000);
        MultiVersionConsistencyControl.setThreadReadPoint(scanner.getMvccReadPoint());
        region.startRegionOperation();
        try {
            List<KeyValue> result;
            do {
                result = Lists.newArrayList();
                scanner.nextRaw(result, null);
                for (KeyValue keyValue : result) {
                    KeyValue newKeyValue = SchemaUtil.upgradeTo2IfNecessary(nColumns, keyValue);
                    if (newKeyValue != null) {
                        sizeBytes += newKeyValue.getLength();
                        if (Type.codeToType(newKeyValue.getType()) == Type.Put) {
                            // Delete old value
                            byte[] buf = keyValue.getBuffer();
                            Delete delete = new Delete(keyValue.getRow());
                            KeyValue deleteKeyValue = new KeyValue(buf, keyValue.getRowOffset(), keyValue.getRowLength(),
                                    buf, keyValue.getFamilyOffset(), keyValue.getFamilyLength(),
                                    buf, keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
                                    keyValue.getTimestamp(), Type.Delete,
                                    ByteUtil.EMPTY_BYTE_ARRAY,0,0);
                            delete.addDeleteMarker(deleteKeyValue);
                            mutations.add(new Pair<Mutation,Integer>(delete,null));
                            sizeBytes += deleteKeyValue.getLength();
                            // Put new value
                            Put put = new Put(newKeyValue.getRow());
                            put.add(newKeyValue);
                            mutations.add(new Pair<Mutation,Integer>(put,null));
                        } else if (Type.codeToType(newKeyValue.getType()) == Type.Delete){
                            // Copy delete marker using new key so that it continues
                            // to delete the key value preceding it that will be updated
                            // as well.
                            Delete delete = new Delete(newKeyValue.getRow());
                            delete.addDeleteMarker(newKeyValue);
                            mutations.add(new Pair<Mutation,Integer>(delete,null));
                        }
                        if (sizeBytes >= batchSizeBytes) {
                            commitBatch(region, mutations);
                            mutations.clear();
                            sizeBytes = 0;
                        }
                        
                    }
                }
            } while (!result.isEmpty());
            commitBatch(region, mutations);
        } finally {
            region.closeRegionOperation();
        }
    }
    
    private static final byte[] ORIG_DEF_CF_NAME = Bytes.toBytes("_0");

    public static void upgradeTo3(HRegion region, List<Mutation> mutations) throws IOException {
        Scan scan = new Scan();
        scan.setTimeRange(MetaDataProtocol.MIN_TABLE_TIMESTAMP, HConstants.LATEST_TIMESTAMP);
        scan.setRaw(true);
        scan.setMaxVersions(MetaDataProtocol.DEFAULT_MAX_META_DATA_VERSIONS);
        RegionScanner scanner = region.getScanner(scan);
        MultiVersionConsistencyControl.setThreadReadPoint(scanner.getMvccReadPoint());
        List<KeyValue> result;
        region.startRegionOperation();
        try {
            byte[] systemTableKeyPrefix = ByteUtil.concat(PhoenixDatabaseMetaData.TYPE_SCHEMA_BYTES, QueryConstants.SEPARATOR_BYTE_ARRAY, PhoenixDatabaseMetaData.TYPE_TABLE_BYTES);
            do {
                result = Lists.newArrayList();
                scanner.nextRaw(result, null);
                for (KeyValue keyValue : result) {
                    byte[] buf = keyValue.getBuffer();
                    int rowOffset = keyValue.getRowOffset();
                    int rowLength = keyValue.getRowLength();
                    if (Type.codeToType(keyValue.getType()) == Type.Put) {
                        // Delete old value
                        Delete delete = new Delete(keyValue.getRow());
                        KeyValue deleteKeyValue = new KeyValue(buf, keyValue.getRowOffset(), keyValue.getRowLength(),
                                buf, keyValue.getFamilyOffset(), keyValue.getFamilyLength(),
                                buf, keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
                                keyValue.getTimestamp(), Type.Delete,
                                ByteUtil.EMPTY_BYTE_ARRAY,0,0);
                        delete.addDeleteMarker(deleteKeyValue);
                        mutations.add(delete);
                        
                        // If not a row in our system.table, then add a new key value prefixed with a null byte for the empty tenant_id.
                        // Otherwise, we already have the mutations for the new system.table in the mutation list, so we don't add them here.
                        if (! ( Bytes.compareTo(buf, rowOffset, rowLength, systemTableKeyPrefix, 0, systemTableKeyPrefix.length) == 0 &&
                                ( rowLength == systemTableKeyPrefix.length ||
                                  buf[rowOffset + systemTableKeyPrefix.length] == QueryConstants.SEPARATOR_BYTE)) ) {
                            KeyValue newKeyValue = SchemaUtil.upgradeTo3(keyValue);
                            // Put new value
                            Put put = new Put(newKeyValue.getRow());
                            put.add(newKeyValue);
                            mutations.add(put);
                            
                            // Add a new DEFAULT_COLUMN_FAMILY_NAME key value as a table header column.
                            // We can match here on any table column header we know will occur for any
                            // table row.
                            if (Bytes.compareTo(
                                    newKeyValue.getBuffer(), newKeyValue.getQualifierOffset(), newKeyValue.getQualifierLength(),
                                    PhoenixDatabaseMetaData.COLUMN_COUNT_BYTES, 0, PhoenixDatabaseMetaData.COLUMN_COUNT_BYTES.length) == 0) {
                                KeyValue defCFNameKeyValue = new KeyValue(newKeyValue.getBuffer(), newKeyValue.getRowOffset(), newKeyValue.getRowLength(),
                                        buf, keyValue.getFamilyOffset(), keyValue.getFamilyLength(),
                                        PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME_BYTES, 0, PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME_BYTES.length,
                                        newKeyValue.getTimestamp(), Type.Put,
                                        ORIG_DEF_CF_NAME,0,ORIG_DEF_CF_NAME.length);
                                Put defCFNamePut = new Put(defCFNameKeyValue.getRow());
                                defCFNamePut.add(defCFNameKeyValue);
                                mutations.add(defCFNamePut);
                            }
                        }
                    } else if (Type.codeToType(keyValue.getType()) == Type.Delete){
                        KeyValue newKeyValue = SchemaUtil.upgradeTo3(keyValue);
                        // Copy delete marker using new key so that it continues
                        // to delete the key value preceding it that will be updated
                        // as well.
                        Delete delete = new Delete(newKeyValue.getRow());
                        // What timestamp will be used?
                        // delete.setTimestamp(keyValue.getTimestamp());
                        delete.addDeleteMarker(newKeyValue);
                        mutations.add(delete);
                    }
                }
            } while (!result.isEmpty());
        } finally {
            region.closeRegionOperation();
        }
    }
    
    private static void commitBatch(HRegion region, List<Pair<Mutation,Integer>> mutations) throws IOException {
        if (mutations.isEmpty()) {
            return;
        }
        @SuppressWarnings("unchecked")
        Pair<Mutation,Integer>[] mutationArray = new Pair[mutations.size()];
        // TODO: should we use the one that is all or none?
        region.batchMutate(mutations.toArray(mutationArray));
    }
    
    private static KeyValue upgradeTo2IfNecessary(int maxSeparators, KeyValue keyValue) {
        int originalLength = keyValue.getRowLength();
        int length = originalLength;
        int offset = keyValue.getRowOffset();
        byte[] buf = keyValue.getBuffer();
        while (originalLength - length < maxSeparators && buf[offset+length-1] == QueryConstants.SEPARATOR_BYTE) {
            length--;
        }
        if (originalLength == length) {
            return null;
        }
        return new KeyValue(buf, offset, length,
                buf, keyValue.getFamilyOffset(), keyValue.getFamilyLength(),
                buf, keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
                keyValue.getTimestamp(), Type.codeToType(keyValue.getType()),
                buf, keyValue.getValueOffset(), keyValue.getValueLength());
    }

    private static final byte[][] OLD_TO_NEW_DATA_TYPE_3_0 = new byte[PDataType.TIMESTAMP.getSqlType() + 1][];
    static {
        OLD_TO_NEW_DATA_TYPE_3_0[PDataType.TIMESTAMP.getSqlType()] = PDataType.INTEGER.toBytes(PDataType.UNSIGNED_TIMESTAMP.getSqlType());
        OLD_TO_NEW_DATA_TYPE_3_0[PDataType.TIME.getSqlType()] = PDataType.INTEGER.toBytes(PDataType.UNSIGNED_TIME.getSqlType());
        OLD_TO_NEW_DATA_TYPE_3_0[PDataType.DATE.getSqlType()] = PDataType.INTEGER.toBytes(PDataType.UNSIGNED_DATE.getSqlType());
    }
    
    
    private static byte[] updateValueIfNecessary(KeyValue keyValue) {
        byte[] buf = keyValue.getBuffer();
        if (Bytes.compareTo(
                buf, keyValue.getQualifierOffset(), keyValue.getQualifierLength(), 
                PhoenixDatabaseMetaData.DATA_TYPE_BYTES, 0, PhoenixDatabaseMetaData.DATA_TYPE_BYTES.length) == 0) {
            int sqlType = PDataType.INTEGER.getCodec().decodeInt(buf, keyValue.getValueOffset(), null);
            // Switch the DATA_TYPE index for TIME types, as they currently don't support negative values
            // In 3.0, we'll switch them to our serialized LONG type instead, as that way we can easily
            // support negative types going forward.
            if (sqlType >= 0 && sqlType < OLD_TO_NEW_DATA_TYPE_3_0.length && OLD_TO_NEW_DATA_TYPE_3_0[sqlType] != null) {
                return OLD_TO_NEW_DATA_TYPE_3_0[sqlType];
            }
        }
        return buf;
    }
    private static KeyValue upgradeTo3(KeyValue keyValue) {
        byte[] buf = keyValue.getBuffer();
        int newLength = keyValue.getRowLength() + 1;
        byte[] newKey = new byte[newLength];
        newKey[0] = QueryConstants.SEPARATOR_BYTE;
        System.arraycopy(buf, keyValue.getRowOffset(), newKey, 1, keyValue.getRowLength());
        byte[] valueBuf = updateValueIfNecessary(keyValue);
        int valueOffset = keyValue.getValueOffset();
        int valueLength = keyValue.getValueLength();
        if (valueBuf != buf) {
            valueOffset = 0;
            valueLength = valueBuf.length;
        }
        return new KeyValue(newKey, 0, newLength,
                buf, keyValue.getFamilyOffset(), keyValue.getFamilyLength(),
                buf, keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
                keyValue.getTimestamp(), Type.codeToType(keyValue.getType()),
                valueBuf, valueOffset, valueLength);
    }

    public static int upgradeColumnCount(String url, Properties info) throws SQLException {
        String upgradeStr = JDBCUtil.findProperty(url, info, UPGRADE_TO_2_0);
        return (upgradeStr == null ? 0 : Integer.parseInt(upgradeStr));
    }

    public static boolean checkIfUpgradeTo2Necessary(ConnectionQueryServices connectionQueryServices, String url,
            Properties info) throws SQLException {
        boolean isUpgrade = upgradeColumnCount(url, info) > 0;
        boolean isUpgradeNecessary = isUpgradeTo2Necessary(connectionQueryServices);
        if (!isUpgrade && isUpgradeNecessary) {
            throw new SQLException("Please run the upgrade script in bin/updateTo2.sh to ensure your data is converted correctly to the 2.0 format");
        }
        if (isUpgrade && !isUpgradeNecessary) {
            info.remove(SchemaUtil.UPGRADE_TO_2_0); // Remove this property and ignore, since upgrade has already been done
            return false;
        }
        return isUpgradeNecessary;
    }

    public static String getEscapedTableName(String schemaName, String tableName) {
        if (schemaName == null || schemaName.length() == 0) {
            return "\"" + tableName + "\"";
        }
        return "\"" + schemaName + "\"." + "\"" + tableName + "\"";
    }

    protected static PhoenixConnection addMetaDataColumn(PhoenixConnection conn, long scn, String columnDef) throws SQLException {
        String url = conn.getURL();
        Properties props = conn.getClientInfo();
        PMetaData metaData = conn.getPMetaData();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(scn));
        PhoenixConnection metaConnection = null;

        Statement stmt = null;
        try {
            metaConnection = new PhoenixConnection(conn.getQueryServices(), url, props, metaData);
            try {
                stmt = metaConnection.createStatement();
                stmt.executeUpdate("ALTER TABLE SYSTEM.\"TABLE\" ADD IF NOT EXISTS " + columnDef);
                return metaConnection;
            } finally {
                if(stmt != null) {
                    stmt.close();
                }
            }
        } finally {
            if(metaConnection != null) {
                metaConnection.close();
            }
        }
    }
    
    public static boolean columnExists(PTable table, String columnName) {
        try {
            table.getColumn(columnName);
            return true;
        } catch (ColumnNotFoundException e) {
            return false;
        } catch (AmbiguousColumnException e) {
            return true;
        }
    }
    
    public static String getSchemaNameFromFullName(String tableName) {
        int index = tableName.indexOf(QueryConstants.NAME_SEPARATOR);
        if (index < 0) {
            return ""; 
        }
        return tableName.substring(0, index);
    }
    
    public static String getTableNameFromFullName(String tableName) {
        int index = tableName.indexOf(QueryConstants.NAME_SEPARATOR);
        if (index < 0) {
            return tableName; 
        }
        return tableName.substring(index+1, tableName.length());
    }

    public static byte[] getTableKeyFromFullName(String fullTableName) {
        int index = fullTableName.indexOf(QueryConstants.NAME_SEPARATOR);
        if (index < 0) {
            return getTableKey(ByteUtil.EMPTY_BYTE_ARRAY, null, fullTableName); 
        }
        String schemaName = fullTableName.substring(0, index);
        String tableName = fullTableName.substring(index+1);
        return getTableKey(ByteUtil.EMPTY_BYTE_ARRAY, schemaName, tableName); 
    }
}
