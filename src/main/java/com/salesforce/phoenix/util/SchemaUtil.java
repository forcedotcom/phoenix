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

import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.*;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.phoenix.coprocessor.MetaDataProtocol;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData;
import com.salesforce.phoenix.parse.HintNode.Hint;
import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.*;



/**
 * 
 * Static class for various schema-related utilities
 *
 * @author jtaylor
 * @since 0.1
 */
public class SchemaUtil {
    private static final Logger logger = LoggerFactory.getLogger(SchemaUtil.class);
    private static final int VAR_LENGTH_ESTIMATE = 10;
    
    public static final DataBlockEncoding DEFAULT_DATA_BLOCK_ENCODING = DataBlockEncoding.FAST_DIFF;
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

    /**
     * Get the key used in the Phoenix metadata row for a table definition
     * @param schemaName
     * @param tableName
     */
    public static byte[] getTableKey(byte[] schemaName, byte[] tableName) {
        return ByteUtil.concat(schemaName, QueryConstants.SEPARATOR_BYTE_ARRAY, tableName);
    }

    public static byte[] getTableKey(String schemaName, String tableName) {
        return ByteUtil.concat(schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(schemaName), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(tableName));
    }

    public static String getTableDisplayName(String schemaName, String tableName) {
        return Bytes.toStringBinary(getTableName(schemaName,tableName));
    }

    public static String getTableDisplayName(byte[] schemaName, byte[] tableName) {
        return Bytes.toStringBinary(getTableName(schemaName,tableName));
    }

    public static String getTableDisplayName(byte[] tableName) {
        return Bytes.toStringBinary(tableName);
    }

    public static String getColumnDisplayName(String schemaName, String tableName, String familyName, String columnName) {
        return Bytes.toStringBinary(getColumnName(
                StringUtil.toBytes(schemaName), StringUtil.toBytes(tableName), 
                StringUtil.toBytes(familyName), StringUtil.toBytes(columnName)));
    }

    public static String getColumnDisplayName(String familyName, String columnName) {
        return getTableDisplayName(familyName, columnName);
    }

    /**
     * Get the HTable name for a given schemaName and tableName
     * @param tableName
     */
    public static byte[] getTableName(String tableName) {
        return getTableName(null, tableName);
    }

    public static byte[] getTableName(String schemaName, String tableName) {
        if (schemaName == null || schemaName.length() == 0) {
            return getTableName(StringUtil.toBytes(tableName));
        }
        return getTableName(StringUtil.toBytes(schemaName),StringUtil.toBytes(tableName));
    }

    public static byte[] getTableName(byte[] tableName) {
        return getTableName(null, tableName);
    }

    public static byte[] getTableName(byte[] schemaName, byte[] tableName) {
        return concatTwoNames(schemaName, tableName);
    }

    public static byte[] getColumnName(byte[] schemaName, byte[] tableName, byte[] familyName, byte[] columnName) {
        byte[] tableNamePart = concatTwoNames(schemaName, tableName);
        byte[] columnNamePart = concatTwoNames(familyName, columnName);
        return concatTwoNames(tableNamePart, columnNamePart);
    }

    private static byte[] concatTwoNames(byte[] nameOne, byte[] nameTwo) {
        if (nameOne == null || nameOne.length == 0) {
            return nameTwo;
        } else if ((nameTwo == null || nameTwo.length == 0)) {
            return nameOne;
        } else {
            return ByteUtil.concat(nameOne, QueryConstants.NAME_SEPARATOR_BYTES, nameTwo);
        }
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
        return Bytes.compareTo(tableName, TYPE_TABLE_NAME) == 0;
    }

    public static boolean isMetaTable(String schemaName, String tableName) {
        return PhoenixDatabaseMetaData.TYPE_SCHEMA.equals(schemaName) && PhoenixDatabaseMetaData.TYPE_TABLE.equals(tableName);
    }

    // Given the splits and the rowKeySchema, find out the keys that 
    public static byte[][] processSplits(byte[][] splits, List<PColumn> pkColumns, Integer saltBucketNum, boolean defaultRowKeyOrder) throws SQLException {
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
    private static byte[] processSplit(byte[] split, List<PColumn> pkColumns) {
        int pos = 0, offset = 0, maxOffset = split.length;
        while (pos < pkColumns.size()) {
            PColumn column = pkColumns.get(pos);
            if (column.getDataType().isFixedWidth()) { // Fixed width
                int length = column.getByteSize();
                if (maxOffset - offset < length) {
                    // The split truncates the field. Fill in the rest of the part and any fields that
                    // are missing after this field.
                    int fillInLength = length - (maxOffset - offset);
                    fillInLength += estimatePartLength(pos + 1, pkColumns);
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
                    fillInLength += estimatePartLength(pos + 1, pkColumns);
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
    private static int estimatePartLength(int pos, List<PColumn> pkColumns) {
        int length = 0;
        while (pos < pkColumns.size()) {
            PColumn column = pkColumns.get(pos++);
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

    public static boolean isUpgradeTo2Necessary(ConnectionQueryServices connServices) throws SQLException {
        HTableInterface htable = connServices.getTable(PhoenixDatabaseMetaData.TYPE_TABLE_NAME);
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
        return true;
    }

    public static String getEscapedTableName(String schemaName, String tableName) {
        if (schemaName == null || schemaName.length() == 0) {
            return "\"" + tableName + "\"";
        }
        return "\"" + schemaName + "\"." + "\"" + tableName + "\"";
    }

    private static PhoenixConnection addMetaDataColumn(PhoenixConnection conn, long scn, String columnDef) throws SQLException {
        String url = conn.getURL();
        Properties props = conn.getClientInfo();
        PMetaData metaData = conn.getPMetaData();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(scn));
        PhoenixConnection metaConnection = new PhoenixConnection(conn.getQueryServices(), url, props, metaData);
        try {
            metaConnection.createStatement().executeUpdate("ALTER TABLE SYSTEM.\"TABLE\" ADD IF NOT EXISTS " + columnDef);
            return metaConnection;
        } finally {
            metaConnection.close();
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
    
    public static void updateSystemTableTo2(PhoenixConnection metaConnection, PTable table) throws SQLException {
        PTable metaTable = metaConnection.getPMetaData().getSchema(PhoenixDatabaseMetaData.TYPE_SCHEMA).getTable(PhoenixDatabaseMetaData.TYPE_TABLE);
        // Execute alter table statement for each column that was added if not already added
        if (metaTable.getTimeStamp() < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP - 1) {
            // Causes row key of system table to be upgraded
            if (checkIfUpgradeTo2Necessary(metaConnection.getQueryServices(), metaConnection.getURL(), metaConnection.getClientInfo())) {
                metaConnection.createStatement().executeQuery("select count(*) from " + PhoenixDatabaseMetaData.TYPE_SCHEMA_AND_TABLE).next();
            }
            
            if (metaTable.getTimeStamp() < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP - 3 && !columnExists(table, DATA_TABLE_NAME)) {
                metaConnection = addMetaDataColumn(metaConnection, MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP - 3, DATA_TABLE_NAME + " VARCHAR NULL");
            }
            if (metaTable.getTimeStamp() < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP - 2 && !columnExists(table, INDEX_STATE)) {
                metaConnection = addMetaDataColumn(metaConnection, MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP - 2, INDEX_STATE + " VARCHAR NULL");
            }
            if (!columnExists(table, IMMUTABLE_ROWS)) {
                addMetaDataColumn(metaConnection, MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP - 1, IMMUTABLE_ROWS + " BOOLEAN NULL");
            }
        }
    }
    
    public static void upgradeTo2(PhoenixConnection conn) throws SQLException {
        /*
         * Our upgrade hack sets a property on the scan that gets activated by an ungrouped aggregate query. 
         * Our UngroupedAggregateRegionObserver coprocessors will perform the required upgrade.
         * The SYSTEM.TABLE will already have been upgraded, so walk through each user table and upgrade
         * any table with a nullable variable length column (VARCHAR or DECIMAL)
         * at the end.
         */
        String query = "select /*+" + Hint.NO_INTRA_REGION_PARALLELIZATION + "*/ " +
                TABLE_SCHEM_NAME + "," +
                TABLE_NAME_NAME + " ," +
                DATA_TYPE + "," +
                NULLABLE +
                " from " + TYPE_SCHEMA_AND_TABLE + 
                " where " + TABLE_CAT_NAME + " is null " +
                " and " + COLUMN_NAME + " is not null " +
                " and " + TABLE_TYPE_NAME  + " = '" + PTableType.USER.getSerializedValue() + "'" +
                " order by " + TABLE_SCHEM_NAME + "," + TABLE_NAME_NAME + "," + ORDINAL_POSITION + " DESC";
        ResultSet rs = conn.createStatement().executeQuery(query);
        String currentTableName = null;
        int nColumns = 0;
        boolean skipToNext = false;
        Map<String,Integer> tablesToUpgrade = Maps.newHashMap();
        while (rs.next()) {
            String tableName = getEscapedTableName(rs.getString(1), rs.getString(2));
            if (currentTableName == null) {
                currentTableName = tableName;
            } else if (!currentTableName.equals(tableName)) {
                if (nColumns > 0) {
                    tablesToUpgrade.put(currentTableName, nColumns);
                    nColumns = 0;
                }
                currentTableName = tableName;
                skipToNext = false;
            } else if (skipToNext) {
                continue;
            }
            PDataType dataType = PDataType.fromSqlType(rs.getInt(3));
            if (dataType.isFixedWidth() || rs.getInt(4) != DatabaseMetaData.attributeNullable) {
                skipToNext = true;
            } else {
                nColumns++;
            }
        }
        
        for (Map.Entry<String, Integer> entry : tablesToUpgrade.entrySet()) {
            String msg = "Upgrading " + entry.getKey() + " for " + entry.getValue() + " columns";
            logger.info(msg);
            System.out.println(msg);
            Properties props = new Properties(conn.getClientInfo());
            props.setProperty(UPGRADE_TO_2_0, entry.getValue().toString());
            Connection newConn = DriverManager.getConnection(conn.getURL(), props);
            try {
                rs = newConn.createStatement().executeQuery("select /*+" + Hint.NO_INTRA_REGION_PARALLELIZATION + "*/ count(*) from " + entry.getKey());
                rs.next();
            } finally {
                newConn.close();
            }
        }
        
        ConnectionQueryServices connServices = conn.getQueryServices();
        HTableInterface htable = null;
        HBaseAdmin admin = connServices.getAdmin();
        try {
            htable = connServices.getTable(PhoenixDatabaseMetaData.TYPE_TABLE_NAME);
            HTableDescriptor htd = new HTableDescriptor(htable.getTableDescriptor());
            htd.setValue(SchemaUtil.UPGRADE_TO_2_0, Boolean.TRUE.toString());
            admin.disableTable(PhoenixDatabaseMetaData.TYPE_TABLE_NAME);
            admin.modifyTable(PhoenixDatabaseMetaData.TYPE_TABLE_NAME, htd);
            admin.enableTable(PhoenixDatabaseMetaData.TYPE_TABLE_NAME);
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            try {
                try {
                    admin.close();
                } finally {
                    htable.close();
                }
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
    }
}
