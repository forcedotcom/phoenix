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
package com.salesforce.phoenix.coprocessor;

import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.*;
import static com.salesforce.phoenix.util.SchemaUtil.getVarCharLength;
import static com.salesforce.phoenix.util.SchemaUtil.getVarChars;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.util.*;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import com.google.common.collect.Lists;
import com.salesforce.phoenix.cache.GlobalCache;
import com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.util.*;

/**
 * 
 * Endpoint co-processor through which all Phoenix metadata mutations flow.
 * We only allow mutations to the latest version of a Phoenix table (i.e. the
 * timeStamp must be increasing).
 * For adding/dropping columns use a sequence number on the table to ensure that
 * the client has the latest version.
 * The timeStamp on the table correlates with the timeStamp on the data row.
 * TODO: we should enforce that a metadata mutation uses a timeStamp bigger than
 * any in use on the data table, b/c otherwise we can end up with data rows that
 * are not valid against a schema row.
 *
 * @author jtaylor
 * @since 0.1
 */
public class MetaDataEndpointImpl extends BaseEndpointCoprocessor implements MetaDataProtocol {
    
    private PTable buildTable(byte[] key, ImmutableBytesPtr cacheKey, HRegion region, long clientTimeStamp) throws IOException {
        Scan scan = new Scan();
        scan.setTimeRange(MIN_TABLE_TIMESTAMP, clientTimeStamp);
        scan.setStartRow(key);
        scan.setStopRow(ByteUtil.nextKey(key));
        RegionScanner scanner = region.getScanner(scan);
        Map<ImmutableBytesPtr,PTable> metaDataCache = GlobalCache.getInstance(this.getEnvironment().getConfiguration()).getMetaDataCache();
        try {
            PTable oldTable = metaDataCache.get(cacheKey);
            long tableTimeStamp = oldTable == null ? MIN_TABLE_TIMESTAMP-1 : oldTable.getTimeStamp();
            PTable newTable = getTable(scanner, clientTimeStamp, tableTimeStamp);
            if (newTable == null) {
                return null;
            }
            if (oldTable == null || tableTimeStamp < newTable.getTimeStamp()) {
                metaDataCache.put(cacheKey, newTable);
            }
            return newTable;
        } finally {
            scanner.close();
        }
    }
    
    // KeyValues for Table
    private static final KeyValue TABLE_TYPE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES);
    private static final KeyValue TABLE_SEQ_NUM_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, TABLE_SEQ_NUM_BYTES);
    private static final KeyValue COLUMN_COUNT_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, COLUMN_COUNT_BYTES);
    private static final KeyValue SALT_BUCKETS_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, SALT_BUCKETS_BYTES);
    private static final KeyValue PK_NAME_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, PK_NAME_BYTES);
    private static final List<KeyValue> TABLE_KV_COLUMNS = Arrays.<KeyValue>asList(
            TABLE_TYPE_KV,
            TABLE_SEQ_NUM_KV,
            COLUMN_COUNT_KV,
            SALT_BUCKETS_KV,
            PK_NAME_KV
            );
    static {
        Collections.sort(TABLE_KV_COLUMNS, KeyValue.COMPARATOR);
    }
    private static final int TABLE_TYPE_INDEX = TABLE_KV_COLUMNS.indexOf(TABLE_TYPE_KV);
    private static final int TABLE_SEQ_NUM_INDEX = TABLE_KV_COLUMNS.indexOf(TABLE_SEQ_NUM_KV);
    private static final int COLUMN_COUNT_INDEX = TABLE_KV_COLUMNS.indexOf(COLUMN_COUNT_KV);
    private static final int SALT_BUCKETS_INDEX = TABLE_KV_COLUMNS.indexOf(SALT_BUCKETS_KV);
    private static final int PK_NAME_INDEX = TABLE_KV_COLUMNS.indexOf(PK_NAME_KV);
    
    // KeyValues for Column
    private static final KeyValue DECIMAL_DIGITS_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, Bytes.toBytes(DECIMAL_DIGITS));
    private static final KeyValue COLUMN_SIZE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, Bytes.toBytes(COLUMN_SIZE));
    private static final KeyValue NULLABLE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, Bytes.toBytes(NULLABLE));
    private static final KeyValue DATA_TYPE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, Bytes.toBytes(DATA_TYPE));
    private static final KeyValue ORDINAL_POSITION_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, Bytes.toBytes(ORDINAL_POSITION));
    private static final KeyValue COLUMN_MODIFIER_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, Bytes.toBytes(COLUMN_MODIFIER));
    private static final List<KeyValue> COLUMN_KV_COLUMNS = Arrays.<KeyValue>asList(
            DECIMAL_DIGITS_KV,
            COLUMN_SIZE_KV,
            NULLABLE_KV,
            DATA_TYPE_KV,
            ORDINAL_POSITION_KV,
            COLUMN_MODIFIER_KV
            );
    static {
        Collections.sort(COLUMN_KV_COLUMNS, KeyValue.COMPARATOR);
    }
    private static final int DECIMAL_DIGITS_INDEX = COLUMN_KV_COLUMNS.indexOf(DECIMAL_DIGITS_KV);
    private static final int COLUMN_SIZE_INDEX = COLUMN_KV_COLUMNS.indexOf(COLUMN_SIZE_KV);
    private static final int NULLABLE_INDEX = COLUMN_KV_COLUMNS.indexOf(NULLABLE_KV);
    private static final int SQL_DATA_TYPE_INDEX = COLUMN_KV_COLUMNS.indexOf(DATA_TYPE_KV);
    private static final int ORDINAL_POSITION_INDEX = COLUMN_KV_COLUMNS.indexOf(ORDINAL_POSITION_KV);
    private static final int COLUMN_MODIFIER_INDEX = COLUMN_KV_COLUMNS.indexOf(COLUMN_MODIFIER_KV);

    private static PName newPName(byte[] keyBuffer, int keyOffset, int keyLength) {
        if (keyLength == 0) {
            return null;
        }
        int length = getVarCharLength(keyBuffer, keyOffset, keyLength);
        // TODO: PNameImpl that doesn't need to copy the bytes
        byte[] pnameBuf = new byte[length];
        System.arraycopy(keyBuffer, keyOffset, pnameBuf, 0, length);
        return new PNameImpl(pnameBuf);
    }
    
    private PTable getTable(RegionScanner scanner, long clientTimeStamp, long tableTimeStamp) throws IOException {
        List<KeyValue> results = Lists.newArrayList();
        scanner.next(results);
        if (results.isEmpty()) {
            return null;
        }
        KeyValue[] tableKeyValues = new KeyValue[TABLE_KV_COLUMNS.size()];
        KeyValue[] colKeyValues = new KeyValue[COLUMN_KV_COLUMNS.size()];
        
        // Create PTable based on KeyValues from scan
        KeyValue keyValue = results.get(0);
        byte[] keyBuffer = keyValue.getBuffer();
        int keyLength = keyValue.getRowLength();
        int keyOffset = keyValue.getRowOffset();
        int offset = getVarCharLength(keyBuffer, keyOffset, keyLength) + 1; // skip schema name
        PName tableName = newPName(keyBuffer, keyOffset + offset, keyLength-offset);
        offset += tableName.getBytes().length + 1;
        // This will prevent the client from continually looking for the current
        // table when we know that there will never be one since we disallow updates
        // unless the table is the latest
        // If we already have a table newer than the one we just found and
        // the client timestamp is less that the existing table time stamp,
        // bump up the timeStamp to right before the client time stamp, since
        // we know it can't possibly change.
        long timeStamp = keyValue.getTimestamp();
//        long timeStamp = tableTimeStamp > keyValue.getTimestamp() && 
//                         clientTimeStamp < tableTimeStamp
//                         ? clientTimeStamp-1 
//                         : keyValue.getTimestamp();

        int i = 0;
        int j = 0;
        while (i < results.size() && j < TABLE_KV_COLUMNS.size()) {
            KeyValue kv = results.get(i);
            KeyValue searchKv = TABLE_KV_COLUMNS.get(j);
            int cmp = Bytes.compareTo(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(), 
                    searchKv.getBuffer(), searchKv.getQualifierOffset(), searchKv.getQualifierLength());
            if (cmp == 0) {
                tableKeyValues[j++] = kv;
                i++;
            } else if (cmp > 0) {
                j++;
            }
        }
        // TABLE_TYPE, TABLE_SEQ_NUM and COLUMN_COUNT are required.
        if (tableKeyValues[TABLE_TYPE_INDEX] == null || tableKeyValues[TABLE_SEQ_NUM_INDEX] == null
                || tableKeyValues[COLUMN_COUNT_INDEX] == null) {
            throw new IllegalStateException("Didn't find expected key values for table row in metadata row");
        }
        KeyValue tableTypeKv = tableKeyValues[TABLE_TYPE_INDEX];
        PTableType tableType = PTableType.fromSerializedValue(tableTypeKv.getBuffer()[tableTypeKv.getValueOffset()]);
        KeyValue tableSeqNumKv = tableKeyValues[TABLE_SEQ_NUM_INDEX];
        long tableSeqNum = PDataType.RAW_LONG.getCodec().decodeLong(tableSeqNumKv.getBuffer(), tableSeqNumKv.getValueOffset(), null);
        KeyValue columnCountKv = tableKeyValues[COLUMN_COUNT_INDEX];
        int columnCount = PDataType.INTEGER.getCodec().decodeInt(columnCountKv.getBuffer(), columnCountKv.getValueOffset(), null);
        KeyValue pkNameKv = tableKeyValues[PK_NAME_INDEX];
        String pkName = pkNameKv != null ? (String)PDataType.VARCHAR.toObject(pkNameKv.getBuffer(), pkNameKv.getValueOffset(), pkNameKv.getValueLength()) : null;
        KeyValue saltBucketNumKv = tableKeyValues[SALT_BUCKETS_INDEX];
        Integer saltBucketNum = saltBucketNumKv != null ? (Integer)PDataType.INTEGER.getCodec().decodeInt(saltBucketNumKv.getBuffer(), saltBucketNumKv.getValueOffset(), null) : null;
        
        List<PColumn> columns = Lists.newArrayListWithExpectedSize(columnCount);
        while (true) {
            results.clear();
            scanner.next(results);
            if (results.isEmpty()) {
                break;
            }
            KeyValue colKv = results.get(0);
            int colKeyLength = colKv.getRowLength();
            PName colName = newPName(colKv.getBuffer(), colKv.getRowOffset() + offset, colKeyLength-offset);
            int colKeyOffset = offset + colName.getBytes().length + 1;
            PName famName = newPName(colKv.getBuffer(), colKv.getRowOffset() + colKeyOffset, colKeyLength-colKeyOffset);
            i = 0;
            j = 0;
            while (i < results.size() && j < COLUMN_KV_COLUMNS.size()) {
                KeyValue kv = results.get(i);
                KeyValue searchKv = COLUMN_KV_COLUMNS.get(j);
                int cmp = Bytes.compareTo(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(), 
                        searchKv.getBuffer(), searchKv.getQualifierOffset(), searchKv.getQualifierLength());
                if (cmp == 0) {
                    colKeyValues[j++] = kv;
                    i++;
                } else {
                    colKeyValues[j++] = null;
                }
            }
            // COLUMN_SIZE and DECIMAL_DIGIT are optional. NULLABLE, DATA_TYPE and ORDINAL_POSITION_KV are required.
            if (colKeyValues[SQL_DATA_TYPE_INDEX] == null || colKeyValues[NULLABLE_INDEX] == null
                    || colKeyValues[ORDINAL_POSITION_INDEX] == null) {
                throw new IllegalStateException("Didn't find all required key values in column metadata row");
            }
            KeyValue columnSizeKv = colKeyValues[COLUMN_SIZE_INDEX];
            Integer maxLength = columnSizeKv == null ? null : PDataType.INTEGER.getCodec().decodeInt(columnSizeKv.getBuffer(), columnSizeKv.getValueOffset(), null);
            KeyValue decimalDigitKv = colKeyValues[DECIMAL_DIGITS_INDEX];
            Integer scale = decimalDigitKv == null ? null : PDataType.INTEGER.getCodec().decodeInt(decimalDigitKv.getBuffer(), decimalDigitKv.getValueOffset(), null);
            KeyValue ordinalPositionKv = colKeyValues[ORDINAL_POSITION_INDEX];
            int position = PDataType.INTEGER.getCodec().decodeInt(ordinalPositionKv.getBuffer(), ordinalPositionKv.getValueOffset(), null);
            KeyValue nullableKv = colKeyValues[NULLABLE_INDEX];
            boolean isNullable = PDataType.INTEGER.getCodec().decodeInt(nullableKv.getBuffer(), nullableKv.getValueOffset(), null) != ResultSetMetaData.columnNoNulls;
            KeyValue sqlDataTypeKv = colKeyValues[SQL_DATA_TYPE_INDEX];
            PDataType dataType = PDataType.fromSqlType(PDataType.INTEGER.getCodec().decodeInt(sqlDataTypeKv.getBuffer(), sqlDataTypeKv.getValueOffset(), null));
            if (maxLength == null && dataType == PDataType.BINARY) dataType = PDataType.VARBINARY; // For backward compatibility.
            KeyValue columnModifierKv = colKeyValues[COLUMN_MODIFIER_INDEX];
            ColumnModifier sortOrder = columnModifierKv == null ? null : ColumnModifier.fromSystemValue(PDataType.INTEGER.getCodec().decodeInt(columnModifierKv.getBuffer(), columnModifierKv.getValueOffset(), null));
            PColumn column = new PColumnImpl(colName, famName, dataType, maxLength, scale, isNullable, position-1, sortOrder);
            columns.add(column);
        }
        
        return new PTableImpl(tableName, tableType, timeStamp, tableSeqNum, pkName, saltBucketNum, columns);
    }
    
    private PTable buildDeletedTable(byte[] key, ImmutableBytesPtr cacheKey, HRegion region, long clientTimeStamp) throws IOException {
        if (clientTimeStamp == HConstants.LATEST_TIMESTAMP) {
            return null;
        }
        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());
        scan.setTimeRange(clientTimeStamp, HConstants.LATEST_TIMESTAMP);
        scan.setStartRow(key);
        scan.setStopRow(ByteUtil.nextKey(key));
        scan.setRaw(true);
        RegionScanner scanner = region.getScanner(scan);
        List<KeyValue> results = Lists.<KeyValue>newArrayList();
        scanner.next(results);
        // HBase ignores the time range on a raw scan (HBASE-7362)
        if (!results.isEmpty() && results.get(0).getTimestamp() > clientTimeStamp) {
            KeyValue kv = results.get(0);
            if (kv.isDelete()) {
                Map<ImmutableBytesPtr,PTable> metaDataCache = GlobalCache.getInstance(this.getEnvironment().getConfiguration()).getMetaDataCache();
                PTable table = newDeletedTableMarker(kv.getTimestamp());
                metaDataCache.put(cacheKey, table);
                return table;
            }
        }
        return null;
    }
    
    private static PTable newDeletedTableMarker(long timestamp) {
        return new PTableImpl(timestamp);
    }
    private static boolean isTableDeleted(PTable table) {
        return table.getName() == null;
    }
    
    /**
     * Inserts the metadata for a Phoenix table.
     */
    @Override
    public MetaDataMutationResult createTable(List<Mutation> tableMetadata) throws IOException {
        Mutation m = tableMetadata.get(0);
        byte[][] rowKeyMetaData = new byte[2][];
        getVarChars(m.getRow(), rowKeyMetaData);
        byte[] schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        try {
            RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
            byte[] key = SchemaUtil.getTableKey(schemaName, tableName);
            HRegion region = env.getRegion();
            MetaDataMutationResult result = checkTableKeyInRegion(key, region);
            if (result != null) {
                return result; 
            }
            Integer lid = region.getLock(null, key, true);
            if (lid == null) {
                throw new IOException("Failed to acquire lock on " + Bytes.toStringBinary(rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX]) + "." + Bytes.toStringBinary(rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX]));
            }
            try {
                ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(key);
                long clientTimeStamp = getClientTimeStamp(tableMetadata);
                Map<ImmutableBytesPtr,PTable> metaDataCache = GlobalCache.getInstance(this.getEnvironment().getConfiguration()).getMetaDataCache();
                PTable table = metaDataCache.get(cacheKey);
                // We always cache the latest version - fault in if not in cache
                if (table != null || (table = buildTable(key, cacheKey, region, HConstants.LATEST_TIMESTAMP)) != null) {
                    if (table.getTimeStamp() < clientTimeStamp) {
                        // If the table is older than the client time stamp and its deleted, continue
                        if (!isTableDeleted(table)) {
                            return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, EnvironmentEdgeManager.currentTimeMillis(), table);
                        }
                    } else {
                        return new MetaDataMutationResult(MutationCode.NEWER_TABLE_FOUND, EnvironmentEdgeManager.currentTimeMillis(), table);
                    }
                }
                // if not found then call newerTableExists and add delete marker for timestamp found
                if (table == null && buildDeletedTable(key, cacheKey, region, clientTimeStamp) != null) {
                    // TODO: handle deleted table on client
                    return new MetaDataMutationResult(MutationCode.NEWER_TABLE_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
                }
                
                region.mutateRowsWithLocks(tableMetadata, Collections.<byte[]>emptySet());
                // Get timeStamp from mutations - the above method sets it if it's unset
                long currentTime = getClientTimeStamp(tableMetadata);
                // Invalidate the cache - the next getTable call will add it
                metaDataCache.remove(cacheKey);
                return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, currentTime, null);
            } finally {
                region.releaseRowLock(lid);
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException(SchemaUtil.getTableDisplayName(schemaName, tableName), t);
            return null; // impossible
        }
    }

    @Override
    public MetaDataMutationResult dropTable(List<Mutation> tableMetadata, boolean isView) throws IOException {
        Mutation m = tableMetadata.get(0);
        byte[][] rowKeyMetaData = new byte[2][];
        getVarChars(m.getRow(), rowKeyMetaData);
        byte[] schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        try {
            // Disallow deletion of a system table
            // TODO: better to check KV, but this is our only system table for now
            if (Bytes.compareTo(TYPE_SCHEMA_BYTES, schemaName) == 0 && Bytes.compareTo(TYPE_TABLE_BYTES, tableName) == 0) {
                return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION, EnvironmentEdgeManager.currentTimeMillis(), null);
            }
            RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
            byte[] key = SchemaUtil.getTableKey(schemaName, tableName);
            HRegion region = env.getRegion();
            MetaDataMutationResult result = checkTableKeyInRegion(key, region);
            if (result != null) {
                return result; 
            }
            final Integer lid = region.getLock(null, key, true);
            if (lid == null) {
                throw new IOException("Failed to acquire lock on " + Bytes.toStringBinary(rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX]) + "." + Bytes.toStringBinary(rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX]));
            }
            try {
                ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(key);
                Map<ImmutableBytesPtr,PTable> metaDataCache = GlobalCache.getInstance(this.getEnvironment().getConfiguration()).getMetaDataCache();
                PTable table = metaDataCache.get(cacheKey);
                long clientTimeStamp = m.getTimeStamp();
                if ((table != null || (table = buildTable(key, cacheKey, region, HConstants.LATEST_TIMESTAMP)) != null) && table.getTimeStamp() >= clientTimeStamp) {
                    return new MetaDataMutationResult(MutationCode.NEWER_TABLE_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
                }
                if (table == null && buildDeletedTable(key, cacheKey, region, clientTimeStamp) != null) {
                    return new MetaDataMutationResult(MutationCode.NEWER_TABLE_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
                }
                Scan scan = new Scan();
                scan.setStartRow(key);
                scan.setTimeRange(MIN_TABLE_TIMESTAMP, clientTimeStamp);
                scan.setStopRow(ByteUtil.nextKey(key));
                RegionScanner scanner = region.getScanner(scan);
                List<KeyValue> results = Lists.newArrayList();
                scanner.next(results);
                if (results.isEmpty()) {
                    return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
                }
                KeyValue typeKeyValue = KeyValueUtil.getColumnLatest(results, PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.TABLE_TYPE_BYTES);
                assert(typeKeyValue != null && typeKeyValue.getValueLength() == 1);
                PTableType type = PTableType.fromSerializedValue(typeKeyValue.getBuffer()[typeKeyValue.getValueOffset()]);
                if ( isView != (type == PTableType.VIEW) ) {
                    // We said to drop a table, but found a view or visa versa
                    return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
                }
                List<Mutation> rowsToDelete = Lists.newArrayListWithExpectedSize(10);
                do {
                    @SuppressWarnings("deprecation") // FIXME: Remove when unintentionally deprecated method is fixed (HBASE-7870).
                    // FIXME: the version of the Delete constructor without the lock args was introduced
                    // in 0.94.4, thus if we try to use it here we can no longer use the 0.94.2 version
                    // of the client.
                    Delete delete = new Delete(results.get(0).getRow(), clientTimeStamp, null);
                    rowsToDelete.add(delete);
                    results.clear();
                    scanner.next(results);
                } while (!results.isEmpty());
                region.mutateRowsWithLocks(rowsToDelete, Collections.<byte[]>emptySet());
                long currentTime = getClientTimeStamp(rowsToDelete);
                metaDataCache.put(cacheKey, newDeletedTableMarker(currentTime));
                // Return the table to the client so that the correct scan will be done to delete the data
                // We could just send a stub table with the minimum info, but I think this won't be a big deal.
                return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, currentTime, table);
            } finally {
                region.releaseRowLock(lid);
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException(SchemaUtil.getTableDisplayName(schemaName, tableName), t);
            return null; // impossible
        }
    }

    private static long getClientTimeStamp(List<Mutation> tableMetadata) {
        Mutation m = tableMetadata.get(0);
        Collection<List<KeyValue>> kvs = m.getFamilyMap().values();
        // Empty if Mutation is a Delete
        // TODO: confirm that Delete timestamp is reset like Put
        return kvs.isEmpty() ? m.getTimeStamp() : kvs.iterator().next().get(0).getTimestamp();
    }
    
    private static long getSequenceNumber(List<Mutation> tableMetaData) {
        Mutation tableMutation = tableMetaData.get(0);
        for (Mutation m : tableMetaData) {
            if (m.getRow().length < tableMutation.getRow().length) {
                tableMutation = m;
            }
        }
        List<KeyValue> kvs = tableMutation.getFamilyMap().get(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES);
        if (kvs != null) {
            for (KeyValue kv : kvs) { // list is not ordered, so search. TODO: we could potentially assume the position
                if (Bytes.compareTo(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(), PhoenixDatabaseMetaData.TABLE_SEQ_NUM_BYTES, 0, PhoenixDatabaseMetaData.TABLE_SEQ_NUM_BYTES.length) == 0) {
                    return PDataType.RAW_LONG.getCodec().decodeLong(kv.getBuffer(), kv.getValueOffset(), null);
                }
            }
        }
        throw new IllegalStateException();
    }

    private static interface Verifier {
        MetaDataMutationResult checkColumns(PTable table, byte[][] rowKeyMetaData, List<Mutation> tableMetadata);
    }
    
    private MetaDataMutationResult mutateColumn(List<Mutation> tableMetadata, Verifier verifier) throws IOException {
        Mutation m = tableMetadata.get(0);
        byte[][] rowKeyMetaData = new byte[4][];
        getVarChars(m.getRow(), 2, rowKeyMetaData);
        byte[] schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        try {
            // Disallow deletion of a system table
            // TODO: better to check KV, but this is our only system table for now
            if (Bytes.compareTo(TYPE_SCHEMA_BYTES, schemaName) == 0 && Bytes.compareTo(TYPE_TABLE_BYTES, tableName) == 0) {
                return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION, EnvironmentEdgeManager.currentTimeMillis(), null);
            }
            RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
            byte[] key = SchemaUtil.getTableKey(schemaName,tableName);
            HRegion region = env.getRegion();
            MetaDataMutationResult result = checkTableKeyInRegion(key, region);
            if (result != null) {
                return result; 
            }
            Integer lid = region.getLock(null, key, true);
            if (lid == null) {
                throw new IOException("Failed to acquire lock on " + Bytes.toStringBinary(schemaName) + "." + Bytes.toStringBinary(tableName));
            }
            try {
                ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(key);
                Map<ImmutableBytesPtr,PTable> metaDataCache = GlobalCache.getInstance(this.getEnvironment().getConfiguration()).getMetaDataCache();
                PTable table = metaDataCache.get(cacheKey);
                // Get client timeStamp from mutations
                long clientTimeStamp = getClientTimeStamp(tableMetadata);
                if (table == null && (table = buildTable(key, cacheKey, region, HConstants.LATEST_TIMESTAMP)) == null) {
                    // if not found then call newerTableExists and add delete marker for timestamp found
                    if (buildDeletedTable(key, cacheKey, region, clientTimeStamp) != null) {
                        return new MetaDataMutationResult(MutationCode.NEWER_TABLE_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
                    }
                    return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
                }
                if (table.getTimeStamp() >= clientTimeStamp) {
                    return new MetaDataMutationResult(MutationCode.NEWER_TABLE_FOUND, EnvironmentEdgeManager.currentTimeMillis(), table);
                } else if (isTableDeleted(table)) {
                    return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
                }
                    
                long expectedSeqNum = getSequenceNumber(tableMetadata) - 1; // lookup TABLE_SEQ_NUM in tableMetaData
                if (expectedSeqNum != table.getSequenceNumber()) {
                    return new MetaDataMutationResult(MutationCode.CONCURRENT_TABLE_MUTATION, EnvironmentEdgeManager.currentTimeMillis(), table);
                }
                
                result = verifier.checkColumns(table, rowKeyMetaData, tableMetadata);
                if (result != null) {
                    return result;
                }
                
                region.mutateRowsWithLocks(tableMetadata, Collections.<byte[]>emptySet());
                // Invalidate from cache
                metaDataCache.remove(cacheKey);
                // Get client timeStamp from mutations, since it may get updated by the mutateRowsWithLocks call
                long currentTime = getClientTimeStamp(tableMetadata);
                return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, currentTime, null);
            } finally {
                region.releaseRowLock(lid);
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException(SchemaUtil.getTableDisplayName(schemaName, tableName), t);
            return null; // impossible
        }
    }

    @Override
    public MetaDataMutationResult addColumn(List<Mutation> tableMetaData) throws IOException {
        return mutateColumn(tableMetaData, new Verifier() {
            @Override
            public MetaDataMutationResult checkColumns(PTable table, byte[][] rowKeyMetaData, List<Mutation> tableMetaData) {
                int keyOffset = rowKeyMetaData[SCHEMA_NAME_INDEX].length + rowKeyMetaData[TABLE_NAME_INDEX].length + 2;
                for (Mutation m : tableMetaData) {
                    byte[] key = m.getRow();
                    int pkCount = getVarChars(key, keyOffset, key.length-keyOffset, 2, rowKeyMetaData);
                    try {
                        if (pkCount > FAMILY_NAME_INDEX && rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX].length > 0) {
                            PColumnFamily family = table.getColumnFamily(rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX]);
                            family.getColumn(rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX]);
                        } else if (pkCount > COLUMN_NAME_INDEX && rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX].length > 0) {
                            table.getPKColumn(new String(rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX]));
                        } else {
                            continue;
                        }
                        return new MetaDataMutationResult(MutationCode.COLUMN_ALREADY_EXISTS, EnvironmentEdgeManager.currentTimeMillis(), table);
                    } catch (ColumnFamilyNotFoundException e) {
                        continue;
                    } catch (ColumnNotFoundException e) {
                        continue;
                    }
                }
                return null;
            }
        });
    }
    
    @Override
    public MetaDataMutationResult dropColumn(List<Mutation> tableMetaData) throws IOException {
        return mutateColumn(tableMetaData, new Verifier() {
            @Override
            public MetaDataMutationResult checkColumns(PTable table, byte[][] rowKeyMetaData, List<Mutation> tableMetaData) {
                int keyOffset = rowKeyMetaData[SCHEMA_NAME_INDEX].length + rowKeyMetaData[TABLE_NAME_INDEX].length + 2;
                boolean deletePKColumn = false;
                for (Mutation m : tableMetaData) {
                    byte[] key = m.getRow();
                    int pkCount = getVarChars(key, keyOffset, key.length-keyOffset, 2, rowKeyMetaData);
                    try {
                        if (pkCount > FAMILY_NAME_INDEX && rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX].length > 0) {
                            PColumnFamily family = table.getColumnFamily(rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX]);
                            family.getColumn(rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX]);
                        } else if (pkCount > COLUMN_NAME_INDEX && rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX].length > 0) {
                            deletePKColumn = true;
                            table.getPKColumn(new String(rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX]));
                        }
                    } catch (ColumnFamilyNotFoundException e) {
                        return new MetaDataMutationResult(MutationCode.COLUMN_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), table);
                    } catch (ColumnNotFoundException e) {
                        return new MetaDataMutationResult(MutationCode.COLUMN_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), table);
                    }
                }
                if (deletePKColumn) {
                    if (table.getPKColumns().size() == 1) {
                        return new MetaDataMutationResult(MutationCode.NO_PK_COLUMNS, EnvironmentEdgeManager.currentTimeMillis(), null);
                    }
                }
                return null;
            }
        });
    }
    
    private static MetaDataMutationResult checkTableKeyInRegion(byte[] key, HRegion region) {
        byte[] startKey = region.getStartKey();
        byte[] endKey = region.getEndKey();
        if (Bytes.compareTo(startKey, key) <= 0 && (Bytes.compareTo(HConstants.LAST_ROW, endKey) == 0 || Bytes.compareTo(key, endKey) < 0)) {
            return null; // normal case;
        }
        return new MetaDataMutationResult(MutationCode.TABLE_NOT_IN_REGION, EnvironmentEdgeManager.currentTimeMillis(), null);
    }
    
    @Override
    public MetaDataMutationResult getTable(byte[] schemaName, byte[] tableName, long tableTimeStamp, long clientTimeStamp) throws IOException {
        try {
            // get the coprocessor environment
            RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
            byte[] key = SchemaUtil.getTableKey(schemaName, tableName);
            // TODO: check that key is within region.getStartKey() and region.getEndKey()
            // and return special code to force client to lookup region from meta.
            HRegion region = env.getRegion();
            MetaDataMutationResult result = checkTableKeyInRegion(key, region);
            if (result != null) {
                return result; 
            }
            
            ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(key);
            Map<ImmutableBytesPtr,PTable> metaDataCache = GlobalCache.getInstance(this.getEnvironment().getConfiguration()).getMetaDataCache();
            PTable table = metaDataCache.get(cacheKey);
            // We only cache the latest, so we'll end up building the table with every call if the client connection has specified an SCN.
            // TODO: If we indicate to the client that we're returning an older version, but there's a newer version available, the client
            // can safely not call this, since we only allow modifications to the latest.
            long currentTime = EnvironmentEdgeManager.currentTimeMillis();
            if (table != null && table.getTimeStamp() < clientTimeStamp) {
                // Table on client is up-to-date with table on server, so just return
                if (isTableDeleted(table)) {
                    return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, currentTime, null);
                } else {
                    return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, currentTime, table.getTimeStamp() != tableTimeStamp ? table : null);
                }
            }
            // Ask Lars about the expense of this call - if we don't take the lock, we still won't get partial results
            Integer lid = region.getLock(null, key, true);
            if (lid == null) {
                throw new IOException("Failed to acquire lock on " + Bytes.toStringBinary(key));
            }
            try {
                // Query for the latest table first, since it's not cached
                table = buildTable(key, cacheKey, region, HConstants.LATEST_TIMESTAMP);
                if (table != null && table.getTimeStamp() < clientTimeStamp) {
                    return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, currentTime, table);
                }
                // Otherwise, query for an older version of the table - it won't be cached 
                table = buildTable(key, cacheKey, region, clientTimeStamp);
                return new MetaDataMutationResult(table == null ? MutationCode.TABLE_NOT_FOUND : MutationCode.TABLE_ALREADY_EXISTS, currentTime, table);
            } finally {
                region.releaseRowLock(lid);
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException(SchemaUtil.getTableDisplayName(schemaName, tableName), t);
            return null; // impossible
        }
    }

    @Override
    public void clearCache() {
        Map<ImmutableBytesPtr,PTable> metaDataCache = GlobalCache.getInstance(this.getEnvironment().getConfiguration()).getMetaDataCache();
        metaDataCache.clear();
    }

    @Override
    public long getVersion() {
        // The first 3 bytes of the long is used to encoding the HBase version as major.minor.patch.
        // The next 3 bytes of the value is used to encode the Phoenix version as major.minor.patch.
        return MetaDataUtil.encodeHBaseAndPhoenixVersions(this.getEnvironment().getHBaseVersion());
    }

}
