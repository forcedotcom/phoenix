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
package com.salesforce.phoenix.execute;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.cache.ServerCacheClient;
import com.salesforce.phoenix.cache.ServerCacheClient.ServerCache;
import com.salesforce.phoenix.index.IndexMetaDataCacheClient;
import com.salesforce.phoenix.index.PhoenixIndexCodec;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.schema.IllegalDataException;
import com.salesforce.phoenix.schema.MetaDataClient;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PRow;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.IndexUtil;
import com.salesforce.phoenix.util.SQLCloseable;
import com.salesforce.phoenix.util.ServerUtil;

/**
 * 
 * Tracks the uncommitted state
 *
 * @author jtaylor
 * @since 0.1
 */
public class MutationState implements SQLCloseable {
    private static final Logger logger = LoggerFactory.getLogger(MutationState.class);

    private PhoenixConnection connection;
    private final long maxSize;
    private final ImmutableBytesPtr tempPtr = new ImmutableBytesPtr();
    private final Map<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> mutations = Maps.newHashMapWithExpectedSize(3); // TODO: Sizing?
    private final long sizeOffset;
    private int numEntries = 0;

    public MutationState(int maxSize, PhoenixConnection connection) {
        this(maxSize,connection,0);
    }
    
    public MutationState(int maxSize, PhoenixConnection connection, long sizeOffset) {
        this.maxSize = maxSize;
        this.connection = connection;
        this.sizeOffset = sizeOffset;
    }
    
    public MutationState(TableRef table, Map<ImmutableBytesPtr,Map<PColumn,byte[]>> mutations, long sizeOffset, long maxSize, PhoenixConnection connection) {
        this.maxSize = maxSize;
        this.connection = connection;
        this.mutations.put(table, mutations);
        this.sizeOffset = sizeOffset;
        this.numEntries = mutations.size();
        throwIfTooBig();
    }
    
    private MutationState(List<Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>>> entries, long sizeOffset, long maxSize, PhoenixConnection connection) {
        this.maxSize = maxSize;
        this.connection = connection;
        this.sizeOffset = sizeOffset;
        for (Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> entry : entries) {
            numEntries += entry.getValue().size();
            this.mutations.put(entry.getKey(), entry.getValue());
        }
        throwIfTooBig();
    }
    
    private void throwIfTooBig() {
        if (numEntries > maxSize) {
            // TODO: throw SQLException ?
            throw new IllegalArgumentException("MutationState size of " + numEntries + " is bigger than max allowed size of " + maxSize);
        }
    }
    
    public long getUpdateCount() {
        return sizeOffset + numEntries;
    }
    /**
     * Combine a newer mutation with this one, where in the event of overlaps,
     * the newer one will take precedence.
     * @param newMutation the newer mutation
     */
    public void join(MutationState newMutation) {
        if (this == newMutation) { // Doesn't make sense
            return;
        }
        // Merge newMutation with this one, keeping state from newMutation for any overlaps
        for (Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> entry : newMutation.mutations.entrySet()) {
            // Replace existing entries for the table with new entries
            Map<ImmutableBytesPtr,Map<PColumn,byte[]>> existingRows = this.mutations.put(entry.getKey(), entry.getValue());
            if (existingRows != null) { // Rows for that table already exist
                // Loop through new rows and replace existing with new
                for (Map.Entry<ImmutableBytesPtr,Map<PColumn,byte[]>> rowEntry : entry.getValue().entrySet()) {
                    // Replace existing row with new row
                    Map<PColumn,byte[]> existingValues = existingRows.put(rowEntry.getKey(), rowEntry.getValue());
                    if (existingValues != null) {
                        Map<PColumn,byte[]> newRow = rowEntry.getValue();
                        // if new row is null, it means delete, and we don't need to merge it with existing row. 
                        if (newRow != null) {
                            // Replace existing column values with new column values
                            for (Map.Entry<PColumn,byte[]> valueEntry : newRow.entrySet()) {
                                existingValues.put(valueEntry.getKey(), valueEntry.getValue());
                            }
                            // Now that the existing row has been merged with the new row, replace it back
                            // again (since it was replaced with the new one above).
                            existingRows.put(rowEntry.getKey(), existingValues);
                        }
                    } else {
                        numEntries++;
                    }
                }
                // Put the existing one back now that it's merged
                this.mutations.put(entry.getKey(), existingRows);
            } else {
                numEntries += entry.getValue().size();
            }
        }
        throwIfTooBig();
    }
    
    private Iterator<Pair<byte[],List<Mutation>>> addRowMutations(final TableRef tableRef, final Map<ImmutableBytesPtr, Map<PColumn, byte[]>> values, long timestamp, boolean includeMutableIndexes) {
        final List<Mutation> mutations = Lists.newArrayListWithExpectedSize(values.size());
        Iterator<Map.Entry<ImmutableBytesPtr,Map<PColumn,byte[]>>> iterator = values.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<ImmutableBytesPtr,Map<PColumn,byte[]>> rowEntry = iterator.next();
            ImmutableBytesPtr key = rowEntry.getKey();
            PRow row = tableRef.getTable().newRow(timestamp, key);
            if (rowEntry.getValue() == null) { // means delete
                row.delete();
            } else {
                for (Map.Entry<PColumn,byte[]> valueEntry : rowEntry.getValue().entrySet()) {
                    row.setValue(valueEntry.getKey(), valueEntry.getValue());
                }
            }
            mutations.addAll(row.toRowMutations());
        }
        final Iterator<PTable> indexes = // Only maintain tables with immutable rows through this client-side mechanism
                (tableRef.getTable().isImmutableRows() || includeMutableIndexes) ? 
                        tableRef.getTable().getIndexes().iterator() : 
                        Iterators.<PTable>emptyIterator();
        return new Iterator<Pair<byte[],List<Mutation>>>() {
            boolean isFirst = true;

            @Override
            public boolean hasNext() {
                return isFirst || indexes.hasNext();
            }

            @Override
            public Pair<byte[], List<Mutation>> next() {
                if (isFirst) {
                    isFirst = false;
                    return new Pair<byte[],List<Mutation>>(tableRef.getTable().getName().getBytes(),mutations);
                }
                PTable index = indexes.next();
                List<Mutation> indexMutations;
                try {
                    indexMutations = IndexUtil.generateIndexData(tableRef.getTable(), index, mutations, tempPtr);
                } catch (SQLException e) {
                    throw new IllegalDataException(e);
                }
                return new Pair<byte[],List<Mutation>>(index.getName().getBytes(),indexMutations);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
            
        };
    }
    
    /**
     * Get the unsorted list of HBase mutations for the tables with uncommitted data.
     * @return list of HBase mutations for uncommitted data.
     */
    public Iterator<Pair<byte[],List<Mutation>>> toMutations() {
        return toMutations(false);
    }
    
    public Iterator<Pair<byte[],List<Mutation>>> toMutations(final boolean includeMutableIndexes) {
        final Iterator<Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>>> iterator = this.mutations.entrySet().iterator();
        if (!iterator.hasNext()) {
            return Iterators.emptyIterator();
        }
        Long scn = connection.getSCN();
        final long timestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        return new Iterator<Pair<byte[],List<Mutation>>>() {
            private Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> current = iterator.next();
            private Iterator<Pair<byte[],List<Mutation>>> innerIterator = init();
                    
            private Iterator<Pair<byte[],List<Mutation>>> init() {
                return addRowMutations(current.getKey(), current.getValue(), timestamp, includeMutableIndexes);
            }
            
            @Override
            public boolean hasNext() {
                return innerIterator.hasNext() || iterator.hasNext();
            }

            @Override
            public Pair<byte[], List<Mutation>> next() {
                if (!innerIterator.hasNext()) {
                    current = iterator.next();
                }
                return innerIterator.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
            
        };
    }
        
    /**
     * Validates that the meta data is still valid based on the current server time
     * and returns the server time to use for the upsert for each table.
     * @param connection
     * @return the server time to use for the upsert
     * @throws SQLException if the table or any columns no longer exist
     */
    private long[] validate() throws SQLException {
        int i = 0;
        Long scn = connection.getSCN();
        MetaDataClient client = new MetaDataClient(connection);
        long[] timeStamps = new long[this.mutations.size()];
        for (Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> entry : mutations.entrySet()) {
            TableRef tableRef = entry.getKey();
            long serverTimeStamp = tableRef.getTimeStamp();
            PTable table = tableRef.getTable();
            if (!connection.getAutoCommit()) {
                serverTimeStamp = client.updateCache(table.getSchemaName().getString(), table.getTableName().getString());
                if (serverTimeStamp < 0) {
                    serverTimeStamp *= -1;
                    // TODO: use bitset?
                    PColumn[] columns = new PColumn[table.getColumns().size()];
                    for (Map.Entry<ImmutableBytesPtr,Map<PColumn,byte[]>> rowEntry : entry.getValue().entrySet()) {
                        Map<PColumn,byte[]> valueEntry = rowEntry.getValue();
                        if (valueEntry != null) {
                            for (PColumn column : valueEntry.keySet()) {
                                columns[column.getPosition()] = column;
                            }
                        }
                    }
                    table = connection.getPMetaData().getTable(tableRef.getTable().getName().getString());
                    for (PColumn column : columns) {
                        if (column != null) {
                            table.getColumnFamily(column.getFamilyName().getString()).getColumn(column.getName().getString());
                        }
                    }
                }
            }
            timeStamps[i++] = scn == null ? serverTimeStamp : scn;
        }
        return timeStamps;
    }
    
    private static void logMutationSize(HTableInterface htable, List<Mutation> mutations) {
        long byteSize = 0;
        int keyValueCount = 0;
        for (Mutation mutation : mutations) {
            for (Entry<byte[], List<KeyValue>> entry : mutation.getFamilyMap().entrySet()) {
                for (KeyValue kv : entry.getValue()) {
                    byteSize += kv.getBuffer().length;
                    keyValueCount++;
                }
            }
        }
        logger.debug("Sending " + mutations.size() + " mutations for " + Bytes.toString(htable.getTableName()) + " with " + keyValueCount + " key values of total size " + byteSize + " bytes");
    }
    
    public void commit() throws SQLException {
        int i = 0;
        long[] serverTimeStamps = validate();
        Iterator<Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>>> iterator = this.mutations.entrySet().iterator();
        List<Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>>> committedList = Lists.newArrayListWithCapacity(this.mutations.size());
        while (iterator.hasNext()) {
            Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> entry = iterator.next();
            Map<ImmutableBytesPtr,Map<PColumn,byte[]>> valuesMap = entry.getValue();
            TableRef tableRef = entry.getKey();
            PTable table = tableRef.getTable();
            table.getIndexMaintainers(tempPtr);
            boolean hasIndexMaintainers = tempPtr.getLength() > 0;
            boolean isDataTable = true;
            long serverTimestamp = serverTimeStamps[i++];
            Iterator<Pair<byte[],List<Mutation>>> mutationsIterator = addRowMutations(tableRef, valuesMap, serverTimestamp, false);
            while (mutationsIterator.hasNext()) {
                Pair<byte[],List<Mutation>> pair = mutationsIterator.next();
                byte[] htableName = pair.getFirst();
                List<Mutation> mutations = pair.getSecond();
                
                ServerCache cache = null;
                if (hasIndexMaintainers && isDataTable) {
                    byte[] attribValue = null;
                    byte[] uuidValue;
                    if (IndexMetaDataCacheClient.useIndexMetadataCache(mutations, tempPtr.getLength())) {
                        IndexMetaDataCacheClient client = new IndexMetaDataCacheClient(connection, tableRef);
                        cache = client.addIndexMetadataCache(mutations, tempPtr);
                        uuidValue = cache.getId();
                    } else {
                        attribValue = ByteUtil.copyKeyBytesIfNecessary(tempPtr);
                        uuidValue = ServerCacheClient.generateId();
                    }
                    // Either set the UUID to be able to access the index metadata from the cache
                    // or set the index metadata directly on the Mutation
                    for (Mutation mutation : mutations) {
                        mutation.setAttribute(PhoenixIndexCodec.INDEX_UUID, uuidValue);
                        if (attribValue != null) {
                            mutation.setAttribute(PhoenixIndexCodec.INDEX_MD, attribValue);
                        }
                    }
                }
                
                SQLException sqlE = null;
                HTableInterface hTable = connection.getQueryServices().getTable(htableName);
                try {
                    if (logger.isDebugEnabled()) logMutationSize(hTable, mutations);
                    long startTime = System.currentTimeMillis();
                    hTable.batch(mutations);
                    if (logger.isDebugEnabled()) logger.debug("Total time for batch call of  " + mutations.size() + " mutations into " + table.getName().getString() + ": " + (System.currentTimeMillis() - startTime) + " ms");
                    committedList.add(entry);
                } catch (Exception e) {
                    // Throw to client with both what was committed so far and what is left to be committed.
                    // That way, client can either undo what was done or try again with what was not done.
                    sqlE = new CommitException(e, this, new MutationState(committedList, this.sizeOffset, this.maxSize, this.connection));
                } finally {
                    try {
                        hTable.close();
                    } catch (IOException e) {
                        if (sqlE != null) {
                            sqlE.setNextException(ServerUtil.parseServerException(e));
                        } else {
                            sqlE = ServerUtil.parseServerException(e);
                        }
                    } finally {
                        try {
                            if (cache != null) {
                                cache.close();
                            }
                        } finally {
                            if (sqlE != null) {
                                throw sqlE;
                            }
                        }
                    }
                }
                isDataTable = false;
            }
            numEntries -= entry.getValue().size();
            iterator.remove(); // Remove batches as we process them
        }
        assert(numEntries==0);
        assert(this.mutations.isEmpty());
    }
    
    public void rollback(PhoenixConnection connection) throws SQLException {
        this.mutations.clear();
        numEntries = 0;
    }
    
    @Override
    public void close() throws SQLException {
    }
}
