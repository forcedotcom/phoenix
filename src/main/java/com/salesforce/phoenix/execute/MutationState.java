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
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.phoenix.exception.PhoenixIOException;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.util.ImmutableBytesPtr;
import com.salesforce.phoenix.util.SQLCloseable;

/**
 * 
 * Tracks the uncommitted state
 *
 * @author jtaylor
 * @since 0.1
 */
public class MutationState implements SQLCloseable {
    private PhoenixConnection connection;
    private final long maxSize;
    private final Map<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> mutations = Maps.newHashMapWithExpectedSize(3); // TODO: Sizing?
    private final long sizeOffset;
    private int numEntries = 0;

    public MutationState(int maxSize, PhoenixConnection connection) {
        this.maxSize = maxSize;
        this.connection = connection;
        this.sizeOffset = 0;
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
        // Merge newMutation with this one, keeping state from newMutation for any overlaps
        for (Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> entry : newMutation.mutations.entrySet()) {
            Map<ImmutableBytesPtr,Map<PColumn,byte[]>> existing = this.mutations.put(entry.getKey(), entry.getValue());
            if (existing != null) {
                for (Map.Entry<ImmutableBytesPtr,Map<PColumn,byte[]>> rowEntry : entry.getValue().entrySet()) {
                    Map<PColumn,byte[]> existingRow = existing.put(rowEntry.getKey(), rowEntry.getValue());
                    if (existingRow != null) {
                        for (Map.Entry<PColumn,byte[]> valueEntry : rowEntry.getValue().entrySet()) {
                            existingRow.put(valueEntry.getKey(), valueEntry.getValue());
                        }
                    } else {
                        numEntries++;
                    }
                }
                // Put the existing one back now that it's merged
                this.mutations.put(entry.getKey(), existing);
            } else {
                numEntries += entry.getValue().size();
            }
        }
        throwIfTooBig();
    }
    
    private static void addRowMutations(PTable table, Iterator<Entry<ImmutableBytesPtr, Map<PColumn, byte[]>>> iterator, long timestamp, List<Mutation> mutations) {
        while (iterator.hasNext()) {
            Map.Entry<ImmutableBytesPtr,Map<PColumn,byte[]>> rowEntry = iterator.next();
            ImmutableBytesPtr key = rowEntry.getKey();
            PRow row = table.newRow(timestamp, key);
            if (rowEntry.getValue() == null) { // means delete
                row.delete();
            } else {
                for (Map.Entry<PColumn,byte[]> valueEntry : rowEntry.getValue().entrySet()) {
                    row.setValue(valueEntry.getKey(), valueEntry.getValue());
                }
            }
            mutations.addAll(row.toRowMutations());
        }
    }
    
    /**
     * Get the unsorted list of HBase mutations for the tables with uncommitted data.
     * @return list of HBase mutations for uncommitted data.
     */
    public List<Mutation> toMutations() {
        Long scn = connection.getSCN();
        long timestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        List<Mutation> mutations = Lists.newArrayListWithExpectedSize(this.numEntries);
        Iterator<Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>>> iterator = this.mutations.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> entry = iterator.next();
            PTable table = entry.getKey().getTable();
            List<Map.Entry<ImmutableBytesPtr,Map<PColumn,byte[]>>> rowMutations = new ArrayList<Map.Entry<ImmutableBytesPtr,Map<PColumn,byte[]>>>(entry.getValue().entrySet());
            addRowMutations(table, rowMutations.iterator(), timestamp, mutations);
        }
        return mutations;
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
                serverTimeStamp = client.updateCache(tableRef.getSchema().getName(), tableRef.getTable().getName().getString());
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
                    table = connection.getPMetaData().getSchema(tableRef.getSchema().getName()).getTable(tableRef.getTable().getName().getString());
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
    
    public void commit() throws SQLException {
        int i = 0;
        long[] serverTimeStamps = validate();
        Iterator<Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>>> iterator = this.mutations.entrySet().iterator();
        List<Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>>> committedList = Lists.newArrayListWithCapacity(this.mutations.size());
        while (iterator.hasNext()) {
            Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> entry = iterator.next();
            TableRef tableRef = entry.getKey();
            PTable table = tableRef.getTable();
            long serverTimestamp = serverTimeStamps[i++];
            List<Mutation> mutations = Lists.newArrayListWithExpectedSize(entry.getValue().size());
            addRowMutations(table, entry.getValue().entrySet().iterator(), serverTimestamp, mutations);
            SQLException sqlE = null;
            HTableInterface hTable = connection.getQueryServices().getTable(tableRef.getTableName());
            try {
                hTable.batch(mutations);
                committedList.add(entry);
                numEntries -= entry.getValue().size();
                iterator.remove(); // Remove batches as we process them
            } catch (Exception e) {
                // Throw to client with both what was committed so far and what is left to be committed.
                // That way, client can either undo what was done or try again with what was not done.
                sqlE = new CommitException(e, this, new MutationState(committedList, this.sizeOffset, this.maxSize, this.connection));
            } finally {
                try {
                    hTable.close();
                } catch (IOException e) {
                    if (sqlE != null) {
                        sqlE.setNextException(new PhoenixIOException(e));
                    } else {
                        sqlE = new PhoenixIOException(e);
                    }
                } finally {
                    if (sqlE != null) {
                        throw sqlE;
                    }
                }
            }
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
