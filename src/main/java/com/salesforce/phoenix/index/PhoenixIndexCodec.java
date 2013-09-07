/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc. All rights reserved. Redistribution and use in source and binary forms, with
 * or without modification, are permitted provided that the following conditions are met: Redistributions of source code
 * must retain the above copyright notice, this list of conditions and the following disclaimer. Redistributions in
 * binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution. Neither the name of Salesforce.com nor the names
 * of its contributors may be used to endorse or promote products derived from this software without specific prior
 * written permission. THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.phoenix.index;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.hbase.index.builder.covered.*;
import com.salesforce.hbase.index.builder.covered.scanner.Scanner;
import com.salesforce.phoenix.cache.*;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.util.*;
/**
 * Phoenix-basec {@link IndexCodec}. Manages all the logic of how to cleanup an index (
 * {@link #getIndexDeletes(TableState)}) as well as what the new index state should be (
 * {@link #getIndexUpserts(TableState)}).
 */
public class PhoenixIndexCodec implements IndexCodec {
    public static final String INDEX_MD = "IdxMD";
    public static final String INDEX_UUID = "IdxUUID";

    private byte[] schemaName;
    private List<Pair<byte[],IndexMaintainer>> indexMaintainers;
    private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();

    @Override
    public void initialize(RegionCoprocessorEnvironment env) {
        String tableName = env.getRegion().getTableDesc().getNameAsString();
        String schemaName = SchemaUtil.getSchemaNameFromFullName(tableName);
        this.schemaName = Bytes.toBytes(schemaName);
    }

    private List<Pair<byte[],IndexMaintainer>> getIndexMaintainers(TableState state) {
        if (indexMaintainers != null) {
            return indexMaintainers;
        }
        byte[] md;
        byte[] uuid = state.getUpdateAttributes().get(INDEX_UUID);
        if (uuid == null) {
            md = state.getUpdateAttributes().get(INDEX_MD);
            if (md == null) {
                indexMaintainers = Collections.emptyList();
            } else {
                List<IndexMaintainer> indexMaintainers = IndexMaintainer.deserialize(md);
                this.indexMaintainers = Lists.newArrayListWithExpectedSize(indexMaintainers.size());
                for (IndexMaintainer indexMaintainer : indexMaintainers) {
                    this.indexMaintainers.add(new Pair<byte[],IndexMaintainer>(
                            SchemaUtil.getTableName(this.schemaName,
                            indexMaintainer.getIndexTableName()), indexMaintainer));
                }
                indexMaintainers = IndexMaintainer.deserialize(md);
            }
        } else {
            byte[] tenantIdBytes = state.getUpdateAttributes().get(PhoenixRuntime.TENANT_ID_ATTRIB);
            ImmutableBytesWritable tenantId = tenantIdBytes == null ? null : new ImmutableBytesWritable(tenantIdBytes);
            TenantCache cache = GlobalCache.getTenantCache(state.getEnvironment().getConfiguration(), tenantId);
            IndexMetaDataCache indexCache = (IndexMetaDataCache)cache.getServerCache(new ImmutableBytesPtr(uuid));
            this.indexMaintainers = Lists.newArrayListWithExpectedSize(indexCache.getIndexMaintainers().size());
            for (IndexMaintainer indexMaintainer : indexCache.getIndexMaintainers()) {
                this.indexMaintainers.add(new Pair<byte[],IndexMaintainer>(
                        SchemaUtil.getTableName(this.schemaName,
                        indexMaintainer.getIndexTableName()), indexMaintainer));
            }
        }
        return indexMaintainers;
    }

    /* 
     * Looks up the current value based on a ColumnReference. Used for both mutable secondary indexes
     * as well as immutable secondary indexes (where an existing map already backs the retrieval of a
     * value given a PColumn).
     * TODO: For Jesse to move to one of his indexing packages
     */
    public static interface ValueGetter {
        byte[] getValue(ColumnReference ref);
    }
    
    private static ValueGetter newValueGetter(Scanner scanner, int expectedSize) throws IOException {
        KeyValue kv;
        final Map<ColumnReference,byte[]> valueMap = Maps.newHashMapWithExpectedSize(expectedSize);
        while ((kv = scanner.next()) != null) {
            valueMap.put(new ColumnReference(kv.getFamily(),kv.getQualifier()), kv.getValue());
        }
        return new ValueGetter() {

            @Override
            public byte[] getValue(ColumnReference ref) {
                return valueMap.get(ref);
            }
            
        };
    }
    
    @SuppressWarnings("deprecation")
    @Override
    public Iterable<IndexUpdate> getIndexUpserts(TableState state) throws IOException {
        List<Pair<byte[],IndexMaintainer>> indexMaintainers = getIndexMaintainers(state);
        if (indexMaintainers.isEmpty()) {
            return Collections.emptyList();
        }
        List<IndexUpdate> indexUpdates = Lists.newArrayList();
        // TODO: state.getCurrentRowKey() should take an ImmutableBytesWritable arg to prevent byte copy
        byte[] dataRowKey = state.getCurrentRowKey();
        for (Pair<byte[],IndexMaintainer> indexPair : indexMaintainers) {
            byte[] tableName = indexPair.getFirst();
            IndexMaintainer maintainer = indexPair.getSecond();
            // TODO: if more efficient, I could do this just once with all columns in all indexes
            Pair<Scanner,IndexUpdate> statePair = state.getIndexedColumnsTableState(maintainer.getAllColumns());
            IndexUpdate indexUpdate = statePair.getSecond();
            Scanner scanner = statePair.getFirst();
            ValueGetter valueGetter = newValueGetter(scanner, maintainer.getAllColumns().size());
            ptr.set(dataRowKey);
            byte[] rowKey = maintainer.buildRowKey(valueGetter, ptr);
            Put put = new Put(rowKey);
            put.setWriteToWAL(false);
            indexUpdate.setTable(tableName);
            indexUpdate.setUpdate(put);
            for (ColumnReference ref : maintainer.getCoverededColumns()) {
                byte[] value = valueGetter.getValue(ref);
                if (value != null) { // FIXME: is this right?
                    put.add(ref.getFamily(), ref.getQualifier(), value);
                }
            }
            // Add the empty key value
            put.add(maintainer.getEmptyKeyValueFamily(), QueryConstants.EMPTY_COLUMN_BYTES, ByteUtil.EMPTY_BYTE_ARRAY);
            indexUpdates.add(indexUpdate);
        }
        return indexUpdates;
    }

    @SuppressWarnings("deprecation")
    @Override
    public Iterable<Pair<Delete, byte[]>> getIndexDeletes(TableState state) throws IOException {
        List<Pair<byte[],IndexMaintainer>> indexMaintainers = getIndexMaintainers(state);
        if (indexMaintainers.isEmpty()) {
            return Collections.emptyList();
        }
        List<Pair<Delete, byte[]>> indexUpdates = Lists.newArrayList();
        // TODO: state.getCurrentRowKey() should take an ImmutableBytesWritable arg to prevent byte copy
        byte[] dataRowKey = state.getCurrentRowKey();
        for (Pair<byte[],IndexMaintainer> indexPair : indexMaintainers) {
            byte[] tableName = indexPair.getFirst();
            IndexMaintainer maintainer = indexPair.getSecond();
            // TODO: if more efficient, I could do this just once with all columns in all indexes
            Pair<Scanner,IndexUpdate> statePair = state.getIndexedColumnsTableState(maintainer.getIndexedColumns());
            Scanner scanner = statePair.getFirst();
            ValueGetter valueGetter = newValueGetter(scanner, maintainer.getAllColumns().size());
            ptr.set(dataRowKey);
            byte[] rowKey = maintainer.buildRowKey(valueGetter, ptr);
            Delete delete = new Delete(rowKey);
            delete.setWriteToWAL(false);
            indexUpdates.add(new Pair<Delete, byte[]>(delete, tableName));
        }
        return indexUpdates;
    }
    
}
