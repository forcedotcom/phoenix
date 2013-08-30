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
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.*;
import com.salesforce.hbase.index.builder.covered.*;
import com.salesforce.hbase.index.builder.covered.scanner.Scanner;
import com.salesforce.phoenix.cache.GlobalCache;
import com.salesforce.phoenix.cache.TenantCache;
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

    Multimap<ColumnReference, IndexMaintainer> indexMap;

    @Override
    public void initialize(RegionCoprocessorEnvironment env) {
        // noop - all information necessary is in the given put/delete
    }

    @SuppressWarnings("unchecked")
    private Multimap<ColumnReference, IndexMaintainer> getIndexMap(TableState state) {
        if (indexMap != null) {
            return indexMap;
        }
        byte[] md;
        byte[] uuid = state.getUpdateAttributes().get(INDEX_UUID);
        if (uuid == null) {
            md = state.getUpdateAttributes().get(INDEX_MD);
            if (md == null) {
                indexMap = ImmutableListMultimap.of();
            } else {
                indexMap = IndexMaintainer.deserialize(md);
            }
        } else {
            byte[] tenantIdBytes = state.getUpdateAttributes().get(PhoenixRuntime.TENANT_ID_ATTRIB);
            ImmutableBytesWritable tenantId = tenantIdBytes == null ? null : new ImmutableBytesWritable(tenantIdBytes);
            TenantCache cache = GlobalCache.getTenantCache(state.getEnvironment().getConfiguration(), tenantId);
            indexMap = (Multimap<ColumnReference, IndexMaintainer>)cache.getServerCache(new ImmutableBytesPtr(uuid));
        }
        return indexMap;
    }

    private static Map<ColumnReference,byte[]> asMap(Scanner scanner, int expectedSize) throws IOException {
        KeyValue kv;
        Map<ColumnReference,byte[]> valueMap = Maps.newHashMapWithExpectedSize(expectedSize);
        while ((kv = scanner.next()) != null) {
            valueMap.put(new ColumnReference(kv.getFamily(),kv.getQualifier()), kv.getValue());
        }
        return valueMap;
    }
    
    @Override
    public Iterable<IndexUpdate> getIndexUpserts(TableState state) throws IOException {
        Multimap<ColumnReference, IndexMaintainer> indexMap = getIndexMap(state);
        if (indexMap.isEmpty()) {
            return Collections.emptyList();
        }
        List<IndexUpdate> indexUpdates = Lists.newArrayList();
        for (KeyValue kv : state.getPendingUpdate()) {
            // TODO: ColumnReference constructor with byte[],offset,length
            Collection<IndexMaintainer> maintainers = indexMap.get(new ColumnReference(kv.getFamily(),kv.getQualifier()));
            for (IndexMaintainer maintainer : maintainers) {
                // TODO: if more efficient, I could do this just once with all columns in all indexes
                Pair<Scanner,IndexUpdate> pair = state.getIndexedColumnsTableState(maintainer.getAllColumns());
                IndexUpdate indexUpdate = pair.getSecond();
                Scanner scanner = pair.getFirst();
                Map<ColumnReference,byte[]> valueMap = asMap(scanner, maintainer.getAllColumns().size());
                byte[] rowKey = maintainer.buildRowKey(valueMap);
                Put put = new Put(rowKey);
                indexUpdate.setTable(maintainer.getIndexTableName());
                indexUpdate.setUpdate(put);
                for (ColumnReference ref : maintainer.getCoverededColumns()) {
                    byte[] value = valueMap.get(ref);
                    if (value != null) { // FIXME: is this right?
                        put.add(ref.getFamily(), ref.getQualifier(), value);
                    }
                }
                // Add the empty key value
                put.add(maintainer.getEmptyKeyValueFamily(), QueryConstants.EMPTY_COLUMN_BYTES, ByteUtil.EMPTY_BYTE_ARRAY);
                indexUpdates.add(indexUpdate);
            }
        }
        return indexUpdates;
    }

    @Override
    public Iterable<Pair<Delete, byte[]>> getIndexDeletes(TableState state) throws IOException {
        Multimap<ColumnReference, IndexMaintainer> indexMap = getIndexMap(state);
        if (indexMap.isEmpty()) {
            return Collections.emptyList();
        }
        List<Pair<Delete, byte[]>> indexUpdates = Lists.newArrayList();
        for (KeyValue kv : state.getPendingUpdate()) {
            // TODO: ColumnReference constructor with byte[],offset,length
            Collection<IndexMaintainer> maintainers = indexMap.get(new ColumnReference(kv.getFamily(),kv.getQualifier()));
            for (IndexMaintainer maintainer : maintainers) {
                // TODO: if more efficient, I could do this just once with all columns in all indexes
                // FIXME: somewhat weird that you get back an IndexUpdate here
                Pair<Scanner,IndexUpdate> pair = state.getIndexedColumnsTableState(maintainer.getIndexedColumns());
                Scanner scanner = pair.getFirst();
                Map<ColumnReference,byte[]> valueMap = asMap(scanner, maintainer.getIndexedColumns().size());
                byte[] rowKey = maintainer.buildRowKey(valueMap);
                Delete delete = new Delete(rowKey);
                indexUpdates.add(new Pair<Delete, byte[]>(delete, maintainer.getIndexTableName()));
            }
        }
        return indexUpdates;
    }
    
}
