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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.Lists;
import com.salesforce.hbase.index.ValueGetter;
import com.salesforce.hbase.index.covered.IndexCodec;
import com.salesforce.hbase.index.covered.IndexUpdate;
import com.salesforce.hbase.index.covered.TableState;
import com.salesforce.hbase.index.scanner.Scanner;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.hbase.index.util.IndexManagementUtil;
import com.salesforce.phoenix.cache.GlobalCache;
import com.salesforce.phoenix.cache.IndexMetaDataCache;
import com.salesforce.phoenix.cache.TenantCache;
import com.salesforce.phoenix.util.PhoenixRuntime;
/**
 * Phoenix-basec {@link IndexCodec}. Manages all the logic of how to cleanup an index (
 * {@link #getIndexDeletes(TableState)}) as well as what the new index state should be (
 * {@link #getIndexUpserts(TableState)}).
 */
public class PhoenixIndexCodec implements IndexCodec {
    public static final String INDEX_MD = "IdxMD";
    public static final String INDEX_UUID = "IdxUUID";

    private List<IndexMaintainer> indexMaintainers;
    private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    private Configuration conf;
    private byte[] uuid;

    @Override
    public void initialize(RegionCoprocessorEnvironment env) {
      this.conf = env.getConfiguration();
    }

    List<IndexMaintainer> getIndexMaintainers() {
       return indexMaintainers;
    }
    
    /**
     * @param m mutation that is being processed
     * @return the {@link IndexMaintainer}s that would maintain the index for an update with the
     *         attributes.
     */
    private boolean initIndexMaintainers(Mutation m) {
        Map<String, byte[]> attributes = m.getAttributesMap();
        byte[] uuid = attributes.get(INDEX_UUID);
        if (uuid == null) {
            this.uuid = null;
            indexMaintainers = Collections.emptyList();
            return false;
        }
        if (this.uuid != null && Bytes.compareTo(this.uuid, uuid) == 0) {
            return true;
        }
        this.uuid = uuid;

        byte[] md = attributes.get(INDEX_MD);
        if (md != null) {
            indexMaintainers = IndexMaintainer.deserialize(md);
        } else {
            byte[] tenantIdBytes = attributes.get(PhoenixRuntime.TENANT_ID_ATTRIB);
            ImmutableBytesWritable tenantId =
                    tenantIdBytes == null ? null : new ImmutableBytesWritable(tenantIdBytes);
            TenantCache cache = GlobalCache.getTenantCache(conf, tenantId);
            IndexMetaDataCache indexCache =
                    (IndexMetaDataCache) cache.getServerCache(new ImmutableBytesPtr(uuid));
            this.indexMaintainers = indexCache.getIndexMaintainers();
        }
        return true;
    }
    
    @Override
    public Iterable<IndexUpdate> getIndexUpserts(TableState state) throws IOException {
        List<IndexMaintainer> indexMaintainers = getIndexMaintainers();
        if (indexMaintainers.isEmpty()) {
            return Collections.emptyList();
        }
        List<IndexUpdate> indexUpdates = Lists.newArrayList();
        // TODO: state.getCurrentRowKey() should take an ImmutableBytesWritable arg to prevent byte copy
        byte[] dataRowKey = state.getCurrentRowKey();
        for (IndexMaintainer maintainer : indexMaintainers) {
            // TODO: if more efficient, I could do this just once with all columns in all indexes
            Pair<Scanner,IndexUpdate> statePair = state.getIndexedColumnsTableState(maintainer.getAllColumns());
            IndexUpdate indexUpdate = statePair.getSecond();
            Scanner scanner = statePair.getFirst();
            ValueGetter valueGetter = IndexManagementUtil.createGetterFromScanner(scanner, dataRowKey);
            ptr.set(dataRowKey);
            // TODO: handle Pair<Put,Delete> because otherwise we'll bloat a sparse covered index
            Put put = maintainer.buildUpdateMutation(valueGetter, ptr);
            indexUpdate.setTable(maintainer.getIndexTableName());
            indexUpdate.setUpdate(put);
            //make sure we close the scanner when we are done
            scanner.close();
            indexUpdates.add(indexUpdate);
        }
        return indexUpdates;
    }

    @Override
    public Iterable<IndexUpdate> getIndexDeletes(TableState state) throws IOException {
        List<IndexMaintainer> indexMaintainers = getIndexMaintainers();
        if (indexMaintainers.isEmpty()) {
            return Collections.emptyList();
        }
        List<IndexUpdate> indexUpdates = Lists.newArrayList();
        // TODO: state.getCurrentRowKey() should take an ImmutableBytesWritable arg to prevent byte copy
        byte[] dataRowKey = state.getCurrentRowKey();
        for (IndexMaintainer maintainer : indexMaintainers) {
            // TODO: if more efficient, I could do this just once with all columns in all indexes
            Pair<Scanner,IndexUpdate> statePair = state.getIndexedColumnsTableState(maintainer.getAllColumns());
            Scanner scanner = statePair.getFirst();
            IndexUpdate indexUpdate = statePair.getSecond();
            indexUpdate.setTable(maintainer.getIndexTableName());
            ValueGetter valueGetter = IndexManagementUtil.createGetterFromScanner(scanner, dataRowKey);
            ptr.set(dataRowKey);
            Delete delete = maintainer.buildDeleteMutation(valueGetter, ptr, state.getPendingUpdate());
            scanner.close();
            indexUpdate.setUpdate(delete);
            indexUpdates.add(indexUpdate);
        }
        return indexUpdates;
    }
    
  @Override
  public boolean isEnabled(Mutation m) {
      return initIndexMaintainers(m);
  }
}