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
package com.salesforce.phoenix.query;

import java.sql.SQLException;
import java.util.*;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;

import com.salesforce.phoenix.compile.MutationPlan;
import com.salesforce.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import com.salesforce.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import com.salesforce.phoenix.execute.MutationState;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.util.SchemaUtil;


/**
 *
 * Implementation of ConnectionQueryServices used in testing where no connection to
 * an hbase cluster is necessary.
 * 
 * TODO: Move into main-phoenix and use this to enable a client to create metadata
 * on the fly (without needing a connection), given a DDL command. This will enable
 * Map/Reduce jobs that create HFiles to use the JDBC driver to insert data (by
 * going under the covers of the PhoenixConnection to get the mutation state that
 * would have been sent to the server on a commit)
 *
 * @author jtaylor
 * @since 0.1
 */
public class ConnectionlessQueryServicesImpl extends DelegateQueryServices implements ConnectionQueryServices  {
    private PMetaData metaData;

    public ConnectionlessQueryServicesImpl(QueryServices queryServices) {
        super(queryServices);
        metaData = PMetaDataImpl.EMPTY_META_DATA;
    }

    @Override
    public ConnectionQueryServices getChildQueryServices(ImmutableBytesWritable childId) {
        return this; // Just reuse the same query services
    }

    @Override
    public HTableInterface getTable(byte[] tableName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StatsManager getStatsManager() {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableMap<HRegionInfo, ServerName> getAllTableRegions(TableRef table) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PMetaData addTable(String schemaName, PTable table) throws SQLException {
        return metaData = metaData.addTable(schemaName, table);
    }

    @Override
    public PMetaData addColumn(String schemaName, String tableName, List<PColumn> columns, long tableSeqNum,
            long tableTimeStamp) throws SQLException {
        return metaData = metaData.addColumn(schemaName, tableName, columns, tableSeqNum, tableTimeStamp);
    }

    @Override
    public PMetaData removeTable(String schemaName, String tableName)
            throws SQLException {
        return metaData = metaData.removeTable(schemaName, tableName);
    }

    @Override
    public PMetaData removeColumn(String schemaName, String tableName, String familyName, String columnName,
            long tableSeqNum, long tableTimeStamp) throws SQLException {
        return metaData = metaData.removeColumn(schemaName, tableName, familyName, columnName, tableSeqNum, tableTimeStamp);
    }

    
    @Override
    public PhoenixConnection connect(String url, Properties info) throws SQLException {
        return new PhoenixConnection(this, url, info, metaData);
    }

    @Override
    public MetaDataMutationResult getTable(byte[] schemaBytes, byte[] tableBytes, long tableTimestamp, long clientTimestamp) throws SQLException {
        // Return result that will cause client to use it's own metadata instead of needing
        // to get anything from the server (since we don't have a connection)
        return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, 0, null);
    }

    @Override
    public MetaDataMutationResult createTable(List<Mutation> tableMetaData, boolean readOnly, Map<String,Object> tableProps, List<Pair<byte[],Map<String,Object>>> families, byte[][] splits) throws SQLException {
        return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, 0, null);
    }

    @Override
    public MetaDataMutationResult dropTable(List<Mutation> tableMetadata, boolean isView) throws SQLException {
        return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, 0, null);
    }

    @Override
    public MetaDataMutationResult addColumn(List<Mutation> tableMetaData, boolean readOnly, Pair<byte[],Map<String,Object>> family) throws SQLException {
        return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, 0, null);
    }

    @Override
    public MetaDataMutationResult dropColumn(List<Mutation> tableMetadata, byte[] emptyCF) throws SQLException {
        return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, 0, null);
    }

    @Override
    public void init(String url, Properties props) throws SQLException {
        SchemaUtil.initMetaData(this, url, new Properties());
    }

    @Override
    public MutationState updateData(MutationPlan plan) throws SQLException {
        return new MutationState(0, plan.getConnection());
    }

    @Override
    public int getLowestClusterHBaseVersion() {
        return 0;
    }

    @Override
    public HBaseAdmin getAdmin() throws SQLException {
        throw new UnsupportedOperationException();
    }
}
