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

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;

import com.salesforce.phoenix.compile.MutationPlan;
import com.salesforce.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import com.salesforce.phoenix.execute.MutationState;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.schema.*;


public class DelegateConnectionQueryServices extends DelegateQueryServices implements ConnectionQueryServices {

    public DelegateConnectionQueryServices(ConnectionQueryServices delegate) {
        super(delegate);
    }
    
    @Override
    protected ConnectionQueryServices getDelegate() {
        return (ConnectionQueryServices)super.getDelegate();
    }
    
    @Override
    public ConnectionQueryServices getChildQueryServices(ImmutableBytesWritable tenantId) {
        return getDelegate().getChildQueryServices(tenantId);
    }

    @Override
    public HTableInterface getTable(byte[] tableName) throws SQLException {
        return getDelegate().getTable(tableName);
    }

    @Override
    public StatsManager getStatsManager() {
        return getDelegate().getStatsManager();
    }

    @Override
    public NavigableMap<HRegionInfo, ServerName> getAllTableRegions(TableRef table) throws SQLException {
        return getDelegate().getAllTableRegions(table);
    }

    @Override
    public PMetaData addTable(String schemaName, PTable table) throws SQLException {
        return getDelegate().addTable(schemaName, table);
    }

    @Override
    public PMetaData addColumn(String schemaName, String tableName, List<PColumn> columns, long tableTimeStamp,
            long tableSeqNum, boolean isImmutableRows) throws SQLException {
        return getDelegate().addColumn(schemaName, tableName, columns, tableTimeStamp, tableSeqNum, isImmutableRows);
    }

    @Override
    public PMetaData removeTable(String schemaName, String tableName)
            throws SQLException {
        return getDelegate().removeTable(schemaName, tableName);
    }

    @Override
    public PMetaData removeColumn(String schemaName, String tableName, String familyName, String columnName,
            long tableTimeStamp, long tableSeqNum) throws SQLException {
        return getDelegate().removeColumn(schemaName, tableName, familyName, columnName, tableTimeStamp, tableSeqNum);
    }

    @Override
    public PhoenixConnection connect(String url, Properties info) throws SQLException {
        return getDelegate().connect(url, info);
    }

    @Override
    public MetaDataMutationResult getTable(byte[] schemaBytes, byte[] tableBytes, long tableTimestamp, long clientTimestamp) throws SQLException {
        return getDelegate().getTable(schemaBytes, tableBytes, tableTimestamp, clientTimestamp);
    }

    @Override
    public MetaDataMutationResult createTable(List<Mutation> tableMetaData, PTableType tableType,
            Map<String, Object> tableProps, List<Pair<byte[], Map<String, Object>>> families, byte[][] splits)
            throws SQLException {
        return getDelegate().createTable(tableMetaData, tableType, tableProps, families, splits);
    }

    @Override
    public MetaDataMutationResult dropTable(List<Mutation> tabeMetaData, PTableType tableType) throws SQLException {
        return getDelegate().dropTable(tabeMetaData, tableType);
    }

    @Override
    public MetaDataMutationResult addColumn(List<Mutation> tabeMetaData, boolean isView, Pair<byte[],Map<String,Object>> family) throws SQLException {
        return getDelegate().addColumn(tabeMetaData, isView, family);
    }

    @Override
    public MetaDataMutationResult dropColumn(List<Mutation> tabeMetaData, byte[] emptyCF) throws SQLException {
        return getDelegate().dropColumn(tabeMetaData, emptyCF);
    }

    @Override
    public MetaDataMutationResult updateIndexState(List<Mutation> tableMetadata, String parentTableName) throws SQLException {
        return getDelegate().updateIndexState(tableMetadata, parentTableName);
    }
    
    @Override
    public void init(String url, Properties props) throws SQLException {
        getDelegate().init(url, props);
    }

    @Override
    public MutationState updateData(MutationPlan plan) throws SQLException {
        return getDelegate().updateData(plan);
    }

    @Override
    public int getLowestClusterHBaseVersion() {
        return getDelegate().getLowestClusterHBaseVersion();
    }

    @Override
    public HBaseAdmin getAdmin() throws SQLException {
        return getDelegate().getAdmin();
    }

    @Override
    public HTableDescriptor getTableDescriptor(byte[] tableName) throws SQLException {
        return getDelegate().getTableDescriptor(tableName);
    }
}
