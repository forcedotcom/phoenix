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

import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.salesforce.phoenix.compile.MutationPlan;
import com.salesforce.phoenix.coprocessor.MetaDataProtocol;
import com.salesforce.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import com.salesforce.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import com.salesforce.phoenix.execute.MutationState;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PIndexState;
import com.salesforce.phoenix.schema.PMetaData;
import com.salesforce.phoenix.schema.PMetaDataImpl;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.PTableImpl;
import com.salesforce.phoenix.schema.PTableType;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.PhoenixRuntime;
import com.salesforce.phoenix.util.SchemaUtil;


/**
 *
 * Implementation of ConnectionQueryServices used in testing where no connection to
 * an hbase cluster is necessary.
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
        return new StatsManager() {

            @Override
            public byte[] getMinKey(TableRef table) {
                return HConstants.EMPTY_START_ROW;
            }

            @Override
            public byte[] getMaxKey(TableRef table) {
                return HConstants.EMPTY_END_ROW;
            }

            @Override
            public void updateStats(TableRef table) throws SQLException {
            }
        };
    }

    @Override
    public List<HRegionLocation> getAllTableRegions(byte[] tableName) throws SQLException {
        return Collections.singletonList(new HRegionLocation(new HRegionInfo(tableName, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW),"localhost",-1));
    }

    @Override
    public PMetaData addTable(PTable table) throws SQLException {
        return metaData = metaData.addTable(table);
    }

    @Override
    public PMetaData addColumn(String tableName, List<PColumn> columns, long tableTimeStamp, long tableSeqNum,
            boolean isImmutableRows) throws SQLException {
        return metaData = metaData.addColumn(tableName, columns, tableTimeStamp, tableSeqNum, isImmutableRows);
    }

    @Override
    public PMetaData removeTable(String tableName)
            throws SQLException {
        return metaData = metaData.removeTable(tableName);
    }

    @Override
    public PMetaData removeColumn(String tableName, String familyName, String columnName, long tableTimeStamp,
            long tableSeqNum) throws SQLException {
        return metaData = metaData.removeColumn(tableName, familyName, columnName, tableTimeStamp, tableSeqNum);
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
    public MetaDataMutationResult createTable(List<Mutation> tableMetaData, PTableType tableType, Map<String,Object> tableProps, List<Pair<byte[],Map<String,Object>>> families, byte[][] splits) throws SQLException {
        return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, 0, null);
    }

    @Override
    public MetaDataMutationResult dropTable(List<Mutation> tableMetadata, PTableType tableType) throws SQLException {
        return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, 0, null);
    }

    @Override
    public MetaDataMutationResult addColumn(List<Mutation> tableMetaData, PTableType readOnly, Pair<byte[],Map<String,Object>> family) throws SQLException {
        return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, 0, null);
    }

    @Override
    public MetaDataMutationResult dropColumn(List<Mutation> tableMetadata, PTableType tableType) throws SQLException {
        return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, 0, null);
    }

    @Override
    public void init(String url, Properties props) throws SQLException {
        props = new Properties(props);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP));
        PhoenixConnection metaConnection = new PhoenixConnection(this, url, props, PMetaDataImpl.EMPTY_META_DATA);
        SQLException sqlE = null;
        try {
            metaConnection.createStatement().executeUpdate(QueryConstants.CREATE_METADATA);
        } catch (SQLException e) {
            sqlE = e;
        } finally {
            try {
                metaConnection.close();
            } catch (SQLException e) {
                if (sqlE != null) {
                    sqlE.setNextException(e);
                } else {
                    sqlE = e;
                }
            }
            if (sqlE != null) {
                throw sqlE;
            }
        }
    }

    @Override
    public MutationState updateData(MutationPlan plan) throws SQLException {
        return new MutationState(0, plan.getConnection());
    }

    @Override
    public int getLowestClusterHBaseVersion() {
        return Integer.MAX_VALUE; // Allow everything for connectionless
    }

    @Override
    public HBaseAdmin getAdmin() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public MetaDataMutationResult updateIndexState(List<Mutation> tableMetadata, String parentTableName) throws SQLException {
        byte[][] rowKeyMetadata = new byte[2][];
        SchemaUtil.getVarChars(tableMetadata.get(0).getRow(), rowKeyMetadata);
        KeyValue newKV = tableMetadata.get(0).getFamilyMap().get(TABLE_FAMILY_BYTES).get(0);
        PIndexState newState =  PIndexState.fromSerializedValue(newKV.getBuffer()[newKV.getValueOffset()]);
        String schemaName = Bytes.toString(rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX]);
        String indexName = Bytes.toString(rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX]);
        String indexTableName = SchemaUtil.getTableName(schemaName, indexName);
        PTable index = metaData.getTable(indexTableName);
        index = PTableImpl.makePTable(index,newState == PIndexState.USABLE ? PIndexState.ACTIVE : newState == PIndexState.UNUSABLE ? PIndexState.INACTIVE : newState);
        return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, 0, index);
    }

    @Override
    public HTableDescriptor getTableDescriptor(byte[] tableName) throws SQLException {
        return null;
    }

    @Override
    public void clearTableRegionCache(byte[] tableName) throws SQLException {
    }

    @Override
    public boolean hasInvalidIndexConfiguration() {
        return false;
    }
}
