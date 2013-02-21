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

import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.*;
import static com.salesforce.phoenix.util.SchemaUtil.getVarChars;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.*;
import com.google.common.collect.*;
import com.salesforce.phoenix.compile.MutationPlan;
import com.salesforce.phoenix.coprocessor.*;
import com.salesforce.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import com.salesforce.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import com.salesforce.phoenix.exception.*;
import com.salesforce.phoenix.execute.MutationState;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData;
import com.salesforce.phoenix.join.HashJoiningRegionObserver;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.schema.TableNotFoundException;
import com.salesforce.phoenix.util.JDBCUtil;
import com.salesforce.phoenix.util.SchemaUtil;

public class ConnectionQueryServicesImpl extends DelegateQueryServices implements ConnectionQueryServices {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionQueryServicesImpl.class);
    private static final int INITIAL_CHILD_SERVICES_CAPACITY = 100;
    private static final int DEFAULT_OUT_OF_ORDER_MUTATIONS_WAIT_TIME_MS = 1000;
    private final Configuration config;
    private final HConnection connection;
    private final StatsManager statsManager;
    private final ConcurrentHashMap<ImmutableBytesWritable,ConnectionQueryServices> childServices;
    // Cache the latest meta data here for future connections
    private volatile PMetaData latestMetaData = PMetaDataImpl.EMPTY_META_DATA;
    private final Object latestMetaDataLock = new Object();

    /**
     * keep a cache of HRegionInfo objects
     */
    private final LoadingCache<TableRef, SortedSet<HRegionInfo>> tableRegionCache;
    
    public ConnectionQueryServicesImpl(QueryServices services, final Configuration config) throws SQLException {
        super(services);
        this.config = config;
        try {
            this.connection = HConnectionManager.createConnection(config);
        } catch (ZooKeeperConnectionException e) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION)
                .setRootCause(e).build().buildException();
        }
        if (this.connection.isClosed()) { // TODO: why the heck doesn't this throw above?
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION).build().buildException();
        }
        // TODO: should we track connection wide memory usage or just org-wide usage?
        // If connection-wide, create a MemoryManager here, otherwise just use the one from the delegate
        this.childServices = new ConcurrentHashMap<ImmutableBytesWritable,ConnectionQueryServices>(INITIAL_CHILD_SERVICES_CAPACITY);
        int statsUpdateFrequencyMs = this.getConfig().getInt(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, QueryServicesOptions.DEFAULT_STATS_UPDATE_FREQ_MS);
        int maxStatsAgeMs = this.getConfig().getInt(QueryServices.MAX_STATS_AGE_MS_ATTRIB, QueryServicesOptions.DEFAULT_MAX_STATS_AGE_MS);
        this.statsManager = new StatsManagerImpl(this, statsUpdateFrequencyMs, maxStatsAgeMs);
        /**
         * keep a cache of HRegionInfo objects
         */
        tableRegionCache = CacheBuilder.newBuilder().
            expireAfterAccess(this.getConfig().getLong(QueryServices.REGION_BOUNDARY_CACHE_TTL_MS_ATTRIB,QueryServicesOptions.DEFAULT_REGION_BOUNDARY_CACHE_TTL_MS), TimeUnit.MILLISECONDS)
            .removalListener(new RemovalListener<TableRef, SortedSet<HRegionInfo>>(){
                @Override
                public void onRemoval(RemovalNotification<TableRef, SortedSet<HRegionInfo>> notification) {
                    logger.info("REMOVE: {}", notification.getKey());
                }
            })
            .build(new CacheLoader<TableRef,SortedSet<HRegionInfo>>(){
                @Override
                public SortedSet<HRegionInfo> load(TableRef key) throws Exception {
                    logger.info("LOAD: {}", key);
                    return MetaScanner.allTableRegions(config, key.getTableName(), false).navigableKeySet();
                }
            });
    }

    @Override
    public StatsManager getStatsManager() {
        return this.statsManager;
    }
    
    @Override
    public HTableInterface getTable(byte[] tableName) throws SQLException {
        try {
            return HTableFactoryProvider.getHTableFactory().getTable(tableName, connection, getExecutor());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public Configuration getConfig() {
        return config;
    }

    /**
     * Closes the underlying connection to zookeeper. The QueryServices
     * may not be used after that point. When a Connection is closed,
     * this is not called, since these instances are pooled by the
     * Driver. Instead, the Driver should call this if the QueryServices
     * is ever removed from the pool
     */
    @Override
    public void close() throws SQLException {
        try {
            connection.close();
        } catch (IOException e) {
            throw new PhoenixIOException(e);
        }
        finally {
            super.close();
        }
    }    

    protected ConnectionQueryServices newChildQueryService() {
        return new ChildQueryServices(this);
    }

    /**
     * Get (and create if necessary) a child QueryService for a given tenantId.
     * The QueryService will be cached for the lifetime of the parent QueryService
     * @param tenantId the tenant ID
     * @return the child QueryService
     */
    @Override
    public ConnectionQueryServices getChildQueryServices(ImmutableBytesWritable tenantId) {
        ConnectionQueryServices childQueryService = childServices.get(tenantId);
        if (childQueryService == null) {
            childQueryService = newChildQueryService();
            ConnectionQueryServices prevQueryService = childServices.putIfAbsent(tenantId, childQueryService);
            return prevQueryService == null ? childQueryService : prevQueryService;
        }
        return childQueryService;
    }

    @Override
    public SortedSet<HRegionInfo> getAllTableRegions(TableRef table) throws SQLException {
        try {
            return tableRegionCache.get(table);
        } catch (ExecutionException e) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.GET_TABLE_REGIONS_FAIL)
                .setRootCause(e).build().buildException();
        }
    }

    @Override
    public PMetaData addTable(String schemaName, PTable table) throws SQLException {
        try {
            // If existing table isn't older than new table, don't replace
            // If a client opens a connection at an earlier timestamp, this can happen
            PTable existingTable = latestMetaData.getSchema(schemaName).getTable(table.getName().getString());
            if (existingTable.getTimeStamp() >= table.getTimeStamp()) {
                return latestMetaData;
            }
        } catch (TableNotFoundException e) {
        } catch (SchemaNotFoundException e) {
        }
        synchronized(latestMetaDataLock) {
            latestMetaData = latestMetaData.addTable(schemaName, table);
            latestMetaDataLock.notifyAll();
            return latestMetaData;
        }
    }

    private static interface Mutator {
        PMetaData mutate(PMetaData metaData) throws SQLException;
    }

    /**
     * Ensures that metaData mutations are handled in the correct order
     */
    private PMetaData metaDataMutated(String schemaName, String tableName, long tableSeqNum, Mutator mutator) throws SQLException {
        synchronized(latestMetaDataLock) {
            PMetaData metaData = latestMetaData;
            PTable table;
            long endTime = System.currentTimeMillis() + DEFAULT_OUT_OF_ORDER_MUTATIONS_WAIT_TIME_MS;
            while (true) {
                try {
                    try {
                        table = metaData.getSchema(schemaName).getTable(tableName);
                        /* If the table is at the prior sequence number, then we're good to go.
                         * We know if we've got this far, that the server validated the mutations,
                         * so we'd just need to wait until the other connection that mutated the same
                         * table is processed.
                         */
                        if (table.getSequenceNumber() + 1 == tableSeqNum) {
                            // TODO: assert that timeStamp is bigger that table timeStamp?
                            metaData = mutator.mutate(metaData);
                            break;
                        } else if (table.getSequenceNumber() >= tableSeqNum) {
                            logger.warn("Attempt to cache older version of " + schemaName + "." + tableName + ": current= " + table.getSequenceNumber() + ", new=" + tableSeqNum);
                            break;
                        }
                    } catch (SchemaNotFoundException e) {
                    } catch (TableNotFoundException e) {
                    }
                    long waitTime = endTime - System.currentTimeMillis();
                    // We waited long enough - just remove the table from the cache
                    // and the next time it's used it'll be pulled over from the server.
                    if (waitTime <= 0) {
                        logger.warn("Unable to update meta data repo within " + (DEFAULT_OUT_OF_ORDER_MUTATIONS_WAIT_TIME_MS/1000) + " seconds for " + schemaName + "." + tableName);
                        metaData = metaData.removeTable(schemaName, tableName);
                        break;
                    }
                    latestMetaDataLock.wait(waitTime);
                } catch (InterruptedException e) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.INTERRUPTED_EXCEPTION)
                        .setRootCause(e).build().buildException();
                }
            }
            latestMetaData = metaData;
            latestMetaDataLock.notifyAll();
            return metaData;
        }
     }

    @Override
    public PMetaData addColumn(final String schemaName, final String tableName, final List<PColumn> columns, final long tableSeqNum, final long tableTimeStamp) throws SQLException {
        return metaDataMutated(schemaName, tableName, tableSeqNum, new Mutator() {
            @Override
            public PMetaData mutate(PMetaData metaData) throws SQLException {
                try {
                    return metaData.addColumn(schemaName, tableName, columns, tableSeqNum, tableTimeStamp);
                } catch (TableNotFoundException e) {
                    // The DROP TABLE may have been processed first, so just ignore.
                    return metaData;
                }
            }
        });
     }

    @Override
    public PMetaData removeTable(final String schemaName, final String tableName) throws SQLException {
        synchronized(latestMetaDataLock) {
            latestMetaData = latestMetaData.removeTable(schemaName, tableName);
            latestMetaDataLock.notifyAll();
            return latestMetaData;
        }
    }

    @Override
    public PMetaData removeColumn(final String schemaName, final String tableName, final String familyName, final String columnName, final long tableSeqNum, final long tableTimeStamp) throws SQLException {
        return metaDataMutated(schemaName, tableName, tableSeqNum, new Mutator() {
            @Override
            public PMetaData mutate(PMetaData metaData) throws SQLException {
                try {
                    return metaData.removeColumn(schemaName, tableName, familyName, columnName, tableSeqNum, tableTimeStamp);
                } catch (TableNotFoundException e) {
                    // The DROP TABLE may have been processed first, so just ignore.
                    return metaData;
                }
            }
        });
    }

    private static boolean hasNewerTables(long scn, PMetaData metaData) {
        for (PSchema schema : metaData.getSchemas().values()) {
            for (PTable table : schema.getTables().values()) {
                if (table.getTimeStamp() >= scn) {
                    return true;
                }
            }
        }
        return false;
    }
    
    private static PMetaData pruneNewerTables(long scn, PMetaData metaData) {
        if (!hasNewerTables(scn, metaData)) {
            return metaData;
        }
        Map<String,PSchema> newSchemas = new HashMap<String,PSchema>(metaData.getSchemas());
        for (Map.Entry<String, PSchema> schemaEntry : metaData.getSchemas().entrySet()) {
            Map<String,PTable> newTables = new HashMap<String,PTable>(schemaEntry.getValue().getTables());
            Iterator<Map.Entry<String, PTable>> iterator = newTables.entrySet().iterator();
            boolean wasModified = false;
            while (iterator.hasNext()) {
                if (iterator.next().getValue().getTimeStamp() >= scn) {
                    iterator.remove();
                    wasModified = true;
                }
            }
            if (wasModified) {
                if (newTables.isEmpty()) {
                    newSchemas.remove(schemaEntry.getKey());
                } else {
                    PSchema newSchema = new PSchemaImpl(schemaEntry.getKey(),newTables);
                    newSchemas.put(schemaEntry.getKey(), newSchema);
                }
            }
        }
        return new PMetaDataImpl(newSchemas);
    }
    
    @Override
    public PhoenixConnection connect(String url, Properties info) throws SQLException {
        Long scn = JDBCUtil.getCurrentSCN(url, info);
        PMetaData metaData = scn == null ? latestMetaData : pruneNewerTables(scn, latestMetaData);
        return new PhoenixConnection(this, url, info, metaData);
    }


    private HColumnDescriptor generateColumnFamilyDescriptor(Pair<byte[],Map<String,Object>> family, boolean readOnly) throws SQLException {
        HColumnDescriptor columnDesc = new HColumnDescriptor(family.getFirst());
        if (!readOnly) {
            columnDesc.setKeepDeletedCells(true);
            columnDesc.setDataBlockEncoding(SchemaUtil.DEFAULT_DATA_BLOCK_ENCODING);
            for (Entry<String,Object> entry : family.getSecond().entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                columnDesc.setValue(key, value == null ? null : value.toString());
            }
        }
        return columnDesc;
    }

    private HTableDescriptor generateTableDescriptor(byte[] tableName, boolean readOnly, Map<String,Object> tableProps, List<Pair<byte[],Map<String,Object>>> families, byte[][] splits) throws SQLException {
        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        for (Entry<String,Object> entry : tableProps.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            descriptor.setValue(key, value == null ? null : value.toString());
        }
        if (families.isEmpty()) {
            if (!readOnly) {
                // Add dummy column family so we have key values for tables that 
                HColumnDescriptor columnDescriptor = generateColumnFamilyDescriptor(new Pair<byte[],Map<String,Object>>(QueryConstants.EMPTY_COLUMN_BYTES,Collections.<String,Object>emptyMap()), readOnly);
                descriptor.addFamily(columnDescriptor);
            }
        } else {
            for (Pair<byte[],Map<String,Object>> family : families) {
                HColumnDescriptor columnDescriptor = generateColumnFamilyDescriptor(family, readOnly);
                descriptor.addFamily(columnDescriptor);
            }
        }
        // The phoenix jar must be available on HBase classpath
        try {
            descriptor.addCoprocessor(ScanRegionObserver.class.getName(), null, 1, null);
            descriptor.addCoprocessor(UngroupedAggregateRegionObserver.class.getName(), null, 1, null);
            descriptor.addCoprocessor(GroupedAggregateRegionObserver.class.getName(), null, 1, null);
            descriptor.addCoprocessor(HashJoiningRegionObserver.class.getName(), null, 1, null);
            // Setup split policy on Phoenix metadata table to ensure that the key values of a Phoenix table
            // stay on the same region.
            if (SchemaUtil.isMetaTable(tableName)) {
                descriptor.addCoprocessor(MetaDataEndpointImpl.class.getName(), null, 1, null);
            }
        } catch (IOException e) {
            throw new PhoenixIOException(e);
        }
        return descriptor;
    }

    private void ensureFamilyCreated(byte[] tableName, boolean readOnly, Pair<byte[],Map<String,Object>> family) throws SQLException {
        HBaseAdmin admin = null;
        SQLException sqlE = null;
        try {
            admin = new HBaseAdmin(this.getConfig());
            try {
                HTableDescriptor existingDesc = admin.getTableDescriptor(tableName);
                HColumnDescriptor oldDescriptor = existingDesc.getFamily(family.getFirst());
                HColumnDescriptor columnDescriptor = generateColumnFamilyDescriptor(family, readOnly);
                if (columnDescriptor.equals(oldDescriptor)) {
                    // Table already has family and it's the same.
                    return;
                }
                admin.disableTable(tableName);
                if (oldDescriptor == null) {
                    if (readOnly) {
                        throw new ReadOnlyTableException("The HBase column families for a read-only table must already exist(" + Bytes.toStringBinary(columnDescriptor.getName()) + ")");
                    }
                    admin.addColumn(tableName, columnDescriptor);
                } else {
                    admin.modifyColumn(tableName, columnDescriptor);
                }
                admin.enableTable(tableName);
            } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
                sqlE = new SQLExceptionInfo.Builder(SQLExceptionCode.TABLE_UNDEFINED).setRootCause(e).build().buildException();
            }
        } catch (IOException e) {
            sqlE = new PhoenixIOException(e);
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
            } catch (IOException e) {
                if (sqlE == null) {
                    sqlE = new PhoenixIOException(e);
                } else {
                    sqlE.setNextException(new PhoenixIOException(e));
                }
            } finally {
                if (sqlE != null) {
                    throw sqlE;
                }
            }
        }
    }
    
    /**
     * 
     * @param tableName
     * @param familyNames
     * @param splits
     * @return true if table was created and false if it already exists
     * @throws SQLException
     */
    private boolean ensureTableCreated(byte[] tableName, boolean readOnly, Map<String,Object> props, List<Pair<byte[],Map<String,Object>>> families, byte[][] splits) throws SQLException {
        HBaseAdmin admin = null;
        SQLException sqlE = null;
        Set<byte[]> familyNames = null;
        boolean isMetaTable = SchemaUtil.isMetaTable(tableName);
        try {
            admin = new HBaseAdmin(this.getConfig());
            HTableDescriptor newDesc = generateTableDescriptor(tableName, readOnly, props, families, splits);
            try {
                HTableDescriptor existingDesc = admin.getTableDescriptor(tableName);
                if (existingDesc.equals(newDesc)) {
                    // Table is already created. Note that the presplits are ignored in this case
                    if (isMetaTable) {
                        checkClientServerCompatibility();
                    }
                    return false;
                }
                // Add column families from existing table that aren't in phoenix description
                for (HColumnDescriptor cd : existingDesc.getColumnFamilies()) {
                    if (newDesc.getFamily(cd.getName()) == null) {
                        if (readOnly) {
                            if (familyNames == null) {
                                familyNames = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
                                for (Pair<byte[],Map<String,Object>> family : families) {
                                    familyNames.add(family.getFirst());
                                }
                            }
                            if (!familyNames.contains(cd.getName())) {
                                throw new ReadOnlyTableException("The HBase column families for a VIEW must already exist(" + Bytes.toStringBinary(cd.getName()) + ")");
                            }
                        }
                        newDesc.addFamily(cd);
                    }
                }
                boolean removePhoenixJarPath = false;
                if (isMetaTable) {
                    /*
                     *  FIXME: remove this once everyone has been upgraded to v 0.94.4+
                     *  This hack checks to see if we've got "phoenix.jar" specified as
                     *  the jar file path. We need to set this to null in this case due
                     *  to a change in behavior of HBase.
                     */
                    String value = existingDesc.getValue("coprocessor$1");
                    removePhoenixJarPath = (value != null && value.startsWith("phoenix.jar"));
                    if (!removePhoenixJarPath) {
                        checkClientServerCompatibility();
                    }
                }
                // Update metadata of table
                // TODO: Take advantage of online schema change ability by setting "hbase.online.schema.update.enable" to true
                admin.disableTable(tableName);
                // TODO: What if not all existing column families are present?
                admin.modifyTable(tableName, newDesc);
                admin.enableTable(tableName);
                /*
                 *  FIXME: remove this once everyone has been upgraded to v 0.94.4+
                 * We've detected that the SYSTEM.TABLE needs to be upgraded, so let's
                 * query and update all tables here.
                 */
                if (removePhoenixJarPath) {
                    upgradeTablesFrom0_94_2to0_94_4(admin);
                    // Do the compatibility check here, now that the jar path has been corrected.
                    // This will work with the new and the old jar, so do the compatibility check now.
                    checkClientServerCompatibility();
                }
                return false;
            } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
            }
            if (readOnly) {
                throw new ReadOnlyTableException("An HBase table for a VIEW must already exist(" + SchemaUtil.getTableDisplayName(tableName) + ")");
            }
            
            /*
             * Remove the splitPolicy attribute due to an HBase bug (see below)
             */
            if (isMetaTable) {
                newDesc.remove(HTableDescriptor.SPLIT_POLICY);
            }
            if (splits == null) {
                admin.createTable(newDesc);
            } else {
                admin.createTable(newDesc, splits);
            }
            if (isMetaTable) {
                checkClientServerCompatibility();
                /*
                 * Now we modify the table to add the split policy, since we know that the client and
                 * server and compatible. This works around a nasty, known HBase bug where if a split
                 * policy class cannot be found on the server, the HBase table is left in a horrible
                 * "ghost" state where it can't be used and can't be deleted without bouncing the master. 
                 */
                newDesc.setValue(HTableDescriptor.SPLIT_POLICY, MetaDataSplitPolicy.class.getName());
                admin.disableTable(tableName);
                admin.modifyTable(tableName, newDesc);
                admin.enableTable(tableName);
            }
            return true;
        } catch (IOException e) {
            sqlE = new PhoenixIOException(e);
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
            } catch (IOException e) {
                if (sqlE == null) {
                    sqlE = new PhoenixIOException(e);
                } else {
                    sqlE.setNextException(new PhoenixIOException(e));
                }
            } finally {
                if (sqlE != null) {
                    throw sqlE;
                }
            }
        }
        return true; // will never make it here
    }

    /**
     * FIXME: Temporary code to convert tables to 0.94.4 format (i.e. no jar specified
     * in coprocessor definition). This is necessary because of a change in
     * HBase behavior between 0.94.3 and 0.94.4. Once everyone has been upgraded
     * this code can be removed.
     * @throws SQLException
     */
    private void upgradeTablesFrom0_94_2to0_94_4(HBaseAdmin admin) throws IOException {
        if (logger.isInfoEnabled()) {
            logger.info("Upgrading tables from HBase 0.94.2 to 0.94.4+");
        }
        /* Use regular HBase scan instead of query because the jar on the server may
         * not be compatible (we don't know yet) and this is our one chance to do
         * the conversion automatically.
         */
        Scan scan = new Scan();
        scan.addColumn(TABLE_FAMILY_BYTES, COLUMN_COUNT_BYTES);
        // Add filter so that we only get the table row and not the column rows
        scan.setFilter(new SingleColumnValueFilter(TABLE_FAMILY_BYTES, COLUMN_COUNT_BYTES, CompareOp.GREATER_OR_EQUAL, PDataType.INTEGER.toBytes(0)));
        HTableInterface table = HTableFactoryProvider.getHTableFactory().getTable(TYPE_TABLE_NAME, connection, getExecutor());
        ResultScanner scanner = table.getScanner(scan);
        Result result = null;
        while ((result = scanner.next()) != null) {
            byte[] rowKey = result.getRow();
            byte[][] rowKeyMetaData = new byte[2][];
            getVarChars(rowKey, rowKeyMetaData);
            byte[] schemaBytes = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
            byte[] tableBytes = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
            byte[] tableName = SchemaUtil.getTableName(schemaBytes, tableBytes);
            if (!SchemaUtil.isMetaTable(tableName)) {
                try {
                    HTableDescriptor existingDesc = admin.getTableDescriptor(tableName);
                    existingDesc.removeCoprocessor(ScanRegionObserver.class.getName());
                    existingDesc.removeCoprocessor(UngroupedAggregateRegionObserver.class.getName());
                    existingDesc.removeCoprocessor(GroupedAggregateRegionObserver.class.getName());
                    existingDesc.removeCoprocessor(HashJoiningRegionObserver.class.getName());
                    existingDesc.addCoprocessor(ScanRegionObserver.class.getName(), null, 1, null);
                    existingDesc.addCoprocessor(UngroupedAggregateRegionObserver.class.getName(), null, 1, null);
                    existingDesc.addCoprocessor(GroupedAggregateRegionObserver.class.getName(), null, 1, null);
                    existingDesc.addCoprocessor(HashJoiningRegionObserver.class.getName(), null, 1, null);
                    boolean wasEnabled = admin.isTableEnabled(tableName);
                    if (wasEnabled) {
                        admin.disableTable(tableName);
                    }
                    admin.modifyTable(tableName, existingDesc);
                    if (wasEnabled) {
                        admin.enableTable(tableName);
                    }
                } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
                    logger.error("Unable to convert " + Bytes.toString(tableName), e);
                } catch (IOException e) {
                    logger.error("Unable to convert " + Bytes.toString(tableName), e);
                }
            }
        }
    }

    private boolean isCompatible(Long serverVersion) {
        return serverVersion != null && MetaDataProtocol.VERSION == serverVersion.longValue();
    }
    
    private void checkClientServerCompatibility() throws SQLException {
        StringBuilder buf = new StringBuilder("The following servers require an updated " + QueryConstants.DEFAULT_COPROCESS_PATH + " to be put in the classpath of HBase: ");
        boolean isIncompatible = false;
        try {
            NavigableMap<HRegionInfo, ServerName> regionInfoMap = MetaScanner.allTableRegions(getConfig(), TYPE_TABLE_NAME, false);
            TreeMap<byte[], ServerName> regionMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
            List<byte[]> regionKeys = Lists.newArrayListWithExpectedSize(regionMap.size());
            for (Map.Entry<HRegionInfo, ServerName> entry : regionInfoMap.entrySet()) {
                regionKeys.add(entry.getKey().getStartKey());
                regionMap.put(entry.getKey().getStartKey(), entry.getValue());
            }
            final Map<byte[],Long> results = Maps.newHashMapWithExpectedSize(regionMap.size());
            connection.processExecs(MetaDataProtocol.class, regionKeys,
                    PhoenixDatabaseMetaData.TYPE_TABLE_NAME, this.getDelegate().getExecutor(), new Batch.Call<MetaDataProtocol,Long>() {
                        @Override
                        public Long call(MetaDataProtocol instance) throws IOException {
                            return instance.getVersion();
                        }
                    }, 
                    new Batch.Callback<Long>(){
                        @Override
                        public void update(byte[] region, byte[] row, Long value) {
                          results.put(region, value);
                        }
                    });
            for (Map.Entry<byte[],Long> result : results.entrySet()) {
                // This is the "phoenix.jar" is in-place, but server is out-of-sync with client case.
                if (!isCompatible(result.getValue())) {
                    isIncompatible = true;
                    ServerName name = regionMap.get(result.getKey());
                    buf.append(name);
                    buf.append(',');
                }
            }
        } catch (Throwable t) {
            // This is the case if the "phoenix.jar" is not on the classpath of HBase on the region server
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INCOMPATIBLE_CLIENT_SERVER_JAR).setRootCause(t)
                .setMessage("Ensure that " + QueryConstants.DEFAULT_COPROCESS_PATH + " is put on the classpath of HBase in every region server: " + t.getMessage())
                .build().buildException();
        }
        if (isIncompatible) {
            buf.setLength(buf.length()-1);
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.OUTDATED_JARS).setMessage(buf.toString()).build().buildException();
        }
    }

    /**
     * Invoke meta data coprocessor with one retry if the key was found to not be in the regions
     * (due to a table split)
     */
    private MetaDataMutationResult metaDataCoprocessorExec(byte[] tableKey, Batch.Call<MetaDataProtocol, MetaDataMutationResult> callable) throws SQLException {
        try {
            boolean retried = false;
            while (true) {
                HRegionLocation regionLocation = retried ? connection.relocateRegion(PhoenixDatabaseMetaData.TYPE_TABLE_NAME, tableKey) : connection.locateRegion(PhoenixDatabaseMetaData.TYPE_TABLE_NAME, tableKey);
                List<byte[]> regionKeys = Collections.singletonList(regionLocation.getRegionInfo().getStartKey());
                final Map<byte[],MetaDataMutationResult> results = Maps.newHashMapWithExpectedSize(1);
                connection.processExecs(MetaDataProtocol.class, regionKeys,
                        PhoenixDatabaseMetaData.TYPE_TABLE_NAME, this.getDelegate().getExecutor(), callable, 
                        new Batch.Callback<MetaDataMutationResult>(){
                            @Override
                            public void update(byte[] region, byte[] row, MetaDataMutationResult value) {
                              results.put(region, value);
                            }
                        });
                assert(results.size() == 1);
                MetaDataMutationResult result = results.values().iterator().next();
                if (result.getMutationCode() == MutationCode.TABLE_NOT_IN_REGION) {
                    if (retried) return result;
                    retried = true;
                    continue;
                }
                return result;
            }
        } catch (IOException e) {
            throw new PhoenixIOException(e);
        } catch (Throwable t) {
            throw new SQLException(t);
        }
    }
    
    @Override
    public MetaDataMutationResult createTable(final List<Mutation> tableMetaData, boolean isView, Map<String,Object> tableProps, final List<Pair<byte[],Map<String,Object>>> families, byte[][] splits) throws SQLException {
        byte[][] rowKeyMetadata = new byte[2][];
        Mutation m = tableMetaData.get(0);
        byte[] key = m.getRow();
        SchemaUtil.getVarChars(key, rowKeyMetadata);
        byte[] schemaBytes = rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        byte[] tableName = SchemaUtil.getTableName(schemaBytes, tableBytes);
        ensureTableCreated(tableName, isView, tableProps, families, splits);

        byte[] tableKey = SchemaUtil.getTableKey(schemaBytes, tableBytes);
        MetaDataMutationResult result = metaDataCoprocessorExec(tableKey,
            new Batch.Call<MetaDataProtocol, MetaDataMutationResult>() {
                @Override
                public MetaDataMutationResult call(MetaDataProtocol instance) throws IOException {
                  return instance.createTable(tableMetaData);
                }
            });
        return result;
    }

    @Override
    public MetaDataMutationResult getTable(final byte[] schemaBytes, final byte[] tableBytes, final long tableTimestamp, final long clientTimestamp) throws SQLException {
        byte[] tableKey = SchemaUtil.getTableKey(schemaBytes, tableBytes);
        return metaDataCoprocessorExec(tableKey,
                new Batch.Call<MetaDataProtocol, MetaDataMutationResult>() {
                    @Override
                    public MetaDataMutationResult call(MetaDataProtocol instance) throws IOException {
                      return instance.getTable(schemaBytes, tableBytes, tableTimestamp, clientTimestamp);
                    }
                });
    }

    @Override
    public MetaDataMutationResult dropTable(final List<Mutation> tableMetaData, final boolean isView) throws SQLException {
        byte[][] rowKeyMetadata = new byte[2][];
        SchemaUtil.getVarChars(tableMetaData.get(0).getRow(), rowKeyMetadata);
        byte[] tableKey = SchemaUtil.getTableKey(rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX], rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX]);
        return metaDataCoprocessorExec(tableKey,
                new Batch.Call<MetaDataProtocol, MetaDataMutationResult>() {
                    @Override
                    public MetaDataMutationResult call(MetaDataProtocol instance) throws IOException {
                      return instance.dropTable(tableMetaData, isView);
                    }
                });
    }

    @Override
    public MetaDataMutationResult addColumn(final List<Mutation> tableMetaData, boolean readOnly, Pair<byte[],Map<String,Object>> family) throws SQLException {
        byte[][] rowKeyMetaData = new byte[2][];
        byte[] rowKey = tableMetaData.get(0).getRow();
        SchemaUtil.getVarChars(rowKey, rowKeyMetaData);
        byte[] schemaBytes = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableBytes = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        byte[] tableKey = SchemaUtil.getTableKey(schemaBytes, tableBytes);
        byte[] tableName = SchemaUtil.getTableName(schemaBytes, tableBytes);
        if (family != null) {
            ensureFamilyCreated(tableName, readOnly, family);
        }
        MetaDataMutationResult result =  metaDataCoprocessorExec(tableKey,
            new Batch.Call<MetaDataProtocol, MetaDataMutationResult>() {
                @Override
                public MetaDataMutationResult call(MetaDataProtocol instance) throws IOException {
                    return instance.addColumn(tableMetaData);
                }
            });
        return result;
    }

    @Override
    public MetaDataMutationResult dropColumn(final List<Mutation> tableMetaData, byte[] emptyCF) throws SQLException {
        byte[][] rowKeyMetadata = new byte[2][];
        SchemaUtil.getVarChars(tableMetaData.get(0).getRow(), rowKeyMetadata);
        byte[] schemaBytes = rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        byte[] tableName = SchemaUtil.getTableName(schemaBytes, tableBytes);
        byte[] tableKey = SchemaUtil.getTableKey(schemaBytes, tableBytes);
        if (emptyCF != null) {
            this.ensureFamilyCreated(tableName, false, new Pair<byte[],Map<String,Object>>(emptyCF,Collections.<String,Object>emptyMap()));
        }
        MetaDataMutationResult result = metaDataCoprocessorExec(tableKey,
            new Batch.Call<MetaDataProtocol, MetaDataMutationResult>() {
                @Override
                public MetaDataMutationResult call(MetaDataProtocol instance) throws IOException {
                    return instance.dropColumn(tableMetaData);
                }
            });
        return result;
    }

    @Override
    public void init(String url, Properties props) throws SQLException {
        SchemaUtil.initMetaData(this, url, props);
    }

    @Override
    public MutationState updateData(MutationPlan plan) throws SQLException { 
        return plan.execute();
    }

}
