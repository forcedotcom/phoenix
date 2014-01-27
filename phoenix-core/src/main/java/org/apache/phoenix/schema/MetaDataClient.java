/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.schema;

import static com.google.common.collect.Lists.newArrayListWithExpectedSize;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static com.google.common.collect.Sets.newLinkedHashSetWithExpectedSize;
import static org.apache.phoenix.exception.SQLExceptionCode.INSUFFICIENT_MULTI_TENANT_COLUMNS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ARRAY_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_MODIFIER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DECIMAL_DIGITS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DISABLE_WAL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_ROWS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_STATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MULTI_TENANT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SALT_BUCKETS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_CAT_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SEQ_NUM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_STATEMENT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_TYPE;
import static org.apache.phoenix.query.QueryServices.DROP_METADATA_ATTRIB;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_DROP_METADATA;
import static org.apache.phoenix.schema.PDataType.VARCHAR;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.PostDDLCompiler;
import org.apache.phoenix.compile.PostIndexDDLCompiler;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.parse.AddColumnStatement;
import org.apache.phoenix.parse.AlterIndexStatement;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnName;
import org.apache.phoenix.parse.CreateIndexStatement;
import org.apache.phoenix.parse.CreateSequenceStatement;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.DropColumnStatement;
import org.apache.phoenix.parse.DropIndexStatement;
import org.apache.phoenix.parse.DropSequenceStatement;
import org.apache.phoenix.parse.DropTableStatement;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.PrimaryKeyConstraint;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable.LinkType;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;

public class MetaDataClient {
    private static final Logger logger = LoggerFactory.getLogger(MetaDataClient.class);

    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();
    private static final String CREATE_TABLE =
            "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"( " +
            TENANT_ID + "," +
            TABLE_SCHEM_NAME + "," +
            TABLE_NAME_NAME + "," +
            TABLE_TYPE_NAME + "," +
            TABLE_SEQ_NUM + "," +
            COLUMN_COUNT + "," +
            SALT_BUCKETS + "," +
            PK_NAME + "," +
            DATA_TABLE_NAME + "," +
            INDEX_STATE + "," +
            IMMUTABLE_ROWS + "," +
            DEFAULT_COLUMN_FAMILY_NAME + "," +
            VIEW_STATEMENT + "," +
            DISABLE_WAL + "," +
            MULTI_TENANT + "," +
            VIEW_TYPE +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String CREATE_LINK =
            "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"( " +
            TENANT_ID + "," +
            TABLE_SCHEM_NAME + "," +
            TABLE_NAME_NAME + "," +
            TABLE_CAT_NAME + "," +
            LINK_TYPE +
            ") VALUES (?, ?, ?, ?, ?)";
    private static final String INCREMENT_SEQ_NUM =
            "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"( " + 
            TENANT_ID + "," +
            TABLE_SCHEM_NAME + "," +
            TABLE_NAME_NAME + "," +
            TABLE_SEQ_NUM  +
            ") VALUES (?, ?, ?, ?)";
    private static final String MUTATE_TABLE =
        "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"( " + 
        TENANT_ID + "," +
        TABLE_SCHEM_NAME + "," +
        TABLE_NAME_NAME + "," +
        TABLE_TYPE_NAME + "," +
        TABLE_SEQ_NUM + "," +
        COLUMN_COUNT + "," +
        IMMUTABLE_ROWS + "," +
        DISABLE_WAL + "," +
        MULTI_TENANT +
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_INDEX_STATE =
            "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"( " + 
            TENANT_ID + "," +
            TABLE_SCHEM_NAME + "," +
            TABLE_NAME_NAME + "," +
            INDEX_STATE +
            ") VALUES (?, ?, ?, ?)";
    private static final String INSERT_COLUMN =
        "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"( " + 
        TENANT_ID + "," +
        TABLE_SCHEM_NAME + "," +
        TABLE_NAME_NAME + "," +
        COLUMN_NAME + "," +
        TABLE_CAT_NAME + "," +
        DATA_TYPE + "," +
        NULLABLE + "," +
        COLUMN_SIZE + "," +
        DECIMAL_DIGITS + "," +
        ORDINAL_POSITION + "," + 
        COLUMN_MODIFIER + "," +
        DATA_TABLE_NAME + "," + // write this both in the column and table rows for access by metadata APIs
        ARRAY_SIZE +
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_COLUMN_POSITION =
        "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\" ( " + 
        TENANT_ID + "," +
        TABLE_SCHEM_NAME + "," +
        TABLE_NAME_NAME + "," +
        COLUMN_NAME + "," +
        TABLE_CAT_NAME + "," +
        ORDINAL_POSITION +
        ") VALUES (?, ?, ?, ?, ?, ?)";
    
    private final PhoenixConnection connection;

    public MetaDataClient(PhoenixConnection connection) {
        this.connection = connection;
    }
    
    public PhoenixConnection getConnection() {
        return connection;
    }

    public long getCurrentTime(String schemaName, String tableName) throws SQLException {
        MetaDataMutationResult result = updateCache(schemaName, tableName, true);
        return result.getMutationTime();
    }
    
    /**
     * Update the cache with the latest as of the connection scn.
     * @param schemaName
     * @param tableName
     * @return the timestamp from the server, negative if the table was added to the cache and positive otherwise
     * @throws SQLException
     */
    public MetaDataMutationResult updateCache(String schemaName, String tableName) throws SQLException {
        return updateCache(schemaName, tableName, false);
    }
    
    private static final MetaDataMutationResult SYSTEM_TABLE_RESULT = new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS,QueryConstants.UNSET_TIMESTAMP,null);
    
    private MetaDataMutationResult updateCache(String schemaName, String tableName, boolean alwaysHitServer) throws SQLException { // TODO: pass byte[] here
        Long scn = connection.getSCN();
        long clientTimeStamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        if (TYPE_SCHEMA.equals(schemaName) && !alwaysHitServer) {
            return SYSTEM_TABLE_RESULT;
        }
        PTable table = null;
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        long tableTimestamp = HConstants.LATEST_TIMESTAMP;
        PName tenantIdName = connection.getTenantId();
        try {
            table = connection.getPMetaData().getTable(fullTableName);
            tableTimestamp = table.getTimeStamp();
        } catch (TableNotFoundException e) {
            // TODO: Try again on services cache, as we may be looking for
            // a global multi-tenant table
        }
        // Don't bother with server call: we can't possibly find a newer table
        if (table != null && tableTimestamp == clientTimeStamp - 1 && !alwaysHitServer) {
            return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS,QueryConstants.UNSET_TIMESTAMP,table);
        }
        
        byte[] tenantId = null;
        int maxTryCount = 1;
        if (tenantIdName != null) {
            tenantId = tenantIdName.getBytes();
            maxTryCount = 2;
        }
        int tryCount = 0;
        MetaDataMutationResult result;
        
        do {
            final byte[] schemaBytes = PDataType.VARCHAR.toBytes(schemaName);
            final byte[] tableBytes = PDataType.VARCHAR.toBytes(tableName);
            result = connection.getQueryServices().getTable(tenantId, schemaBytes, tableBytes, tableTimestamp, clientTimeStamp);
            
            if (TYPE_SCHEMA.equals(schemaName)) {
                return result;
            }
            MutationCode code = result.getMutationCode();
            PTable resultTable = result.getTable();
            // We found an updated table, so update our cache
            if (resultTable != null) {
                // Don't cache the table unless it has the same tenantId
                // as the connection or it's not multi-tenant.
                if (tryCount == 0 || !resultTable.isMultiTenant()) {
                    connection.addTable(resultTable);
                    return result;
                }
            } else {
                // if (result.getMutationCode() == MutationCode.NEWER_TABLE_FOUND) {
                // TODO: No table exists at the clientTimestamp, but a newer one exists.
                // Since we disallow creation or modification of a table earlier than the latest
                // timestamp, we can handle this such that we don't ask the
                // server again.
                // If table was not found at the current time stamp and we have one cached, remove it.
                // Otherwise, we're up to date, so there's nothing to do.
                if (table != null) {
                    result.setTable(table);
                    if (code == MutationCode.TABLE_ALREADY_EXISTS) {
                        return result;
                    }
                    if (code == MutationCode.TABLE_NOT_FOUND && tryCount + 1 == maxTryCount) {
                        connection.removeTable(fullTableName);
                    }
                }
            }
            tenantId = null;
        } while (++tryCount < maxTryCount);
        
        return result;
    }


    private void addColumnMutation(String schemaName, String tableName, PColumn column, PreparedStatement colUpsert, String parentTableName) throws SQLException {
        colUpsert.setString(1, connection.getTenantId() == null ? null : connection.getTenantId().getString());
        colUpsert.setString(2, schemaName);
        colUpsert.setString(3, tableName);
        colUpsert.setString(4, column.getName().getString());
        colUpsert.setString(5, column.getFamilyName() == null ? null : column.getFamilyName().getString());
        colUpsert.setInt(6, column.getDataType().getSqlType());
        colUpsert.setInt(7, column.isNullable() ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls);
        if (column.getMaxLength() == null) {
            colUpsert.setNull(8, Types.INTEGER);
        } else {
            colUpsert.setInt(8, column.getMaxLength());
        }
        if (column.getScale() == null) {
            colUpsert.setNull(9, Types.INTEGER);
        } else {
            colUpsert.setInt(9, column.getScale());
        }
        colUpsert.setInt(10, column.getPosition() + 1);
        colUpsert.setInt(11, ColumnModifier.toSystemValue(column.getColumnModifier()));
        colUpsert.setString(12, parentTableName);
        if (column.getArraySize() == null) {
            colUpsert.setNull(13, Types.INTEGER);
        } else {
            colUpsert.setInt(13, column.getArraySize());
        }
        colUpsert.execute();
    }

    private PColumn newColumn(int position, ColumnDef def, PrimaryKeyConstraint pkConstraint) throws SQLException {
        try {
            ColumnName columnDefName = def.getColumnDefName();
            ColumnModifier columnModifier = def.getColumnModifier();
            boolean isPK = def.isPK();
            if (pkConstraint != null) {
                Pair<ColumnName,ColumnModifier> pkColumnModifier = pkConstraint.getColumn(columnDefName);
                if (pkColumnModifier != null) {
                    isPK = true;
                    columnModifier = pkColumnModifier.getSecond();
                }
            }
            
            String columnName = columnDefName.getColumnName();
            PName familyName = null;
            if (def.isPK() && !pkConstraint.getColumnNames().isEmpty() ) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_ALREADY_EXISTS)
                    .setColumnName(columnName).build().buildException();
            }
            if (def.getColumnDefName().getFamilyName() != null) {
                String family = def.getColumnDefName().getFamilyName();
                if (isPK) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_WITH_FAMILY_NAME)
                        .setColumnName(columnName).setFamilyName(family).build().buildException();
                } else if (!def.isNull()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.KEY_VALUE_NOT_NULL)
                        .setColumnName(columnName).setFamilyName(family).build().buildException();
                }
                familyName = PNameFactory.newName(family);
            } else if (!isPK) {
                familyName = PNameFactory.newName(QueryConstants.DEFAULT_COLUMN_FAMILY);
            }
            
            PColumn column = new PColumnImpl(PNameFactory.newName(columnName), familyName, def.getDataType(),
                    def.getMaxLength(), def.getScale(), def.isNull(), position, columnModifier, def.getArraySize());
            return column;
        } catch (IllegalArgumentException e) { // Based on precondition check in constructor
            throw new SQLException(e);
        }
    }

    public MutationState createTable(CreateTableStatement statement, byte[][] splits, PTable parent, String viewStatement, ViewType viewType) throws SQLException {
        PTable table = createTableInternal(statement, splits, parent, viewStatement, viewType);
        if (table == null || table.getType() == PTableType.VIEW) {
            return new MutationState(0,connection);
        }
        
        // Hack to get around the case when an SCN is specified on the connection.
        // In this case, we won't see the table we just created yet, so we hack
        // around it by forcing the compiler to not resolve anything.
        PostDDLCompiler compiler = new PostDDLCompiler(connection);
        //connection.setAutoCommit(true);
        // Execute any necessary data updates
        Long scn = connection.getSCN();
        long ts = (scn == null ? table.getTimeStamp() : scn);
        // Getting the schema through the current connection doesn't work when the connection has an scn specified
        // Since the table won't be added to the current connection.
        TableRef tableRef = new TableRef(null, table, ts, false);
        byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(table.getColumnFamilies());
        MutationPlan plan = compiler.compile(Collections.singletonList(tableRef), emptyCF, null, null, tableRef.getTimeStamp());
        return connection.getQueryServices().updateData(plan);
    }

    private MutationState buildIndexAtTimeStamp(PTable index, NamedTableNode dataTableNode) throws SQLException {
        // If our connection is at a fixed point-in-time, we need to open a new
        // connection so that our new index table is visible.
        Properties props = new Properties(connection.getClientInfo());
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(connection.getSCN()+1));
        PhoenixConnection conn = DriverManager.getConnection(connection.getURL(), props).unwrap(PhoenixConnection.class);
        MetaDataClient newClientAtNextTimeStamp = new MetaDataClient(conn);
        
        // Re-resolve the tableRef from the now newer connection
        conn.setAutoCommit(true);
        ColumnResolver resolver = FromCompiler.getResolver(dataTableNode, conn);
        TableRef tableRef = resolver.getTables().get(0);
        boolean success = false;
        SQLException sqlException = null;
        try {
            MutationState state = newClientAtNextTimeStamp.buildIndex(index, tableRef);
            success = true;
            return state;
        } catch (SQLException e) {
            sqlException = e;
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                if (sqlException == null) {
                    // If we're not in the middle of throwing another exception
                    // then throw the exception we got on close.
                    if (success) {
                        sqlException = e;
                    }
                } else {
                    sqlException.setNextException(e);
                }
            }
            if (sqlException != null) {
                throw sqlException;
            }
        }
        throw new IllegalStateException(); // impossible
    }
    
    private MutationState buildIndex(PTable index, TableRef dataTableRef) throws SQLException {
        PostIndexDDLCompiler compiler = new PostIndexDDLCompiler(connection, dataTableRef);
        MutationPlan plan = compiler.compile(index);
        MutationState state = connection.getQueryServices().updateData(plan);
        AlterIndexStatement indexStatement = FACTORY.alterIndex(FACTORY.namedTable(null, 
                TableName.create(index.getSchemaName().getString(), index.getTableName().getString())),
                dataTableRef.getTable().getTableName().getString(), false, PIndexState.ACTIVE);
        alterIndex(indexStatement);
        return state;
    }

    /**
     * Create an index table by morphing the CreateIndexStatement into a CreateTableStatement and calling
     * MetaDataClient.createTable. In doing so, we perform the following translations:
     * 1) Change the type of any columns being indexed to types that support null if the column is nullable.
     *    For example, a BIGINT type would be coerced to a DECIMAL type, since a DECIMAL type supports null
     *    when it's in the row key while a BIGINT does not.
     * 2) Append any row key column from the data table that is not in the indexed column list. Our indexes
     *    rely on having a 1:1 correspondence between the index and data rows.
     * 3) Change the name of the columns to include the column family. For example, if you have a column
     *    named "B" in a column family named "A", the indexed column name will be "A:B". This makes it easy
     *    to translate the column references in a query to the correct column references in an index table
     *    regardless of whether the column reference is prefixed with the column family name or not. It also
     *    has the side benefit of allowing the same named column in different column families to both be
     *    listed as an index column.
     * @param statement
     * @param splits
     * @return MutationState from population of index table from data table
     * @throws SQLException
     */
    public MutationState createIndex(CreateIndexStatement statement, byte[][] splits) throws SQLException {
        PrimaryKeyConstraint pk = statement.getIndexConstraint();
        TableName indexTableName = statement.getIndexTableName();
        
        List<Pair<ColumnName, ColumnModifier>> indexedPkColumns = pk.getColumnNames();
        List<ColumnName> includedColumns = statement.getIncludeColumns();
        TableRef tableRef = null;
        PTable table = null;
        boolean retry = true;
        while (true) {
            try {
                ColumnResolver resolver = FromCompiler.getResolver(statement, connection);
                tableRef = resolver.getTables().get(0);
                PTable dataTable = tableRef.getTable();
                int hbaseVersion = connection.getQueryServices().getLowestClusterHBaseVersion();
                if (!dataTable.isImmutableRows()) {
                    if (hbaseVersion < PhoenixDatabaseMetaData.MUTABLE_SI_VERSION_THRESHOLD) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_MUTABLE_INDEXES).setTableName(indexTableName.getTableName()).build().buildException();
                    }
                    if (connection.getQueryServices().hasInvalidIndexConfiguration()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_MUTABLE_INDEX_CONFIG).setTableName(indexTableName.getTableName()).build().buildException();
                    }
                }
                Set<PColumn> unusedPkColumns;
                if (dataTable.getBucketNum() != null) { // Ignore SALT column
                    unusedPkColumns = new LinkedHashSet<PColumn>(dataTable.getPKColumns().subList(1, dataTable.getPKColumns().size()));
                } else {
                    unusedPkColumns = new LinkedHashSet<PColumn>(dataTable.getPKColumns());
                }
                List<Pair<ColumnName, ColumnModifier>> allPkColumns = Lists.newArrayListWithExpectedSize(unusedPkColumns.size());
                List<ColumnDef> columnDefs = Lists.newArrayListWithExpectedSize(includedColumns.size() + indexedPkColumns.size());
                
                // First columns are the indexed ones
                for (Pair<ColumnName, ColumnModifier> pair : indexedPkColumns) {
                    ColumnName colName = pair.getFirst();
                    PColumn col = resolver.resolveColumn(null, colName.getFamilyName(), colName.getColumnName()).getColumn();
                    unusedPkColumns.remove(col);
                    PDataType dataType = IndexUtil.getIndexColumnDataType(col);
                    colName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(col));
                    allPkColumns.add(new Pair<ColumnName, ColumnModifier>(colName, pair.getSecond()));
                    columnDefs.add(FACTORY.columnDef(colName, dataType.getSqlTypeName(), col.isNullable(), col.getMaxLength(), col.getScale(), false, null));
                }
                
                // Next all the PK columns from the data table that aren't indexed
                if (!unusedPkColumns.isEmpty()) {
                    for (PColumn col : unusedPkColumns) {
                        ColumnName colName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(col));
                        allPkColumns.add(new Pair<ColumnName, ColumnModifier>(colName, col.getColumnModifier()));
                        PDataType dataType = IndexUtil.getIndexColumnDataType(col);
                        columnDefs.add(FACTORY.columnDef(colName, dataType.getSqlTypeName(), col.isNullable(), col.getMaxLength(), col.getScale(), false, col.getColumnModifier()));
                    }
                }
                pk = FACTORY.primaryKey(null, allPkColumns);
                
                // Last all the included columns (minus any PK columns)
                for (ColumnName colName : includedColumns) {
                    PColumn col = resolver.resolveColumn(null, colName.getFamilyName(), colName.getColumnName()).getColumn();
                    if (SchemaUtil.isPKColumn(col)) {
                        if (!unusedPkColumns.contains(col)) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_EXIST_IN_DEF).build().buildException();
                        }
                    } else {
                        colName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(col));
                        // Check for duplicates between indexed and included columns
                        if (pk.contains(colName)) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_EXIST_IN_DEF).build().buildException();
                        }
                        if (!SchemaUtil.isPKColumn(col)) {
                            // Need to re-create ColumnName, since the above one won't have the column family name
                            colName = ColumnName.caseSensitiveColumnName(col.getFamilyName().getString(), IndexUtil.getIndexColumnName(col));
                            columnDefs.add(FACTORY.columnDef(colName, col.getDataType().getSqlTypeName(), col.isNullable(), col.getMaxLength(), col.getScale(), false, col.getColumnModifier()));
                        }
                    }
                }
                
                CreateTableStatement tableStatement = FACTORY.createTable(indexTableName, statement.getProps(), columnDefs, pk, statement.getSplitNodes(), PTableType.INDEX, statement.ifNotExists(), null, null, statement.getBindCount());
                table = createTableInternal(tableStatement, splits, tableRef.getTable(), null, null); // TODO: tenant-specific index
                break;
            } catch (ConcurrentTableMutationException e) { // Can happen if parent data table changes while above is in progress
                if (retry) {
                    retry = false;
                    continue;
                }
                throw e;
            }
        }
        if (table == null) {
            return new MutationState(0,connection);
        }
        // If our connection is at a fixed point-in-time, we need to open a new
        // connection so that our new index table is visible.
        if (connection.getSCN() != null) {
            return buildIndexAtTimeStamp(table, statement.getTable());
        }
        
        return buildIndex(table, tableRef);
    }

    public MutationState dropSequence(DropSequenceStatement statement) throws SQLException {
        Long scn = connection.getSCN();
        long timestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        String schemaName = statement.getSequenceName().getSchemaName();
        String sequenceName = statement.getSequenceName().getTableName();
        String tenantId = connection.getTenantId() == null ? null : connection.getTenantId().getString();
        try {
            connection.getQueryServices().dropSequence(tenantId, schemaName, sequenceName, timestamp);
        } catch (SequenceNotFoundException e) {
            if (statement.ifExists()) {
                return new MutationState(0, connection);
            }
            throw e;
        }
        return new MutationState(1, connection);
    }
    
    public MutationState createSequence(CreateSequenceStatement statement, long startWith, long incrementBy, int cacheSize) throws SQLException {
        Long scn = connection.getSCN();
        long timestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        String schemaName = statement.getSequenceName().getSchemaName();
        String sequenceName = statement.getSequenceName().getTableName();
        String tenantId = connection.getTenantId() == null ? null : connection.getTenantId().getString();
        try {
            connection.getQueryServices().createSequence(tenantId, schemaName, sequenceName, startWith, incrementBy, cacheSize, timestamp);
        } catch (SequenceAlreadyExistsException e) {
            if (statement.ifNotExists()) {
                return new MutationState(0, connection);
            }
            throw e;
        }
        return new MutationState(1, connection);
    }
    
    private static ColumnDef findColumnDefOrNull(List<ColumnDef> colDefs, ColumnName colName) {
        for (ColumnDef colDef : colDefs) {
            if (colDef.getColumnDefName().getColumnName().equals(colName.getColumnName())) {
                return colDef;
            }
        }
        return null;
    }
    
    private PTable createTableInternal(CreateTableStatement statement, byte[][] splits, final PTable parent, String viewStatement, ViewType viewType) throws SQLException {
        final PTableType tableType = statement.getTableType();
        boolean wasAutoCommit = connection.getAutoCommit();
        connection.rollback();
        try {
            connection.setAutoCommit(false);
            List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize(statement.getColumnDefs().size() + 3);
            
            TableName tableNameNode = statement.getTableName();
            String schemaName = tableNameNode.getSchemaName();
            String tableName = tableNameNode.getTableName();
            String parentTableName = null;
            String tenantId = connection.getTenantId() == null ? null : connection.getTenantId().getString();
            boolean isParentImmutableRows = false;
            if (parent != null && tableType == PTableType.INDEX) {
                isParentImmutableRows = parent.isImmutableRows();
                parentTableName = parent.getTableName().getString();
                // Pass through data table sequence number so we can check it hasn't changed
                PreparedStatement incrementStatement = connection.prepareStatement(INCREMENT_SEQ_NUM);
                incrementStatement.setString(1, connection.getTenantId() == null ? null : connection.getTenantId().getString());
                incrementStatement.setString(2, schemaName);
                incrementStatement.setString(3, parentTableName);
                incrementStatement.setLong(4, parent.getSequenceNumber());
                incrementStatement.execute();
                // Get list of mutations and add to table meta data that will be passed to server
                // to guarantee order. This row will always end up last
                tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                connection.rollback();

                // Add row linking from data table row to index table row
                PreparedStatement linkStatement = connection.prepareStatement(CREATE_LINK);
                linkStatement.setString(1, connection.getTenantId() == null ? null : connection.getTenantId().getString());
                linkStatement.setString(2, schemaName);
                linkStatement.setString(3, parentTableName);
                linkStatement.setString(4, tableName);
                linkStatement.setByte(5, LinkType.INDEX_TABLE.getSerializedValue());
                linkStatement.execute();
            }
            
            PrimaryKeyConstraint pkConstraint = statement.getPrimaryKeyConstraint();
            String pkName = null;
            List<Pair<ColumnName,ColumnModifier>> pkColumnsNames = Collections.<Pair<ColumnName,ColumnModifier>>emptyList();
            Iterator<Pair<ColumnName,ColumnModifier>> pkColumnsIterator = Iterators.emptyIterator();
            if (pkConstraint != null) {
                pkColumnsNames = pkConstraint.getColumnNames();
                pkColumnsIterator = pkColumnsNames.iterator();
                pkName = pkConstraint.getName();
            }
            
            Map<String,Object> tableProps = Maps.newHashMapWithExpectedSize(statement.getProps().size());
            Map<String,Object> commonFamilyProps = Collections.emptyMap();
            // Somewhat hacky way of determining if property is for HColumnDescriptor or HTableDescriptor
            HColumnDescriptor defaultDescriptor = new HColumnDescriptor(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES);
            if (!statement.getProps().isEmpty()) {
                commonFamilyProps = Maps.newHashMapWithExpectedSize(statement.getProps().size());
                
                Collection<Pair<String,Object>> props = statement.getProps().get(QueryConstants.ALL_FAMILY_PROPERTIES_KEY);
                for (Pair<String,Object> prop : props) {
                    if (defaultDescriptor.getValue(prop.getFirst()) == null) {
                        tableProps.put(prop.getFirst(), prop.getSecond());
                    } else {
                        commonFamilyProps.put(prop.getFirst(), prop.getSecond());
                    }
                }
            }
            
            boolean isSalted = false;
            Integer saltBucketNum = null;
            String defaultFamilyName = null;
            boolean isImmutableRows = false;
            boolean multiTenant = false;
            // Although unusual, it's possible to set a mapped VIEW as having immutable rows.
            // This tells Phoenix that you're managing the index maintenance yourself.
            if (tableType != PTableType.VIEW || viewType == ViewType.MAPPED) {
                Boolean isImmutableRowsProp = (Boolean) tableProps.remove(PTable.IS_IMMUTABLE_ROWS_PROP_NAME);
                if (isImmutableRowsProp == null) {
                    isImmutableRows = connection.getQueryServices().getProps().getBoolean(QueryServices.IMMUTABLE_ROWS_ATTRIB, QueryServicesOptions.DEFAULT_IMMUTABLE_ROWS);
                } else {
                    isImmutableRows = isImmutableRowsProp;
                }
            }
            
            if (tableType != PTableType.VIEW) {
                saltBucketNum = (Integer) tableProps.remove(PhoenixDatabaseMetaData.SALT_BUCKETS);
                if (saltBucketNum != null && (saltBucketNum < 0 || saltBucketNum > SaltingUtil.MAX_BUCKET_NUM)) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_BUCKET_NUM).build().buildException();
                }
                // Salt the index table if the data table is salted
                if (saltBucketNum == null) {
                    if (parent != null) {
                        saltBucketNum = parent.getBucketNum();
                    }
                } else if (saltBucketNum.intValue() == 0) {
                    saltBucketNum = null; // Provides a way for an index to not be salted if its data table is salted
                }
                isSalted = (saltBucketNum != null);
                
                defaultFamilyName = (String)tableProps.remove(PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME);
                if (defaultFamilyName == null) {
                    // Until we change this default, we need to have all tables set it
                    defaultFamilyName = QueryConstants.DEFAULT_COLUMN_FAMILY;
                }
                
                Boolean multiTenantProp = (Boolean) tableProps.remove(PhoenixDatabaseMetaData.MULTI_TENANT);
                multiTenant = Boolean.TRUE.equals(multiTenantProp);
            }
            
            boolean disableWAL = false;
            Boolean disableWALProp = (Boolean) tableProps.remove(PhoenixDatabaseMetaData.DISABLE_WAL);
            if (disableWALProp == null) {
                disableWAL = isParentImmutableRows; // By default, disable WAL for immutable indexes
            } else {
                disableWAL = disableWALProp;
            }
            // Delay this check as it is supported to have IMMUTABLE_ROWS and SALT_BUCKETS defined on views
            if (statement.getTableType() == PTableType.VIEW && !tableProps.isEmpty()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.VIEW_WITH_PROPERTIES).build().buildException();
            }
            
            List<ColumnDef> colDefs = statement.getColumnDefs();
            List<PColumn> columns;
            LinkedHashSet<PColumn> pkColumns;    
            
            if (tenantId != null && tableType != PTableType.VIEW) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CREATE_TENANT_SPECIFIC_TABLE)
                    .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            }
            
            List<PName> physicalNames = Collections.emptyList();
            if (tableType == PTableType.VIEW) {
                physicalNames = Collections.singletonList(PNameFactory.newName(parent.getPhysicalName().getString()));
                if (viewType == ViewType.MAPPED) {
                    columns = newArrayListWithExpectedSize(colDefs.size());
                    pkColumns = newLinkedHashSetWithExpectedSize(colDefs.size());  
                } else {
                    // Propagate property values to VIEW.
                    // TODO: formalize the known set of these properties
                    multiTenant = parent.isMultiTenant();
                    saltBucketNum = parent.getBucketNum();
                    isImmutableRows = parent.isImmutableRows();
                    disableWAL = (disableWALProp == null ? parent.isWALDisabled() : disableWALProp);
                    defaultFamilyName = parent.getDefaultFamilyName() == null ? null : parent.getDefaultFamilyName().getString();
                    columns = newArrayListWithExpectedSize(parent.getColumns().size() + colDefs.size());
                    columns.addAll(parent.getColumns());
                    pkColumns = newLinkedHashSet(parent.getPKColumns());

                    // Add row linking from data table row to physical table row
                    PreparedStatement linkStatement = connection.prepareStatement(CREATE_LINK);
                    for (PName physicalName : physicalNames) {
                        linkStatement.setString(1, connection.getTenantId() == null ? null : connection.getTenantId().getString());
                        linkStatement.setString(2, schemaName);
                        linkStatement.setString(3, tableName);
                        linkStatement.setString(4, physicalName.getString());
                        linkStatement.setByte(5, LinkType.PHYSICAL_TABLE.getSerializedValue());
                        linkStatement.execute();
                    }
                }
            } else {
                columns = newArrayListWithExpectedSize(colDefs.size());
                pkColumns = newLinkedHashSetWithExpectedSize(colDefs.size() + 1); // in case salted  
            }
            
            PreparedStatement colUpsert = connection.prepareStatement(INSERT_COLUMN);
            Map<String, PName> familyNames = Maps.newLinkedHashMap();
            boolean isPK = false;
            
            int positionOffset = columns.size();
            if (isSalted) {
                positionOffset++;
                pkColumns.add(SaltingUtil.SALTING_COLUMN);
            }
            int position = positionOffset;
            
            for (ColumnDef colDef : colDefs) {
                if (colDef.isPK()) {
                    if (isPK) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_ALREADY_EXISTS)
                            .setColumnName(colDef.getColumnDefName().getColumnName()).build().buildException();
                    }
                    isPK = true;
                }
                PColumn column = newColumn(position++, colDef, pkConstraint);
                if (SchemaUtil.isPKColumn(column)) {
                    // TODO: remove this constraint?
                    if (pkColumnsIterator.hasNext() && !column.getName().getString().equals(pkColumnsIterator.next().getFirst().getColumnName())) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_OUT_OF_ORDER)
                            .setSchemaName(schemaName)
                            .setTableName(tableName)
                            .setColumnName(column.getName().getString())
                            .build().buildException();
                    }
                    if (tableType == PTableType.VIEW && viewType != ViewType.MAPPED) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_DEFINE_PK_FOR_VIEW)
                            .setSchemaName(schemaName)
                            .setTableName(tableName)
                            .setColumnName(colDef.getColumnDefName().getColumnName())
                            .build().buildException();
                    }
                    // disallow array type usage in primary key constraint
                    if (colDef.isArray()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.ARRAY_NOT_ALLOWED_IN_PRIMARY_KEY)
                        .setSchemaName(schemaName)
                        .setTableName(tableName)
                        .setColumnName(colDef.getColumnDefName().getColumnName())
                        .build().buildException();
                    }
                    if (!pkColumns.add(column)) {
                        throw new ColumnAlreadyExistsException(schemaName, tableName, column.getName().getString());
                    }
                }
                if (tableType == PTableType.VIEW && hasColumnWithSameNameAndFamily(columns, column)) {
                    // we only need to check for dup columns for views because they inherit columns from parent
                    throw new ColumnAlreadyExistsException(schemaName, tableName, column.getName().getString());
                }
                columns.add(column);
                if (colDef.getDataType() == PDataType.VARBINARY 
                        && SchemaUtil.isPKColumn(column)
                        && pkColumnsNames.size() > 1 
                        && column.getPosition() < pkColumnsNames.size() - 1) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.VARBINARY_IN_ROW_KEY)
                        .setSchemaName(schemaName)
                        .setTableName(tableName)
                        .setColumnName(column.getName().getString())
                        .build().buildException();
                }
                if (column.getFamilyName() != null) {
                    familyNames.put(column.getFamilyName().getString(),column.getFamilyName());
                }
            }
            // We need a PK definition for a TABLE or mapped VIEW
            if (!isPK && pkColumnsNames.isEmpty() && tableType != PTableType.VIEW && viewType != ViewType.MAPPED) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_MISSING)
                    .setSchemaName(schemaName)
                    .setTableName(tableName)
                    .build().buildException();
            }
            if (!pkColumnsNames.isEmpty() && pkColumnsNames.size() != pkColumns.size() - positionOffset) { // Then a column name in the primary key constraint wasn't resolved
                Iterator<Pair<ColumnName,ColumnModifier>> pkColumnNamesIterator = pkColumnsNames.iterator();
                while (pkColumnNamesIterator.hasNext()) {
                    ColumnName colName = pkColumnNamesIterator.next().getFirst();
                    ColumnDef colDef = findColumnDefOrNull(colDefs, colName);
                    if (colDef == null) {
                        throw new ColumnNotFoundException(schemaName, tableName, null, colName.getColumnName());
                    }
                    if (colDef.getColumnDefName().getFamilyName() != null) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_WITH_FAMILY_NAME)
                        .setSchemaName(schemaName)
                        .setTableName(tableName)
                        .setColumnName(colDef.getColumnDefName().getColumnName() )
                        .setFamilyName(colDef.getColumnDefName().getFamilyName())
                        .build().buildException();
                    }
                }
                // The above should actually find the specific one, but just in case...
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_PRIMARY_KEY_CONSTRAINT)
                    .setSchemaName(schemaName)
                    .setTableName(tableName)
                    .build().buildException();
            }
            
            List<Pair<byte[],Map<String,Object>>> familyPropList = Lists.newArrayListWithExpectedSize(familyNames.size());
            if (!statement.getProps().isEmpty()) {
                for (String familyName : statement.getProps().keySet()) {
                    if (!familyName.equals(QueryConstants.ALL_FAMILY_PROPERTIES_KEY)) {
                        if (familyNames.get(familyName) == null) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.PROPERTIES_FOR_FAMILY)
                                .setFamilyName(familyName).build().buildException();
                        } else if (statement.getTableType() == PTableType.VIEW) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.VIEW_WITH_PROPERTIES).build().buildException();
                        }
                    }
                }
            }
            throwIfInsufficientColumns(schemaName, tableName, pkColumns, isSalted, multiTenant);
            
            for (PName familyName : familyNames.values()) {
                Collection<Pair<String,Object>> props = statement.getProps().get(familyName.getString());
                if (props.isEmpty()) {
                    familyPropList.add(new Pair<byte[],Map<String,Object>>(familyName.getBytes(),commonFamilyProps));
                } else {
                    Map<String,Object> combinedFamilyProps = Maps.newHashMapWithExpectedSize(props.size() + commonFamilyProps.size());
                    combinedFamilyProps.putAll(commonFamilyProps);
                    for (Pair<String,Object> prop : props) {
                        combinedFamilyProps.put(prop.getFirst(), prop.getSecond());
                    }
                    familyPropList.add(new Pair<byte[],Map<String,Object>>(familyName.getBytes(),combinedFamilyProps));
                }
            }
            
            
            // Bootstrapping for our SYSTEM.TABLE that creates itself before it exists 
            if (SchemaUtil.isMetaTable(schemaName,tableName)) {
                PTable table = PTableImpl.makePTable(PNameFactory.newName(schemaName),PNameFactory.newName(tableName), tableType, null,
                        MetaDataProtocol.MIN_TABLE_TIMESTAMP, PTable.INITIAL_SEQ_NUM, PNameFactory.newName(QueryConstants.SYSTEM_TABLE_PK_NAME),
                        null, columns, null, Collections.<PTable>emptyList(), isImmutableRows, 
                        Collections.<PName>emptyList(), defaultFamilyName == null ? null : PNameFactory.newName(defaultFamilyName),
                        null, Boolean.TRUE.equals(disableWAL), false, null);
                connection.addTable(table);
            } else if (tableType == PTableType.INDEX) {
                if (tableProps.get(HTableDescriptor.MAX_FILESIZE) == null) {
                    int nIndexRowKeyColumns = isPK ? 1 : pkColumnsNames.size();
                    int nIndexKeyValueColumns = columns.size() - nIndexRowKeyColumns;
                    int nBaseRowKeyColumns = parent.getPKColumns().size() - (parent.getBucketNum() == null ? 0 : 1);
                    int nBaseKeyValueColumns = parent.getColumns().size() - parent.getPKColumns().size();
                    /* 
                     * Approximate ratio between index table size and data table size:
                     * More or less equal to the ratio between the number of key value columns in each. We add one to
                     * the key value column count to take into account our empty key value. We add 1/4 for any key
                     * value data table column that was moved into the index table row key.
                     */
                    double ratio = (1+nIndexKeyValueColumns + (nIndexRowKeyColumns - nBaseRowKeyColumns)/4d)/(1+nBaseKeyValueColumns);
                    HTableDescriptor descriptor = connection.getQueryServices().getTableDescriptor(parent.getPhysicalName().getBytes());
                    if (descriptor != null) { // Is null for connectionless
                        long maxFileSize = descriptor.getMaxFileSize();
                        if (maxFileSize == -1) { // If unset, use default
                            maxFileSize = HConstants.DEFAULT_MAX_FILE_SIZE;
                        }
                        tableProps.put(HTableDescriptor.MAX_FILESIZE, (long)(maxFileSize * ratio));
                    }
                }
            }
            
            for (PColumn column : columns) {
                addColumnMutation(schemaName, tableName, column, colUpsert, parentTableName);
            }
            
            tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
            connection.rollback();
            
            String dataTableName = parent == null || tableType == PTableType.VIEW ? null : parent.getTableName().getString();
            PIndexState indexState = parent == null || tableType == PTableType.VIEW  ? null : PIndexState.BUILDING;
            PreparedStatement tableUpsert = connection.prepareStatement(CREATE_TABLE);
            tableUpsert.setString(1, tenantId);
            tableUpsert.setString(2, schemaName);
            tableUpsert.setString(3, tableName);
            tableUpsert.setString(4, tableType.getSerializedValue());
            tableUpsert.setLong(5, PTable.INITIAL_SEQ_NUM);
            tableUpsert.setInt(6, position);
            if (saltBucketNum != null) {
                tableUpsert.setInt(7, saltBucketNum);
            } else {
                tableUpsert.setNull(7, Types.INTEGER);
            }
            tableUpsert.setString(8, pkName);
            tableUpsert.setString(9, dataTableName);
            tableUpsert.setString(10, indexState == null ? null : indexState.getSerializedValue());
            tableUpsert.setBoolean(11, isImmutableRows);
            tableUpsert.setString(12, defaultFamilyName);
            tableUpsert.setString(13, viewStatement);
            tableUpsert.setBoolean(14, disableWAL);
            tableUpsert.setBoolean(15, multiTenant);
            if (viewType == null) {
                tableUpsert.setNull(16, Types.TINYINT);
            } else {
                tableUpsert.setByte(16, viewType.getSerializedValue());
            }
            tableUpsert.execute();
            
            tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
            connection.rollback();
            
            /*
             * The table metadata must be in the following order:
             * 1) table header row
             * 2) everything else
             * 3) parent table header row
             */
            Collections.reverse(tableMetaData);
            
            splits = SchemaUtil.processSplits(splits, pkColumns, saltBucketNum, connection.getQueryServices().getProps().getBoolean(
                    QueryServices.ROW_KEY_ORDER_SALTED_TABLE_ATTRIB, QueryServicesOptions.DEFAULT_ROW_KEY_ORDER_SALTED_TABLE));
            MetaDataMutationResult result = connection.getQueryServices().createTable(
                    tableMetaData, 
                    viewType == ViewType.MAPPED ? physicalNames.get(0).getBytes() : null,
                    tableType, tableProps, familyPropList, splits);
            MutationCode code = result.getMutationCode();
            switch(code) {
            case TABLE_ALREADY_EXISTS:
                connection.addTable(result.getTable());
                if (!statement.ifNotExists()) {
                    throw new TableAlreadyExistsException(schemaName, tableName);
                }
                return null;
            case PARENT_TABLE_NOT_FOUND:
                throw new TableNotFoundException(schemaName, parent.getName().getString());
            case NEWER_TABLE_FOUND:
                throw new NewerTableAlreadyExistsException(schemaName, tableName);
            case UNALLOWED_TABLE_MUTATION:
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MUTATE_TABLE)
                    .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            case CONCURRENT_TABLE_MUTATION:
                connection.addTable(result.getTable());
                throw new ConcurrentTableMutationException(schemaName, tableName);
            default:
                PTable table =  PTableImpl.makePTable(
                        PNameFactory.newName(schemaName), PNameFactory.newName(tableName), tableType, indexState, result.getMutationTime(), PTable.INITIAL_SEQ_NUM, 
                        pkName == null ? null : PNameFactory.newName(pkName), saltBucketNum, columns, dataTableName == null ? null : PNameFactory.newName(dataTableName), 
                        Collections.<PTable>emptyList(), isImmutableRows, physicalNames,
                        defaultFamilyName == null ? null : PNameFactory.newName(defaultFamilyName),
                                viewStatement, Boolean.TRUE.equals(disableWAL), multiTenant, viewType);
                connection.addTable(table);
                return table;
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }
    
    private static boolean hasColumnWithSameNameAndFamily(Collection<PColumn> columns, PColumn column) {
        for (PColumn currColumn : columns) {
           if (Objects.equal(currColumn.getFamilyName(), column.getFamilyName()) &&
               Objects.equal(currColumn.getName(), column.getName())) {
               return true;
           }
        }
        return false;
    }
    
    /**
     * A table can be a parent table to tenant-specific tables if all of the following conditions are true:
     * <p>
     * FOR TENANT-SPECIFIC TABLES WITH TENANT_TYPE_ID SPECIFIED:
     * <ol>
     * <li>It has 3 or more PK columns AND
     * <li>First PK (tenant id) column is not nullible AND 
     * <li>Firsts PK column's data type is either VARCHAR or CHAR AND
     * <li>Second PK (tenant type id) column is not nullible AND
     * <li>Second PK column data type is either VARCHAR or CHAR
     * </ol>
     * FOR TENANT-SPECIFIC TABLES WITH NO TENANT_TYPE_ID SPECIFIED:
     * <ol>
     * <li>It has 2 or more PK columns AND
     * <li>First PK (tenant id) column is not nullible AND 
     * <li>Firsts PK column's data type is either VARCHAR or CHAR
     * </ol>
     */
    private static void throwIfInsufficientColumns(String schemaName, String tableName, Collection<PColumn> columns, boolean isSalted, boolean isMultiTenant) throws SQLException {
        if (!isMultiTenant) {
            return;
        }
        int nPKColumns = columns.size() - (isSalted ? 1 : 0);
        if (nPKColumns < 2) {
            throw new SQLExceptionInfo.Builder(INSUFFICIENT_MULTI_TENANT_COLUMNS).setSchemaName(schemaName).setTableName(tableName).build().buildException();
        }
        Iterator<PColumn> iterator = columns.iterator();
        if (isSalted) {
            iterator.next();
        }
        PColumn tenantIdCol = iterator.next();
        if (!tenantIdCol.getDataType().isCoercibleTo(VARCHAR)) {
            throw new SQLExceptionInfo.Builder(INSUFFICIENT_MULTI_TENANT_COLUMNS).setSchemaName(schemaName).setTableName(tableName).build().buildException();
        }
    }

    public MutationState dropTable(DropTableStatement statement) throws SQLException {
        String schemaName = statement.getTableName().getSchemaName();
        String tableName = statement.getTableName().getTableName();
        return dropTable(schemaName, tableName, null, statement.getTableType(), statement.ifExists());
    }

    public MutationState dropIndex(DropIndexStatement statement) throws SQLException {
        String schemaName = statement.getTableName().getSchemaName();
        String tableName = statement.getIndexName().getName();
        String parentTableName = statement.getTableName().getTableName();
        return dropTable(schemaName, tableName, parentTableName, PTableType.INDEX, statement.ifExists());
    }

    private MutationState dropTable(String schemaName, String tableName, String parentTableName, PTableType tableType, boolean ifExists) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            String tenantId = connection.getTenantId() == null ? null : connection.getTenantId().getString();
            byte[] key = SchemaUtil.getTableKey(tenantId, schemaName, tableName);
            Long scn = connection.getSCN();
            long clientTimeStamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
            List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize(2);
            @SuppressWarnings("deprecation") // FIXME: Remove when unintentionally deprecated method is fixed (HBASE-7870).
            Delete tableDelete = new Delete(key, clientTimeStamp, null);
            tableMetaData.add(tableDelete);
            if (parentTableName != null) {
                byte[] linkKey = MetaDataUtil.getParentLinkKey(tenantId, schemaName, parentTableName, tableName);
                @SuppressWarnings("deprecation") // FIXME: Remove when unintentionally deprecated method is fixed (HBASE-7870).
                Delete linkDelete = new Delete(linkKey, clientTimeStamp, null);
                tableMetaData.add(linkDelete);
            }

            MetaDataMutationResult result = connection.getQueryServices().dropTable(tableMetaData, tableType);
            MutationCode code = result.getMutationCode();
            switch(code) {
                case TABLE_NOT_FOUND:
                    if (!ifExists) {
                        throw new TableNotFoundException(schemaName, tableName);
                    }
                    break;
                case NEWER_TABLE_FOUND:
                    throw new NewerTableAlreadyExistsException(schemaName, tableName);
                case UNALLOWED_TABLE_MUTATION:
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MUTATE_TABLE)
                        .setSchemaName(schemaName).setTableName(tableName).build().buildException();
                default:
                    try {
                        // TODO: should we update the parent table by removing the index?
                        connection.removeTable(tableName);
                    } catch (TableNotFoundException ignore) { } // Ignore - just means wasn't cached
                    
                    boolean dropMetaData = connection.getQueryServices().getProps().getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
                    if (result.getTable() != null && tableType != PTableType.VIEW && !dropMetaData) {
                        connection.setAutoCommit(true);
                        // Delete everything in the column. You'll still be able to do queries at earlier timestamps
                        long ts = (scn == null ? result.getMutationTime() : scn);
                        // Create empty table and schema - they're only used to get the name from
                        // PName name, PTableType type, long timeStamp, long sequenceNumber, List<PColumn> columns
                        PTable table = result.getTable();
                        List<TableRef> tableRefs = Lists.newArrayListWithExpectedSize(1 + table.getIndexes().size());
                        tableRefs.add(new TableRef(null, table, ts, false));
                        // TODO: Let the standard mutable secondary index maintenance handle this?
                        for (PTable index: table.getIndexes()) {
                            tableRefs.add(new TableRef(null, index, ts, false));
                        }
                        MutationPlan plan = new PostDDLCompiler(connection).compile(tableRefs, null, null, Collections.<PColumn>emptyList(), ts);
                        return connection.getQueryServices().updateData(plan);
                    }
                    break;
                }
                 return new MutationState(0,connection);
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    private MutationCode processMutationResult(String schemaName, String tableName, MetaDataMutationResult result) throws SQLException {
        final MutationCode mutationCode = result.getMutationCode();
        switch (mutationCode) {
        case TABLE_NOT_FOUND:
            connection.removeTable(tableName);
            throw new TableNotFoundException(schemaName, tableName);
        case UNALLOWED_TABLE_MUTATION:
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MUTATE_TABLE)
                .setSchemaName(schemaName).setTableName(tableName).build().buildException();
        case COLUMN_ALREADY_EXISTS:
        case COLUMN_NOT_FOUND:
            break;
        case CONCURRENT_TABLE_MUTATION:
            connection.addTable(result.getTable());
            if (logger.isDebugEnabled()) {
                logger.debug("CONCURRENT_TABLE_MUTATION for table " + SchemaUtil.getTableName(schemaName, tableName));
            }
            throw new ConcurrentTableMutationException(schemaName, tableName);
        case NEWER_TABLE_FOUND:
            if (result.getTable() != null) {
                connection.addTable(result.getTable());
            }
            throw new NewerTableAlreadyExistsException(schemaName, tableName);
        case NO_PK_COLUMNS:
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_MISSING)
                .setSchemaName(schemaName).setTableName(tableName).build().buildException();
        case TABLE_ALREADY_EXISTS:
            break;
        default:
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNEXPECTED_MUTATION_CODE).setSchemaName(schemaName)
                .setTableName(tableName).setMessage("mutation code: " + mutationCode).build().buildException();
        }
        return mutationCode;
    }

    private  long incrementTableSeqNum(PTable table, PTableType expectedType, int columnCountDelta) throws SQLException {
        return incrementTableSeqNum(table, expectedType, table.isImmutableRows(), table.isWALDisabled(), table.isMultiTenant(), columnCountDelta);
    }
    
    private long incrementTableSeqNum(PTable table, PTableType expectedType, boolean isImmutableRows, boolean disableWAL, boolean isMultiTenant, int columnCountDelta) throws SQLException {
        String schemaName = table.getSchemaName().getString();
        String tableName = table.getTableName().getString();
        // Ordinal position is 1-based and we don't count SALT column in ordinal position
        int totalColumnCount = table.getColumns().size() + (table.getBucketNum() == null ? 0 : -1);
        final long seqNum = table.getSequenceNumber() + 1;
        PreparedStatement tableUpsert = connection.prepareStatement(MUTATE_TABLE);
        try {
            tableUpsert.setString(1, connection.getTenantId() == null ? null : connection.getTenantId().getString());
            tableUpsert.setString(2, schemaName);
            tableUpsert.setString(3, tableName);
            tableUpsert.setString(4, expectedType.getSerializedValue());
            tableUpsert.setLong(5, seqNum);
            tableUpsert.setInt(6, totalColumnCount + columnCountDelta);
            tableUpsert.setBoolean(7, isImmutableRows);
            tableUpsert.setBoolean(8, disableWAL);
            tableUpsert.setBoolean(9, isMultiTenant);
            tableUpsert.execute();
        } finally {
            tableUpsert.close();
        }
        return seqNum;
    }
    
    public MutationState addColumn(AddColumnStatement statement) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);
            TableName tableNameNode = statement.getTable().getName();
            String schemaName = tableNameNode.getSchemaName();
            String tableName = tableNameNode.getTableName();
            
            boolean retried = false;
            while (true) {
                List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize(2);
                ColumnResolver resolver = FromCompiler.getResolver(statement, connection);
                PTable table = resolver.getTables().get(0).getTable();
                if (logger.isDebugEnabled()) {
                    logger.debug("Resolved table to " + table.getName().getString() + " with seqNum " + table.getSequenceNumber() + " at timestamp " + table.getTimeStamp() + " with " + table.getColumns().size() + " columns: " + table.getColumns());
                }
                
                int position = table.getColumns().size();
                
                List<PColumn> currentPKs = table.getPKColumns();
                PColumn lastPK = currentPKs.get(currentPKs.size()-1);
                // Disallow adding columns if the last column is VARBIANRY.
                if (lastPK.getDataType() == PDataType.VARBINARY) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.VARBINARY_LAST_PK)
                        .setColumnName(lastPK.getName().getString()).build().buildException();
                }
                // Disallow adding columns if last column is fixed width and nullable.
                if (lastPK.isNullable() && lastPK.getDataType().isFixedWidth()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.NULLABLE_FIXED_WIDTH_LAST_PK)
                        .setColumnName(lastPK.getName().getString()).build().buildException();
                }
                          
                boolean isImmutableRows = table.isImmutableRows();
                Boolean isImmutableRowsProp = (Boolean)statement.getProps().remove(PTable.IS_IMMUTABLE_ROWS_PROP_NAME);
                if (isImmutableRowsProp != null) {
                    isImmutableRows = isImmutableRowsProp;
                }
                boolean multiTenant = table.isMultiTenant();
                Boolean multiTenantProp = (Boolean) statement.getProps().remove(PhoenixDatabaseMetaData.MULTI_TENANT);
                if (multiTenantProp != null) {
                    multiTenant = Boolean.TRUE.equals(multiTenantProp);
                }
                
                boolean disableWAL = Boolean.TRUE.equals(statement.getProps().remove(DISABLE_WAL));
                if (statement.getProps().get(PhoenixDatabaseMetaData.SALT_BUCKETS) != null) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.SALT_ONLY_ON_CREATE_TABLE)
                    .setTableName(table.getName().getString()).build().buildException();
                }
                if (statement.getProps().get(PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME) != null) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.DEFAULT_COLUMN_FAMILY_ONLY_ON_CREATE_TABLE)
                    .setTableName(table.getName().getString()).build().buildException();
                }
                
                boolean isAddingPKColumn = false;
                PreparedStatement colUpsert = connection.prepareStatement(INSERT_COLUMN);
                
                List<ColumnDef> columnDefs = statement.getColumnDefs();
                if (columnDefs == null) {                    
                    columnDefs = Lists.newArrayListWithExpectedSize(1);
                }

                List<Pair<byte[],Map<String,Object>>> families = Lists.newArrayList();                
                List<PColumn> columns = Lists.newArrayListWithExpectedSize(columnDefs.size());

                if ( columnDefs.size() > 0 ) {
                    for( ColumnDef colDef : columnDefs) {
                        if (colDef != null && !colDef.isNull()) {
                            if(colDef.isPK()) {
                                throw new SQLExceptionInfo.Builder(SQLExceptionCode.NOT_NULLABLE_COLUMN_IN_ROW_KEY)
                                .setColumnName(colDef.getColumnDefName().getColumnName()).build().buildException();
                            } else {
                                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ADD_NOT_NULLABLE_COLUMN)
                                .setColumnName(colDef.getColumnDefName().getColumnName()).build().buildException();
                            }
                        }                        
                        throwIfAlteringViewPK(colDef, table);
                        PColumn column = newColumn(position++, colDef, PrimaryKeyConstraint.EMPTY);
                        columns.add(column);
                        addColumnMutation(schemaName, tableName, column, colUpsert, null);

                        // TODO: support setting properties on other families?
                        if (column.getFamilyName() != null) {
                            families.add(new Pair<byte[],Map<String,Object>>(column.getFamilyName().getBytes(),statement.getProps()));
                        } else { // If adding to primary key, then add the same column to all indexes on the table
                            isAddingPKColumn = true;
                            for (PTable index : table.getIndexes()) {
                                int indexColPosition = index.getColumns().size();
                                PDataType indexColDataType = IndexUtil.getIndexColumnDataType(column);
                                ColumnName indexColName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(column));
                                ColumnDef indexColDef = FACTORY.columnDef(indexColName, indexColDataType.getSqlTypeName(), column.isNullable(), column.getMaxLength(), column.getScale(), true, column.getColumnModifier());
                                PColumn indexColumn = newColumn(indexColPosition, indexColDef, PrimaryKeyConstraint.EMPTY);
                                addColumnMutation(schemaName, index.getTableName().getString(), indexColumn, colUpsert, index.getParentTableName().getString());
                            }
                        }

                        tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                        connection.rollback();
                    }
                } else {
                 // Only support setting IMMUTABLE_ROWS=true and DISABLE_WAL=true on ALTER TABLE SET command
                    if (!statement.getProps().isEmpty()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.SET_UNSUPPORTED_PROP_ON_ALTER_TABLE)
                        .setTableName(table.getName().getString()).build().buildException();
                    }
                    // Check that HBase configured properly for mutable secondary indexing
                    // if we're changing from an immutable table to a mutable table and we
                    // have existing indexes.
                    if (isImmutableRowsProp != null && !isImmutableRows && table.isImmutableRows() && !table.getIndexes().isEmpty()) {
                        int hbaseVersion = connection.getQueryServices().getLowestClusterHBaseVersion();
                        if (hbaseVersion < PhoenixDatabaseMetaData.MUTABLE_SI_VERSION_THRESHOLD) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_MUTABLE_INDEXES).setSchemaName(schemaName).setTableName(tableName).build().buildException();
                        }
                        if (connection.getQueryServices().hasInvalidIndexConfiguration()) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_MUTABLE_INDEX_CONFIG).setSchemaName(schemaName).setTableName(tableName).build().buildException();
                        }
                    }
                    // TODO: if switching table to multiTenant or multiType, do some error checking
                }
                
                if (isAddingPKColumn && !table.getIndexes().isEmpty()) {
                    for (PTable index : table.getIndexes()) {
                        incrementTableSeqNum(index, index.getType(), 1);
                    }
                    tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                    connection.rollback();
                }
                long seqNum = incrementTableSeqNum(table, statement.getTableType(), isImmutableRows, disableWAL, multiTenant, 1);
                
                tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                connection.rollback();
                // Force the table header row to be first
                Collections.reverse(tableMetaData);
                
                Pair<byte[],Map<String,Object>> family = families.size() > 0 ? families.get(0) : null;
                
                // Figure out if the empty column family is changing as a result of adding the new column
                // The empty column family of an index will never change as a result of adding a new data column
                byte[] emptyCF = null;
                byte[] projectCF = null;
                if (table.getType() != PTableType.VIEW && family != null) {
                    if (table.getColumnFamilies().isEmpty()) {
                        emptyCF = family.getFirst();
                    } else {
                        try {
                            table.getColumnFamily(family.getFirst());
                        } catch (ColumnFamilyNotFoundException e) {
                            projectCF = family.getFirst();
                            emptyCF = SchemaUtil.getEmptyColumnFamily(table.getColumnFamilies());
                        }
                    }
                }
                MetaDataMutationResult result = connection.getQueryServices().addColumn(tableMetaData, statement.getTableType(), families);
                try {
                    MutationCode code = processMutationResult(schemaName, tableName, result);
                    if (code == MutationCode.COLUMN_ALREADY_EXISTS) {
                        connection.addTable(result.getTable());
                        if (!statement.ifNotExists()) {
                            throw new ColumnAlreadyExistsException(schemaName, tableName, SchemaUtil.findExistingColumn(result.getTable(), columns));
                        }
                        return new MutationState(0,connection);
                    }
                    // Only update client side cache if we aren't adding a PK column to a table with indexes.
                    // We could update the cache manually then too, it'd just be a pain.
                    if (!isAddingPKColumn || table.getIndexes().isEmpty()) {
                        connection.addColumn(SchemaUtil.getTableName(schemaName, tableName), columns, result.getMutationTime(), seqNum, isImmutableRows);
                    }
                    if (emptyCF != null) {
                        Long scn = connection.getSCN();
                        connection.setAutoCommit(true);
                        // Delete everything in the column. You'll still be able to do queries at earlier timestamps
                        long ts = (scn == null ? result.getMutationTime() : scn);
                        MutationPlan plan = new PostDDLCompiler(connection).compile(Collections.singletonList(new TableRef(null, table, ts, false)), emptyCF, projectCF, null, ts);
                        return connection.getQueryServices().updateData(plan);
                    }
                    return new MutationState(0,connection);
                } catch (ConcurrentTableMutationException e) {
                    if (retried) {
                        throw e;
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("Caught ConcurrentTableMutationException for table " + SchemaUtil.getTableName(schemaName, tableName) + ". Will try again...");
                    }
                    retried = true;
                }
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }


    private String dropColumnMutations(PTable table, List<PColumn> columnsToDrop, List<Mutation> tableMetaData) throws SQLException {
        String tenantId = connection.getTenantId() == null ? null : connection.getTenantId().getString();
        String schemaName = table.getSchemaName().getString();
        String tableName = table.getTableName().getString();
        String familyName = null;
        StringBuilder buf = new StringBuilder("DELETE FROM " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\" WHERE ");
        buf.append(TENANT_ID);
        if (tenantId == null || tenantId.length() == 0) {
            buf.append(" IS NULL AND ");
        } else {
            buf.append(" = ? AND ");
        }
        buf.append(TABLE_SCHEM_NAME);
        if (schemaName == null || schemaName.length() == 0) {
            buf.append(" IS NULL AND ");
        } else {
            buf.append(" = ? AND ");
        }
        buf.append (TABLE_NAME_NAME + " = ? AND " + COLUMN_NAME + " = ? AND " + TABLE_CAT_NAME);
        buf.append(" = ?");
        
        // TODO: when DeleteCompiler supports running an fully qualified IN query on the client-side,
        // we can use a single IN query here instead of executing a different query per column being dropped.
        PreparedStatement colDelete = connection.prepareStatement(buf.toString());
        try {
            for(PColumn columnToDrop : columnsToDrop) {
                int i = 1;
                if (tenantId != null && tenantId.length() > 0) {
                    colDelete.setString(i++, tenantId);
                }
                if (schemaName != null & schemaName.length() > 0) {
                    colDelete.setString(i++, schemaName);    
                }
                colDelete.setString(i++, tableName);
                colDelete.setString(i++, columnToDrop.getName().getString());
                colDelete.setString(i++, columnToDrop.getFamilyName() == null ? null : columnToDrop.getFamilyName().getString());
                colDelete.execute();
            }
        } finally {
            if(colDelete != null) {
                colDelete.close();
            }
        }
        
       Collections.sort(columnsToDrop,new Comparator<PColumn> () {
           @Override
            public int compare(PColumn left, PColumn right) {
               return Ints.compare(left.getPosition(), right.getPosition());
            }
        });
    
        int columnsToDropIndex = 0;
        PreparedStatement colUpdate = connection.prepareStatement(UPDATE_COLUMN_POSITION);
        colUpdate.setString(1, connection.getTenantId() == null ? null : connection.getTenantId().getString());
        colUpdate.setString(2, schemaName);
        colUpdate.setString(3, tableName);
        for (int i = columnsToDrop.get(columnsToDropIndex).getPosition() + 1; i < table.getColumns().size(); i++) {
            PColumn column = table.getColumns().get(i);
            if(columnsToDrop.contains(column)) {
                columnsToDropIndex++;
                continue;
            }
            colUpdate.setString(4, column.getName().getString());
            colUpdate.setString(5, column.getFamilyName() == null ? null : column.getFamilyName().getString());
            colUpdate.setInt(6, column.getPosition() - columnsToDropIndex);
            colUpdate.execute();
        }
       return familyName;
    }
    
    /**
     * Calculate what the new column family will be after the column is dropped, returning null
     * if unchanged.
     * @param table table containing column to drop
     * @param columnToDrop column being dropped
     * @return the new column family or null if unchanged.
     */
    private static byte[] getNewEmptyColumnFamilyOrNull (PTable table, PColumn columnToDrop) {
        if (table.getType() != PTableType.VIEW && !SchemaUtil.isPKColumn(columnToDrop) && table.getColumnFamilies().get(0).getName().equals(columnToDrop.getFamilyName()) && table.getColumnFamilies().get(0).getColumns().size() == 1) {
            return SchemaUtil.getEmptyColumnFamily(table.getColumnFamilies().subList(1, table.getColumnFamilies().size()));
        }
        // If unchanged, return null
        return null;
    }
    
    public MutationState dropColumn(DropColumnStatement statement) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);
            TableName tableNameNode = statement.getTable().getName();
            String schemaName = tableNameNode.getSchemaName();
            String tableName = tableNameNode.getTableName();
            String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
            boolean retried = false;
            while (true) {
                final ColumnResolver resolver = FromCompiler.getResolver(statement, connection);
                PTable table = resolver.getTables().get(0).getTable();
                List<ColumnName> columnRefs = statement.getColumnRefs();
                if(columnRefs == null) {
                    columnRefs = Lists.newArrayListWithCapacity(0);
                }
                TableRef tableRef = null;
                List<ColumnRef> columnsToDrop = Lists.newArrayListWithExpectedSize(columnRefs.size() + table.getIndexes().size());
                List<TableRef> indexesToDrop = Lists.newArrayListWithExpectedSize(table.getIndexes().size());
                List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize((table.getIndexes().size() + 1) * (1 + table.getColumns().size() - columnRefs.size()));
                List<PColumn>  tableColumnsToDrop = Lists.newArrayListWithExpectedSize(columnRefs.size());
                
                for(ColumnName column : columnRefs) {
                    ColumnRef columnRef = null;
                    try {
                        columnRef = resolver.resolveColumn(null, column.getFamilyName(), column.getColumnName());
                    } catch (ColumnNotFoundException e) {
                        if (statement.ifExists()) {
                            return new MutationState(0,connection);
                        }
                        throw e;
                    }
                    tableRef = columnRef.getTableRef();
                    PColumn columnToDrop = columnRef.getColumn();
                    tableColumnsToDrop.add(columnToDrop);
                    if (SchemaUtil.isPKColumn(columnToDrop)) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_DROP_PK)
                            .setColumnName(columnToDrop.getName().getString()).build().buildException();
                    }
                    columnsToDrop.add(new ColumnRef(tableRef, columnToDrop.getPosition()));
                }
                
                dropColumnMutations(table, tableColumnsToDrop, tableMetaData);
                for (PTable index : table.getIndexes()) {
                    List<PColumn> indexColumnsToDrop = Lists.newArrayListWithExpectedSize(columnRefs.size());
                    for(PColumn columnToDrop : tableColumnsToDrop) {
                        String indexColumnName = IndexUtil.getIndexColumnName(columnToDrop);
                        try {
                            PColumn indexColumn = index.getColumn(indexColumnName);
                            if (SchemaUtil.isPKColumn(indexColumn)) {
                                indexesToDrop.add(new TableRef(index));
                            } else {
                                indexColumnsToDrop.add(indexColumn);
                                columnsToDrop.add(new ColumnRef(tableRef, columnToDrop.getPosition()));
                            }
                        } catch (ColumnNotFoundException e) {
                        }
                    }
                    if(!indexColumnsToDrop.isEmpty()) {
                        incrementTableSeqNum(index, index.getType(), -1);
                        dropColumnMutations(index, indexColumnsToDrop, tableMetaData);
                    }
                    
                }
                tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                connection.rollback();
                
                long seqNum = incrementTableSeqNum(table, statement.getTableType(), -1);
                tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                connection.rollback();
                // Force table header to be first in list
                Collections.reverse(tableMetaData);
               
                /*
                 * Ensure our "empty column family to be" exists. Somewhat of an edge case, but can occur if we drop the last column
                 * in a column family that was the empty column family. In that case, we have to pick another one. If there are no other
                 * ones, then we need to create our default empty column family. Note that this may no longer be necessary once we
                 * support declaring what the empty column family is on a table, as:
                 * - If you declare it, we'd just ensure it's created at DDL time and never switch what it is unless you change it
                 * - If you don't declare it, we can just continue to use the old empty column family in this case, dynamically updating
                 *    the empty column family name on the PTable.
                 */
                for (ColumnRef columnRefToDrop : columnsToDrop) {
                    PTable tableContainingColumnToDrop = columnRefToDrop.getTable();
                    byte[] emptyCF = getNewEmptyColumnFamilyOrNull(tableContainingColumnToDrop, columnRefToDrop.getColumn());
                    if (emptyCF != null) {
                        try {
                            tableContainingColumnToDrop.getColumnFamily(emptyCF);
                        } catch (ColumnFamilyNotFoundException e) {
                            // Only if it's not already a column family do we need to ensure it's created
                            List<Pair<byte[],Map<String,Object>>> family = Lists.newArrayListWithExpectedSize(1);
                            family.add(new Pair<byte[],Map<String,Object>>(emptyCF,Collections.<String,Object>emptyMap()));
                            // Just use a Put without any key values as the Mutation, as addColumn will treat this specially
                            // TODO: pass through schema name and table name instead to these methods as it's cleaner
                            byte[] tenantIdBytes = connection.getTenantId() == null ? null : connection.getTenantId().getBytes();
                            if (tenantIdBytes == null) tenantIdBytes = ByteUtil.EMPTY_BYTE_ARRAY;
                            connection.getQueryServices().addColumn(
                                    Collections.<Mutation>singletonList(new Put(SchemaUtil.getTableKey
                                            (tenantIdBytes, tableContainingColumnToDrop.getSchemaName().getBytes(),
                                            tableContainingColumnToDrop.getTableName().getBytes()))),
                                    tableContainingColumnToDrop.getType(),family);
                        }
                    }
                }
                MetaDataMutationResult result = connection.getQueryServices().dropColumn(tableMetaData, statement.getTableType());
                try {
                    MutationCode code = processMutationResult(schemaName, tableName, result);
                    if (code == MutationCode.COLUMN_NOT_FOUND) {
                        connection.addTable(result.getTable());
                        if (!statement.ifExists()) {
                            throw new ColumnNotFoundException(schemaName, tableName, Bytes.toString(result.getFamilyName()), Bytes.toString(result.getColumnName()));
                        }
                        return new MutationState(0, connection);
                    }
                    // If we've done any index metadata updates, don't bother trying to update
                    // client-side cache as it would be too painful. Just let it pull it over from
                    // the server when needed.
                    if (columnsToDrop.size() > 0 && indexesToDrop.isEmpty()) {
                        for(PColumn columnToDrop : tableColumnsToDrop) {
                            connection.removeColumn(SchemaUtil.getTableName(schemaName, tableName), columnToDrop.getFamilyName().getString() , columnToDrop.getName().getString(), result.getMutationTime(), seqNum);
                        }
                    }
                    // If we have a VIEW, then only delete the metadata, and leave the table data alone
                    if (table.getType() != PTableType.VIEW) {
                        MutationState state = null;
                        connection.setAutoCommit(true);
                        Long scn = connection.getSCN();
                        // Delete everything in the column. You'll still be able to do queries at earlier timestamps
                        long ts = (scn == null ? result.getMutationTime() : scn);
                        PostDDLCompiler compiler = new PostDDLCompiler(connection);
                        boolean dropMetaData = connection.getQueryServices().getProps().getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
                        if(!dropMetaData){
                            // Drop any index tables that had the dropped column in the PK
                            connection.getQueryServices().updateData(compiler.compile(indexesToDrop, null, null, Collections.<PColumn>emptyList(), ts));
                        }
                        // Update empty key value column if necessary
                        for (ColumnRef droppedColumnRef : columnsToDrop) {
                            // Painful, but we need a TableRef with a pre-set timestamp to prevent attempts
                            // to get any updates from the region server.
                            // TODO: move this into PostDDLCompiler
                            // TODO: consider filtering mutable indexes here, but then the issue is that
                            // we'd need to force an update of the data row empty key value if a mutable
                            // secondary index is changing its empty key value family.
                            droppedColumnRef = new ColumnRef(droppedColumnRef, ts);
                            TableRef droppedColumnTableRef = droppedColumnRef.getTableRef();
                            PColumn droppedColumn = droppedColumnRef.getColumn();
                            MutationPlan plan = compiler.compile(
                                    Collections.singletonList(droppedColumnTableRef), 
                                    getNewEmptyColumnFamilyOrNull(droppedColumnTableRef.getTable(), droppedColumn), 
                                    null, 
                                    Collections.singletonList(droppedColumn), 
                                    ts);
                            state = connection.getQueryServices().updateData(plan);
                        }
                        // Return the last MutationState
                        return state;
                    }
                    return new MutationState(0, connection);
                } catch (ConcurrentTableMutationException e) {
                    if (retried) {
                        throw e;
                    }
                    table = connection.getPMetaData().getTable(fullTableName);
                    retried = true;
                }
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    public MutationState alterIndex(AlterIndexStatement statement) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            String dataTableName = statement.getTableName();
            String schemaName = statement.getTable().getName().getSchemaName();
            String indexName = statement.getTable().getName().getTableName();
            PIndexState newIndexState = statement.getIndexState();
            if (newIndexState == PIndexState.REBUILD) {
                newIndexState = PIndexState.BUILDING;
            }
            connection.setAutoCommit(false);
            // Confirm index table is valid and up-to-date
            TableRef indexRef = FromCompiler.getResolver(statement, connection).getTables().get(0);
            PreparedStatement tableUpsert = null;
            try {
                tableUpsert = connection.prepareStatement(UPDATE_INDEX_STATE);
                tableUpsert.setString(1, connection.getTenantId() == null ? null : connection.getTenantId().getString());
                tableUpsert.setString(2, schemaName);
                tableUpsert.setString(3, indexName);
                tableUpsert.setString(4, newIndexState.getSerializedValue());
                tableUpsert.execute();
            } finally {
                if(tableUpsert != null) {
                    tableUpsert.close();
                }
            }
            List<Mutation> tableMetadata = connection.getMutationState().toMutations().next().getSecond();
            connection.rollback();

            MetaDataMutationResult result = connection.getQueryServices().updateIndexState(tableMetadata, dataTableName);
            MutationCode code = result.getMutationCode();
            if (code == MutationCode.TABLE_NOT_FOUND) {
                throw new TableNotFoundException(schemaName,indexName);
            }
            if (code == MutationCode.UNALLOWED_TABLE_MUTATION) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_INDEX_STATE_TRANSITION)
                .setMessage(" currentState=" + indexRef.getTable().getIndexState() + ". requestedState=" + newIndexState )
                .setSchemaName(schemaName).setTableName(indexName).build().buildException();
            }
            if (code == MutationCode.TABLE_ALREADY_EXISTS) {
                if (result.getTable() != null) { // To accommodate connection-less update of index state
                    connection.addTable(result.getTable());
                }
            }
            if (newIndexState == PIndexState.BUILDING) {
                PTable index = indexRef.getTable();
                // First delete any existing rows of the index
                Long scn = connection.getSCN();
                long ts = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
                MutationPlan plan = new PostDDLCompiler(connection).compile(Collections.singletonList(indexRef), null, null, Collections.<PColumn>emptyList(), ts);
                connection.getQueryServices().updateData(plan);
                NamedTableNode dataTableNode = NamedTableNode.create(null, TableName.create(schemaName, dataTableName), Collections.<ColumnDef>emptyList());
                // Next rebuild the index
                if (connection.getSCN() != null) {
                    return buildIndexAtTimeStamp(index, dataTableNode);
                }
                TableRef dataTableRef = FromCompiler.getResolver(dataTableNode, connection).getTables().get(0);
                return buildIndex(index, dataTableRef);
            }
            return new MutationState(1, connection);
        } catch (TableNotFoundException e) {
            if (!statement.ifExists()) {
                throw e;
            }
            return new MutationState(0, connection);
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }
    
    private void throwIfAlteringViewPK(ColumnDef col, PTable table) throws SQLException {
        if (col != null && col.isPK() && table.getType() == PTableType.VIEW) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MODIFY_VIEW_PK)
            .setSchemaName(table.getSchemaName().getString())
            .setTableName(table.getTableName().getString())
            .setColumnName(col.getColumnDefName().getColumnName())
            .build().buildException();
        }
    }
}
