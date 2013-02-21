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
package com.salesforce.phoenix.schema;

import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.*;

import java.sql.*;
import java.util.*;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.*;
import com.salesforce.phoenix.compile.*;
import com.salesforce.phoenix.coprocessor.*;
import com.salesforce.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import com.salesforce.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.execute.MutationState;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.util.SchemaUtil;

public class MetaDataClient {
    private final PhoenixConnection connection;

    public MetaDataClient(PhoenixConnection connection) {
        this.connection = connection;
    }

    /**
     * Update the cache with the latest as of the connection scn.
     * @param schemaName
     * @param tableName
     * @return the timestamp from the server, negative if the cache was updated and positive otherwise
     * @throws SQLException
     */
    public long updateCache(String schemaName, String tableName) throws SQLException {
        Long scn = connection.getSCN();
        long clientTimeStamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        // TODO: better to check the type of the table
        if (TYPE_SCHEMA.equals(schemaName) && TYPE_TABLE.equals(tableName)) {
            return clientTimeStamp;
        }
        final byte[] schemaBytes = PDataType.VARCHAR.toBytes(schemaName);
        final byte[] tableBytes = PDataType.VARCHAR.toBytes(tableName);
        PTable table = null;
        long tableTimestamp = HConstants.LATEST_TIMESTAMP;
        try {
            table = connection.getPMetaData().getSchema(schemaName).getTable(tableName);
            tableTimestamp = table.getTimeStamp();
        } catch (SchemaNotFoundException e) {
            
        } catch (TableNotFoundException e) {
            
        }
        // Don't bother with server call: we can't possibly find a newer table
        if (tableTimestamp == clientTimeStamp - 1) {
            return clientTimeStamp;
        }
        MetaDataMutationResult result = connection.getQueryServices().getTable(schemaBytes, tableBytes, tableTimestamp, clientTimeStamp);
        MutationCode code = result.getMutationCode();
        PTable resultTable = result.getTable();
        // We found an updated table, so update our cache
        if (resultTable != null) {
            connection.addTable(schemaName, resultTable);
            return -result.getMutationTime();
        } else {
            // if (result.getMutationCode() == MutationCode.NEWER_TABLE_FOUND) {
            // TODO: No table exists at the clientTimestamp, but a newer one exists.
            // Since we disallow creation or modification of a table earlier than the latest
            // timestamp, we can handle this such that we don't ask the
            // server again.
            // If table was not found at the current time stamp and we have one cached, remove it.
            // Otherwise, we're up to date, so there's nothing to do.
            if (code == MutationCode.TABLE_NOT_FOUND && table != null) {
                connection.removeTable(schemaName, tableName);
                return -result.getMutationTime();
            }
        }
        return result.getMutationTime();
    }

    private static final String CREATE_TABLE =
        "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"( " + 
        TABLE_SCHEM_NAME + "," +
        TABLE_NAME_NAME + "," +
        TABLE_TYPE_NAME + "," +
        TABLE_SEQ_NUM + "," +
        COLUMN_COUNT + "," +
        PK_NAME + 
        ") VALUES (?, ?, ?, ?, ?, ?)";
    private static final String MUTATE_TABLE =
        "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"( " + 
        TABLE_SCHEM_NAME + "," +
        TABLE_NAME_NAME + "," +
        TABLE_TYPE_NAME + "," +
        TABLE_SEQ_NUM + "," +
        COLUMN_COUNT +
        ") VALUES (?, ?, ?, ?, ?)";
    private static final String INSERT_COLUMN =
        "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"( " + 
        TABLE_SCHEM_NAME + "," +
        TABLE_NAME_NAME + "," +
        COLUMN_NAME + "," +
        TABLE_CAT_NAME + "," +
        DATA_TYPE + "," +
        NULLABLE + "," +
        COLUMN_SIZE + "," +
        ORDINAL_POSITION +
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_COLUMN_POSITION =
        "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\" ( " + 
        TABLE_SCHEM_NAME + "," +
        TABLE_NAME_NAME + "," +
        COLUMN_NAME + "," +
        TABLE_CAT_NAME + "," +
        ORDINAL_POSITION +
        ") VALUES (?, ?, ?, ?, ?)";

    private void addColumnMutation(String schemaName, String tableName, PColumn column, PreparedStatement colUpsert) throws SQLException {
        colUpsert.setString(1, schemaName);
        colUpsert.setString(2, tableName);
        colUpsert.setString(3, column.getName().getString());
        colUpsert.setString(4, column.getFamilyName() == null ? null : column.getFamilyName().getString());
        colUpsert.setInt(5, column.getDataType().getSqlType());
        colUpsert.setInt(6, column.isNullable() ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls);
        if (column.getMaxLength() == null) {
            colUpsert.setNull(7, Types.INTEGER);
        } else {
            colUpsert.setInt(7, column.getMaxLength());
        }
        colUpsert.setInt(8, column.getPosition()+1);
        colUpsert.execute();
    }

    private PColumn newColumn(int position, ColumnDef def, Set<String> pkColumns) throws SQLException {
        try {
            String columnName = def.getColumnDefName().getColumnName().getName();
            PName familyName = null;
            if (def.isPK() && !pkColumns.isEmpty() ) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_ALREADY_EXISTS)
                    .setColumnName(columnName).build().buildException();
            }
            boolean isPK = def.isPK() || pkColumns.contains(columnName);
            if (def.getColumnDefName().getFamilyName() != null) {
                String family = def.getColumnDefName().getFamilyName().getName();
                if (isPK) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_WITH_FAMILY_NAME)
                        .setColumnName(columnName).setFamilyName(family).build().buildException();
                } else if (!def.isNull()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.KEY_VALUE_NOT_NULL)
                        .setColumnName(columnName).setFamilyName(family).build().buildException();
                }
                familyName = new PNameImpl(family);
            } else if (!isPK) {
                familyName = QueryConstants.DEFAULT_COLUMN_FAMILY_NAME;
            }
            PColumn column = new PColumnImpl(new PNameImpl(columnName), familyName,
                    def.getDataType(), def.getMaxLength(), def.isNull(), position);
            return column;
        } catch (IllegalArgumentException e) { // Based on precondition check in constructor
            throw new SQLException(e);
        }
    }

    public MutationState createTable(CreateTableStatement statement, byte[][] splits) throws SQLException {
        PTableType tableType = statement.getTableType();
        boolean isView = tableType == PTableType.VIEW;
        if (isView && !statement.getProps().isEmpty()) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.VIEW_WITH_TABLE_CONFIG).build().buildException();
        }
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);
            TableName tableNameNode = statement.getTableName();
            String schemaName = tableNameNode.getSchemaName();
            String tableName = tableNameNode.getTableName();
            
            PrimaryKeyConstraint pkConstraint = statement.getPrimaryKeyConstraint();
            String pkName = null;
            Set<String> pkColumns = Collections.<String>emptySet();
            Iterator<String> pkColumnsIterator = Iterators.emptyIterator();
            if (pkConstraint != null) {
                pkColumns = pkConstraint.getColumnNames();
                pkColumnsIterator = pkColumns.iterator();
                pkName = pkConstraint.getName();
            }
            
            List<ColumnDef> colDefs = statement.getColumnDefs();
            List<PColumn> columns = Lists.newArrayListWithExpectedSize(colDefs.size());
            PreparedStatement colUpsert = connection.prepareStatement(INSERT_COLUMN);
            int columnOrdinal = 0;
            Map<String, PName> familyNames = Maps.newLinkedHashMap();
            boolean isPK = false;
            for (ColumnDef colDef : colDefs) {
                if (colDef.isPK()) {
                    if (isPK) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_ALREADY_EXISTS)
                            .setColumnName(colDef.getColumnDefName().getColumnName().getName()).build().buildException();
                    }
                    isPK = true;
                }
                PColumn column = newColumn(columnOrdinal++,colDef,pkColumns);
                if (SchemaUtil.isPKColumn(column)) {
                    // TODO: remove this constraint
                    if (!pkColumns.isEmpty() && !column.getName().getString().equals(pkColumnsIterator.next())) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_OUT_OF_ORDER).setSchemaName(schemaName)
                            .setTableName(tableName).setColumnName(column.getName().getString()).build().buildException();
                    }
                }
                columns.add(column);
                if (colDef.getDataType() == PDataType.BINARY && colDefs.size() > 1) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.BINARY_IN_ROW_KEY).setSchemaName(schemaName)
                        .setTableName(tableName).setColumnName(column.getName().getString()).build().buildException();
                }
                if (column.getFamilyName() != null) {
                    familyNames.put(column.getFamilyName().getString(),column.getFamilyName());
                }
            }
            if (!isPK && pkColumns.isEmpty()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_MISSING)
                    .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            }
            
            List<Pair<byte[],Map<String,Object>>> familyPropList = Lists.newArrayListWithExpectedSize(familyNames.size());
            Map<String,Object> commonFamilyProps = Collections.emptyMap();
            Map<String,Object> tableProps = Collections.emptyMap();
            if (!statement.getProps().isEmpty()) {
                if (statement.isView()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.VIEW_WITH_PROPERTIES).build().buildException();
                }
                for (String familyName : statement.getProps().keySet()) {
                    if (!familyName.equals(QueryConstants.ALL_FAMILY_PROPERTIES_KEY)) {
                        if (familyNames.get(familyName) == null) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.PROPERTIES_FOR_FAMILY)
                                .setFamilyName(familyName).build().buildException();
                        }
                    }
                }
                commonFamilyProps = Maps.newHashMapWithExpectedSize(statement.getProps().size());
                tableProps = Maps.newHashMapWithExpectedSize(statement.getProps().size());
                
                Collection<Pair<String,Object>> props = statement.getProps().get(QueryConstants.ALL_FAMILY_PROPERTIES_KEY);
                // Somewhat hacky way of determining if property is for HColumnDescriptor or HTableDescriptor
                HColumnDescriptor defaultDescriptor = new HColumnDescriptor(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES);
                for (Pair<String,Object> prop : props) {
                    if (defaultDescriptor.getValue(prop.getFirst()) != null) {
                        commonFamilyProps.put(prop.getFirst(), prop.getSecond());
                    } else {
                        tableProps.put(prop.getFirst(), prop.getSecond());
                    }
                }
            }
                
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
            if (tableType == PTableType.SYSTEM) {
                PTable table = new PTableImpl(new PNameImpl(tableName), tableType, MetaDataProtocol.MIN_TABLE_TIMESTAMP, 0, QueryConstants.SYSTEM_TABLE_PK_NAME, columns);
                connection.addTable(schemaName, table);
            }
            
            for (PColumn column : columns) {
                addColumnMutation(schemaName, tableName, column, colUpsert);
            }
            
            PreparedStatement tableUpsert = connection.prepareStatement(CREATE_TABLE);
            tableUpsert.setString(1, schemaName);
            tableUpsert.setString(2, tableName);
            tableUpsert.setString(3, tableType.getSerializedValue());
            tableUpsert.setInt(4, 0);
            tableUpsert.setInt(5, columnOrdinal);
            tableUpsert.setString(6, pkName);
            tableUpsert.execute();
            
            final List<Mutation> tableMetaData = connection.getMutationState().toMutations();
            connection.rollback();
            
            MetaDataMutationResult result = connection.getQueryServices().createTable(tableMetaData, isView, tableProps, familyPropList, splits);
            MutationCode code = result.getMutationCode();
            switch(code) {
            case TABLE_ALREADY_EXISTS:
                connection.addTable(schemaName, result.getTable());
                if (!statement.ifNotExists()) {
                    throw new TableAlreadyExistsException(schemaName, tableName);
                }
                break;
            case NEWER_TABLE_FOUND:
                // TODO: add table if in result?
                throw new NewerTableAlreadyExistsException(schemaName, tableName);
            case UNALLOWED_TABLE_MUTATION:
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MUTATE_TABLE)
                    .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            default:
                PTable table = new PTableImpl(new PNameImpl(tableName), tableType, result.getMutationTime(), 0, pkName, columns);
                connection.addTable(schemaName, table);
                if (tableType == PTableType.USER) {
                    connection.setAutoCommit(true);
                    // Delete everything in the column. You'll still be able to do queries at earlier timestamps
                    Long scn = connection.getSCN();
                    long ts = (scn == null ? result.getMutationTime() : scn);
                    PSchema schema = new PSchemaImpl(schemaName,ImmutableMap.<String,PTable>of(table.getName().getString(), table));
                    TableRef tableRef = new TableRef(null, table, schema, ts);
                    byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(table.getColumnFamilies());
                    MutationPlan plan = new PostDDLCompiler(connection).compile(tableRef, emptyCF, null, ts);
                    return connection.getQueryServices().updateData(plan);
                }
                break;
            }
            return new MutationState(0,connection);
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    public MutationState dropTable(DropTableStatement statement) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            TableName tableNameNode = statement.getTableName();
            String schemaName = tableNameNode.getSchemaName();
            String tableName = tableNameNode.getTableName();
            byte[] key = SchemaUtil.getTableKey(schemaName, tableName);
            Long scn = connection.getSCN();
            @SuppressWarnings("deprecation") // FIXME: Remove when unintentionally deprecated method is fixed (HBASE-7870).
            List<Mutation> tableMetaData = Collections.<Mutation>singletonList(new Delete(key, scn == null ? HConstants.LATEST_TIMESTAMP : scn));
            MetaDataMutationResult result = connection.getQueryServices().dropTable(tableMetaData, statement.isView());
            MutationCode code = result.getMutationCode();
            switch(code) {
            case TABLE_NOT_FOUND:
                if (!statement.ifExists()) {
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
                    connection.removeTable(schemaName, tableName);
                } catch (TableNotFoundException e) { // Ignore - just means wasn't cached
                }
                if (!statement.isView()) {
                    connection.setAutoCommit(true);
                    // Delete everything in the column. You'll still be able to do queries at earlier timestamps
                    long ts = (scn == null ? result.getMutationTime() : scn);
                    // Create empty table and schema - they're only used to get the name from
                    // PName name, PTableType type, long timeStamp, long sequenceNumber, List<PColumn> columns
                    PTable table = result.getTable();
                    PSchema schema = new PSchemaImpl(schemaName,ImmutableMap.<String,PTable>of(table.getName().getString(), table));
                    TableRef tableRef = new TableRef(null, table, schema, ts);
                    MutationPlan plan = new PostDDLCompiler(connection).compile(tableRef, null, Collections.<PColumn>emptyList(), ts);
                    return connection.getQueryServices().updateData(plan);
                }
                break;
            }
            return new MutationState(0,connection);
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    private PTable getLatestTable(String schemaName, String tableName) throws SQLException {
        boolean retried = false;
        PTable table = null;
        while (true) {
            try {
                table = connection.getPMetaData().getSchema(schemaName).getTable(tableName);
            } catch (TableNotFoundException e) {
                if (!retried) {
                    retried = true;
                    if (this.updateCache(schemaName, tableName) < 0) {
                        continue;
                    }
                }
            }
            break;
        }
        if (table == null) {
            throw new TableNotFoundException(schemaName, tableName);
        }
        return table;
    }

    private MutationCode processMutationResult(String schemaName, String tableName, MetaDataMutationResult result) throws SQLException {
        final MutationCode mutationCode = result.getMutationCode();
        switch (mutationCode) {
        case TABLE_NOT_FOUND:
            connection.removeTable(schemaName, tableName);
            throw new TableNotFoundException(schemaName, tableName);
        case UNALLOWED_TABLE_MUTATION:
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MUTATE_TABLE)
                .setSchemaName(schemaName).setTableName(tableName).build().buildException();
        case COLUMN_ALREADY_EXISTS:
        case COLUMN_NOT_FOUND:
            break;
        case CONCURRENT_TABLE_MUTATION:
            connection.addTable(schemaName, result.getTable());
            throw new ConcurrentTableMutationException(schemaName, tableName);
        case NEWER_TABLE_FOUND:
            if (result.getTable() != null) {
                connection.addTable(schemaName, result.getTable());
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

    public MutationState addColumn(AddColumnStatement statement) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);
            TableName tableNameNode = statement.getTableName();
            String schemaName = tableNameNode.getSchemaName();
            String tableName = tableNameNode.getTableName();
            
            PTable table = getLatestTable(schemaName, tableName);
            PSchema schema = connection.getPMetaData().getSchema(schemaName);
            boolean retried = false;
            while (true) {
                int ordinalPosition = table.getColumns().size();
                    
                List<PColumn> columns = Lists.newArrayListWithExpectedSize(1);
                ColumnDef colDef = statement.getColumnDef();
                if (!colDef.isNull() && colDef.isPK()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.NOT_NULLABLE_COLUMN_IN_ROW_KEY)
                        .setColumnName(colDef.getColumnDefName().getColumnName().getName()).build().buildException();
                }
                
                PreparedStatement colUpsert = connection.prepareStatement(INSERT_COLUMN);
                Pair<byte[],Map<String,Object>> family = null;
                PColumn column = newColumn(ordinalPosition++,colDef,Collections.<String>emptySet());
                addColumnMutation(schemaName, tableName, column, colUpsert);
                columns.add(column);
                if (column.getFamilyName() != null) {
                    family = new Pair<byte[],Map<String,Object>>(column.getFamilyName().getBytes(),statement.getProps());
                }
                final long seqNum = table.getSequenceNumber() + 1;
                PreparedStatement tableUpsert = connection.prepareStatement(MUTATE_TABLE);
                tableUpsert.setString(1, schemaName);
                tableUpsert.setString(2, tableName);
                tableUpsert.setString(3, table.getType().getSerializedValue());
                tableUpsert.setLong(4, seqNum);
                tableUpsert.setInt(5, ordinalPosition);
                tableUpsert.execute();
                
                final List<Mutation> tableMetaData = connection.getMutationState().toMutations();
                connection.rollback();
                byte[] emptyCF = null;
                if (table.getType() != PTableType.VIEW && family != null && table.getColumnFamilies().isEmpty()) {
                    emptyCF = family.getFirst();
                }
                MetaDataMutationResult result = connection.getQueryServices().addColumn(tableMetaData, table.getType() == PTableType.VIEW, family);
                try {
                    MutationCode code = processMutationResult(schemaName, tableName, result);
                    if (code == MutationCode.COLUMN_ALREADY_EXISTS) {
                        connection.addTable(schemaName, result.getTable());
                        if (!statement.ifNotExists()) {
                            throw new ColumnAlreadyExistsException(schemaName, tableName, SchemaUtil.findExistingColumn(result.getTable(), columns));
                        }
                        return new MutationState(0,connection);
                    }
                    connection.addColumn(schemaName, tableName, columns, seqNum, result.getMutationTime());
                    if (emptyCF != null) {
                        Long scn = connection.getSCN();
                        connection.setAutoCommit(true);
                        // Delete everything in the column. You'll still be able to do queries at earlier timestamps
                        long ts = (scn == null ? result.getMutationTime() : scn);
                        TableRef tableRef = new TableRef(null, table, schema, ts);
                        MutationPlan plan = new PostDDLCompiler(connection).compile(tableRef, emptyCF, null, ts);
                        return connection.getQueryServices().updateData(plan);
                    }
                    return new MutationState(0,connection);
                } catch (ConcurrentTableMutationException e) {
                    if (retried) {
                        throw e;
                    }
                    table = connection.getPMetaData().getSchema(schemaName).getTable(tableName);
                    retried = true;
                }
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    public MutationState dropColumn(DropColumnStatement statement) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);
            TableName tableNameNode = statement.getTableName();
            String schemaName = tableNameNode.getSchemaName();
            String tableName = tableNameNode.getTableName();
            PTable table = getLatestTable(schemaName, tableName); // TODO: Do in resolver?
            boolean retried = false;
            while (true) {
                final ColumnResolver resolver = FromCompiler.getResolver(statement, connection);
                ColumnRef columnRef = null;
                try {
                    columnRef = resolver.resolveColumn((ColumnParseNode)statement.getColumnRef());
                } catch (ColumnNotFoundException e) {
                    if (statement.ifExists()) {
                        return new MutationState(0,connection);
                    }
                    throw e;
                }
                TableRef tableRef = columnRef.getTableRef();
                PColumn columnToDrop = columnRef.getColumn();
                if (SchemaUtil.isPKColumn(columnToDrop)) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_DROP_PK)
                        .setColumnName(columnToDrop.getName().getString()).build().buildException();
                }
                int columnCount = table.getColumns().size() - 1;
                String familyName = null;
                List<String> binds = Lists.newArrayListWithExpectedSize(4);
                StringBuilder buf = new StringBuilder("DELETE FROM " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\" WHERE " + TABLE_SCHEM_NAME);
                if (schemaName == null || schemaName.length() == 0) {
                    buf.append(" IS NULL AND ");
                } else {
                    buf.append(" = ? AND ");
                    binds.add(schemaName);
                }
                buf.append (TABLE_NAME_NAME + " = ? AND " + COLUMN_NAME + " = ? AND " + TABLE_CAT_NAME);
                binds.add(tableName);
                binds.add(columnToDrop.getName().getString());
                if (columnToDrop.getFamilyName() == null) {
                    buf.append(" IS NULL");
                } else {
                    buf.append(" = ?");
                    binds.add(familyName = columnToDrop.getFamilyName().getString());
                }
                
                PreparedStatement colDelete = connection.prepareStatement(buf.toString());
                for (int i = 0; i < binds.size(); i++) {
                    colDelete.setString(i+1, binds.get(i));
                }
                colDelete.execute();
                
                PreparedStatement colUpdate = connection.prepareStatement(UPDATE_COLUMN_POSITION);
                colUpdate.setString(1, schemaName);
                colUpdate.setString(2, tableName);
                for (int i = columnToDrop.getPosition() + 1; i < table.getColumns().size(); i++) {
                    PColumn column = table.getColumns().get(i);
                    colUpdate.setString(3, column.getName().getString());
                    colUpdate.setString(4, column.getFamilyName() == null ? null : column.getFamilyName().getString());
                    colUpdate.setInt(5, i);
                    colUpdate.execute();
                }
                final long seqNum = table.getSequenceNumber() + 1;
                PreparedStatement tableUpsert = connection.prepareStatement(MUTATE_TABLE);
                tableUpsert.setString(1, schemaName);
                tableUpsert.setString(2, tableName);
                tableUpsert.setString(3, table.getType().getSerializedValue());
                tableUpsert.setLong(4, seqNum);
                tableUpsert.setInt(5, columnCount);
                tableUpsert.execute();
                
                final List<Mutation> tableMetaData = connection.getMutationState().toMutations();
                connection.rollback();
                // If we're dropping the last KV colum, we have to pass an indication along to the dropColumn call
                // to populate a new empty KV column
                byte[] emptyCF = null;
                if (table.getType() != PTableType.VIEW && !SchemaUtil.isPKColumn(columnToDrop) && table.getColumnFamilies().get(0).getName().equals(columnToDrop.getFamilyName()) && table.getColumnFamilies().get(0).getColumns().size() == 1) {
                    emptyCF = SchemaUtil.getEmptyColumnFamily(table.getColumnFamilies().subList(1, table.getColumnFamilies().size()));
                }
                MetaDataMutationResult result = connection.getQueryServices().dropColumn(tableMetaData, emptyCF != null && Bytes.compareTo(emptyCF, QueryConstants.EMPTY_COLUMN_BYTES)==0 ? emptyCF : null);
                try {
                    MutationCode code = processMutationResult(schemaName, tableName, result);
                    if (code == MutationCode.COLUMN_NOT_FOUND) {
                        connection.addTable(schemaName, result.getTable());
                        if (!statement.ifExists()) {
                            throw new ColumnNotFoundException(schemaName, tableName, familyName, columnToDrop.getName().getString());
                        }
                        return new MutationState(0, connection);
                    }
                    connection.removeColumn(schemaName, tableName, familyName, columnToDrop.getName().getString(), seqNum, result.getMutationTime());
                    // If we have a VIEW, then only delete the metadata, and leave the table data alone
                    if (table.getType() != PTableType.VIEW) {
                        connection.setAutoCommit(true);
                        Long scn = connection.getSCN();
                        // Delete everything in the column. You'll still be able to do queries at earlier timestamps
                        long ts = (scn == null ? result.getMutationTime() : scn);
                        MutationPlan plan = new PostDDLCompiler(connection).compile(tableRef, emptyCF, Collections.singletonList(columnToDrop), ts);
                        return connection.getQueryServices().updateData(plan);
                    }
                    return new MutationState(0, connection);
                } catch (ConcurrentTableMutationException e) {
                    if (retried) {
                        throw e;
                    }
                    table = connection.getPMetaData().getSchema(schemaName).getTable(tableName);
                    retried = true;
                }
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }
}
