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

import org.apache.hadoop.hbase.*;
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
import com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.util.*;

public class MetaDataClient {
    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();
    private static final String CREATE_TABLE =
            "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"( " + 
            TABLE_SCHEM_NAME + "," +
            TABLE_NAME_NAME + "," +
            TABLE_TYPE_NAME + "," +
            TABLE_SEQ_NUM + "," +
            COLUMN_COUNT + "," +
            SALT_BUCKETS + "," +
            PK_NAME + "," +
            DATA_TABLE_NAME + "," +
            INDEX_STATE + "," +
            IMMUTABLE_ROWS +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String CREATE_INDEX_LINK =
            "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"( " +
            TABLE_SCHEM_NAME + "," +
            TABLE_NAME_NAME + "," +
            TABLE_CAT_NAME +
            ") VALUES (?, ?, ?)";
    private static final String INCREMENT_SEQ_NUM =
            "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"( " + 
            TABLE_SCHEM_NAME + "," +
            TABLE_NAME_NAME + "," +
            TABLE_SEQ_NUM  +
            ") VALUES (?, ?, ?)";
    private static final String MUTATE_TABLE =
        "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"( " + 
        TABLE_SCHEM_NAME + "," +
        TABLE_NAME_NAME + "," +
        TABLE_TYPE_NAME + "," +
        TABLE_SEQ_NUM + "," +
        COLUMN_COUNT + "," +
        IMMUTABLE_ROWS +
        ") VALUES (?, ?, ?, ?, ?, ?)";
    // For system table, don't set IMMUTABLE_ROWS, since at upgrade time
    // between 1.2 and 2.0, we'll add this column (and we don't yet have
    // the column while upgrading). 
    private static final String MUTATE_SYSTEM_TABLE =
            "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"( " + 
            TABLE_SCHEM_NAME + "," +
            TABLE_NAME_NAME + "," +
            TABLE_TYPE_NAME + "," +
            TABLE_SEQ_NUM + "," +
            COLUMN_COUNT + 
            ") VALUES (?, ?, ?, ?, ?)";
    private static final String UPDATE_INDEX_STATE =
            "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"( " + 
            TABLE_SCHEM_NAME + "," +
            TABLE_NAME_NAME + "," +
            INDEX_STATE +
            ") VALUES (?, ?, ?)";
    private static final String INSERT_COLUMN =
        "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"( " + 
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
        DATA_TABLE_NAME + // write this both in the column and table rows for access by metadata APIs
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    // Don't write DATA_TABLE_NAME for system table since at upgrade time from 1.2 to 2.0, we won't have this column yet.
    private static final String INSERT_SYSTEM_COLUMN =
            "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"( " + 
            TABLE_SCHEM_NAME + "," +
            TABLE_NAME_NAME + "," +
            COLUMN_NAME + "," +
            TABLE_CAT_NAME + "," +
            DATA_TYPE + "," +
            NULLABLE + "," +
            COLUMN_SIZE + "," +
            DECIMAL_DIGITS + "," +
            ORDINAL_POSITION + "," + 
            COLUMN_MODIFIER + 
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_COLUMN_POSITION =
        "UPSERT INTO " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\" ( " + 
        TABLE_SCHEM_NAME + "," +
        TABLE_NAME_NAME + "," +
        COLUMN_NAME + "," +
        TABLE_CAT_NAME + "," +
        ORDINAL_POSITION +
        ") VALUES (?, ?, ?, ?, ?)";
    
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
        // TODO: review - this seems weird
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


    private void addColumnMutation(String schemaName, String tableName, PColumn column, PreparedStatement colUpsert, String parentTableName, boolean isSalted) throws SQLException {
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
        if (column.getScale() == null) {
            colUpsert.setNull(8, Types.INTEGER);
        } else {
            colUpsert.setInt(8, column.getScale());
        }
        colUpsert.setInt(9, column.getPosition() + (isSalted ? 0 : 1));
        colUpsert.setInt(10, ColumnModifier.toSystemValue(column.getColumnModifier()));
        if (colUpsert.getParameterMetaData().getParameterCount() > 10) {
            colUpsert.setString(11, parentTableName);
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
                familyName = new PNameImpl(family);
            } else if (!isPK) {
                familyName = QueryConstants.DEFAULT_COLUMN_FAMILY_NAME;
            }
            
            PColumn column = new PColumnImpl(new PNameImpl(columnName), familyName, def.getDataType(),
                    def.getMaxLength(), def.getScale(), def.isNull(), position, columnModifier);
            return column;
        } catch (IllegalArgumentException e) { // Based on precondition check in constructor
            throw new SQLException(e);
        }
    }

    public MutationState createTable(CreateTableStatement statement, byte[][] splits) throws SQLException {
        
        PTable table = createTable(statement, splits, null);
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
        String schemaName = statement.getTableName().getSchemaName();
        PSchema schema = new PSchemaImpl(schemaName,ImmutableMap.<String,PTable>of(table.getName().getString(), table));
        TableRef tableRef = new TableRef(null, table, schema, ts, false);
        byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(table.getColumnFamilies());
        MutationPlan plan = compiler.compile(Collections.singletonList(tableRef), emptyCF, null, null, tableRef.getTimeStamp());
        return connection.getQueryServices().updateData(plan);
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
        String dataTableName = statement.getTable().getName().getTableName();
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
                if (!tableRef.getTable().isImmutableRows()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.INDEX_ONLY_ON_IMMUTABLE_TABLE).setSchemaName(tableRef.getSchema().getName())
                    .setTableName(tableRef.getTable().getName().getString()).build().buildException();
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
                    colName = SchemaUtil.isPKColumn(col) ? colName : ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(col));
                    allPkColumns.add(new Pair<ColumnName, ColumnModifier>(colName, pair.getSecond()));
                    columnDefs.add(FACTORY.columnDef(colName, dataType.getSqlTypeName(), col.isNullable(), col.getMaxLength(), col.getScale(), false, null));
                }
                
                // Next all the PK columns from the data table that aren't indexed
                if (!unusedPkColumns.isEmpty()) {
                    for (PColumn col : unusedPkColumns) {
                        ColumnName colName = ColumnName.caseSensitiveColumnName(col.getName().getString());
                        allPkColumns.add(new Pair<ColumnName, ColumnModifier>(colName, col.getColumnModifier()));
                        PDataType dataType = IndexUtil.getIndexColumnDataType(col);
                        columnDefs.add(FACTORY.columnDef(colName, dataType.getSqlTypeName(), col.isNullable(), col.getMaxLength(), col.getScale(), false, col.getColumnModifier()));
                    }
                    pk = FACTORY.primaryKey(null, allPkColumns);
                }
                
                // Last all the included columns (minus any PK columns)
                for (ColumnName colName : includedColumns) {
                    PColumn col = resolver.resolveColumn(null, colName.getFamilyName(), colName.getColumnName()).getColumn();
                    colName = SchemaUtil.isPKColumn(col) ? colName : ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(col));
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
                
                CreateTableStatement tableStatement = FACTORY.createTable(indexTableName, statement.getProps(), columnDefs, pk, statement.getSplitNodes(), PTableType.INDEX, statement.ifNotExists(), statement.getBindCount());
                table = createTable(tableStatement, splits, tableRef.getTable());
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
        boolean success = false;
        MetaDataClient client = this;
        SQLException sqlException = null;
        try {
            // If our connection is at a fixed point-in-time, we need to open a new
            // connection so that our new index table is visible.
            if (connection.getSCN() != null) {
                Properties props = new Properties(connection.getClientInfo());
                props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(connection.getSCN()+1));
                client = new MetaDataClient(DriverManager.getConnection(connection.getURL(), props).unwrap(PhoenixConnection.class));
                // Re-resolve the tableRef from the now newer connection
                ColumnResolver resolver = FromCompiler.getResolver(statement, client.connection);
                tableRef = resolver.getTables().get(0);
            }
            PostIndexDDLCompiler compiler = new PostIndexDDLCompiler(client.connection, tableRef);
            MutationPlan plan = compiler.compile(table);
            MutationState state = client.connection.getQueryServices().updateData(plan);
            AlterIndexStatement indexStatement = FACTORY.alterIndex(FACTORY.namedTable(null, indexTableName), dataTableName, false, PIndexState.ACTIVE);
            client.alterIndex(indexStatement);
            success = true;
            return state;
        } catch (SQLException e) {
            sqlException = e;
        } finally {
            // If we had to open another connection, then close it here
            if (client != this) {
                try {
                    client.connection.close();
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
            }
            if (sqlException != null) {
                throw sqlException;
            }
        }
        throw new IllegalStateException(); // impossible
    }

    private PTable createTable(CreateTableStatement statement, byte[][] splits, PTable parent) throws SQLException {
        PTableType tableType = statement.getTableType();
        boolean wasAutoCommit = connection.getAutoCommit();
        connection.rollback();
        try {
            connection.setAutoCommit(false);
            List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize(statement.getColumnDefs().size() + 3);
            
            TableName tableNameNode = statement.getTableName();
            String schemaName = tableNameNode.getSchemaName();
            String tableName = tableNameNode.getTableName();
            String parentTableName = null;
            if (parent != null) {
                parentTableName = parent.getName().getString();
                // Pass through data table sequence number so we can check it hasn't changed
                PreparedStatement incrementStatement = connection.prepareStatement(INCREMENT_SEQ_NUM);
                incrementStatement.setString(1, schemaName);
                incrementStatement.setString(2, parentTableName);
                incrementStatement.setLong(3, parent.getSequenceNumber());
                incrementStatement.execute();
                // Get list of mutations and add to table meta data that will be passed to server
                // to guarantee order. This row will always end up last
                tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                connection.rollback();

                // Add row linking from data table row to index table row
                PreparedStatement linkStatement = connection.prepareStatement(CREATE_INDEX_LINK);
                linkStatement.setString(1, schemaName);
                linkStatement.setString(2, parent.getName().getString());
                linkStatement.setString(3, tableName);
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
            
            List<ColumnDef> colDefs = statement.getColumnDefs();
            List<PColumn> columns = Lists.newArrayListWithExpectedSize(colDefs.size());
            List<PColumn> pkColumns = Lists.newArrayListWithExpectedSize(colDefs.size() + 1); // in case salted
            PreparedStatement colUpsert = connection.prepareStatement(INSERT_COLUMN);
            Map<String, PName> familyNames = Maps.newLinkedHashMap();
            boolean isPK = false;
            
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
            
            Integer saltBucketNum = (Integer) tableProps.remove(PhoenixDatabaseMetaData.SALT_BUCKETS);
            if (saltBucketNum != null && (saltBucketNum <= 0 || saltBucketNum > SaltingUtil.MAX_BUCKET_NUM)) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_BUCKET_NUM).build().buildException();
            }
            // Salt the index table if the data table is salted
            if (saltBucketNum == null && parent != null) {
                saltBucketNum = parent.getBucketNum();
            }
            boolean isSalted = (saltBucketNum != null);
            
            boolean isImmutableRows;
            Boolean isImmutableRowsProp = (Boolean) tableProps.remove(PTable.IS_IMMUTABLE_ROWS_PROP_NAME);
            if (isImmutableRowsProp == null) {
                isImmutableRows = connection.getQueryServices().getProps().getBoolean(QueryServices.IMMUTABLE_ROWS_ATTRIB, QueryServicesOptions.DEFAULT_IMMUTABLE_ROWS);
            } else {
                isImmutableRows = isImmutableRowsProp;
            }

            // Delay this check as it is supported to have IMMUTABLE_ROWS and SALT_BUCKETS defined on views
            if (statement.getTableType() == PTableType.VIEW && !tableProps.isEmpty()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.VIEW_WITH_PROPERTIES).build().buildException();
            }
            
            int position = 0;
            if (isSalted) {
                position = 1;
                pkColumns.add(SaltingUtil.SALTING_COLUMN);
            }
            
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
                    if (!pkColumnsNames.isEmpty() && !column.getName().getString().equals(pkColumnsIterator.next().getFirst().getColumnName())) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_OUT_OF_ORDER).setSchemaName(schemaName)
                            .setTableName(tableName).setColumnName(column.getName().getString()).build().buildException();
                    }
                    pkColumns.add(column);
                }
                columns.add(column);
                if (colDef.getDataType() == PDataType.VARBINARY 
                        && SchemaUtil.isPKColumn(column)
                        && pkColumnsNames.size() > 1 
                        && column.getPosition() < pkColumnsNames.size() - 1) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.VARBINARY_IN_ROW_KEY).setSchemaName(schemaName)
                        .setTableName(tableName).setColumnName(column.getName().getString()).build().buildException();
                }
                if (column.getFamilyName() != null) {
                    familyNames.put(column.getFamilyName().getString(),column.getFamilyName());
                }
            }
            if (!isPK && pkColumnsNames.isEmpty()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_MISSING)
                    .setSchemaName(schemaName).setTableName(tableName).build().buildException();
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
                PTable table = PTableImpl.makePTable(new PNameImpl(tableName), tableType, null, MetaDataProtocol.MIN_TABLE_TIMESTAMP, PTable.INITIAL_SEQ_NUM, QueryConstants.SYSTEM_TABLE_PK_NAME, null, columns, null, Collections.<PTable>emptyList(), isImmutableRows);
                connection.addTable(schemaName, table);
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
                    HTableDescriptor descriptor = connection.getQueryServices().getTableDescriptor(SchemaUtil.getTableName(schemaName, parent.getName().getString()));
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
                addColumnMutation(schemaName, tableName, column, colUpsert, parentTableName, isSalted);
            }
            
            tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
            connection.rollback();
            
            String dataTableName = parent == null ? null : parent.getName().getString();
            PIndexState indexState = parent == null ? null : PIndexState.BUILDING;
            PreparedStatement tableUpsert = connection.prepareStatement(CREATE_TABLE);
            tableUpsert.setString(1, schemaName);
            tableUpsert.setString(2, tableName);
            tableUpsert.setString(3, tableType.getSerializedValue());
            tableUpsert.setLong(4, PTable.INITIAL_SEQ_NUM);
            tableUpsert.setInt(5, position);
            if (saltBucketNum != null) {
                tableUpsert.setInt(6, saltBucketNum);
            } else {
                tableUpsert.setNull(6, Types.INTEGER);
            }
            tableUpsert.setString(7, pkName);
            tableUpsert.setString(8, dataTableName);
            tableUpsert.setString(9, indexState == null ? null : indexState.getSerializedValue());
            tableUpsert.setBoolean(10, isImmutableRows);
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
            MetaDataMutationResult result = connection.getQueryServices().createTable(tableMetaData, tableType, tableProps, familyPropList, splits);
            MutationCode code = result.getMutationCode();
            switch(code) {
            case TABLE_ALREADY_EXISTS:
                connection.addTable(schemaName, result.getTable());
                if (!statement.ifNotExists()) {
                    throw new TableAlreadyExistsException(schemaName, tableName, result.getTable());
                }
                return null;
            case PARENT_TABLE_NOT_FOUND:
                throw new TableNotFoundException(schemaName, parent.getName().getString());
            case NEWER_TABLE_FOUND:
                throw new NewerTableAlreadyExistsException(schemaName, tableName, result.getTable());
            case UNALLOWED_TABLE_MUTATION:
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MUTATE_TABLE)
                    .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            case CONCURRENT_TABLE_MUTATION:
                connection.addTable(schemaName, result.getTable());
                throw new ConcurrentTableMutationException(schemaName, tableName);
            default:
                PTable table =  PTableImpl.makePTable(
                        new PNameImpl(tableName), tableType, indexState, result.getMutationTime(), PTable.INITIAL_SEQ_NUM, 
                        pkName == null ? null : new PNameImpl(pkName), saltBucketNum, columns, dataTableName == null ? null : new PNameImpl(dataTableName), Collections.<PTable>emptyList(), isImmutableRows);
                connection.addTable(schemaName, table);
                return table;
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
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
            byte[] key = SchemaUtil.getTableKey(schemaName, tableName);
            Long scn = connection.getSCN();
            long clientTimeStamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
            List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize(2);
            @SuppressWarnings("deprecation") // FIXME: Remove when unintentionally deprecated method is fixed (HBASE-7870).
            Delete tableDelete = new Delete(key, clientTimeStamp, null);
            tableMetaData.add(tableDelete);
            if (parentTableName != null) {
                byte[] linkKey = MetaDataUtil.getParentLinkKey(schemaName, parentTableName, tableName);
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
                throw new NewerTableAlreadyExistsException(schemaName, tableName, result.getTable());
            case UNALLOWED_TABLE_MUTATION:
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MUTATE_TABLE)
                    .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            default:
                try {
                    // TODO: should we update the parent table by removing the index?
                    connection.removeTable(schemaName, tableName);
                } catch (TableNotFoundException e) { } // Ignore - just means wasn't cached
                if (result.getTable() != null && tableType != PTableType.VIEW) {
                    connection.setAutoCommit(true);
                    // Delete everything in the column. You'll still be able to do queries at earlier timestamps
                    long ts = (scn == null ? result.getMutationTime() : scn);
                    // Create empty table and schema - they're only used to get the name from
                    // PName name, PTableType type, long timeStamp, long sequenceNumber, List<PColumn> columns
                    PTable table = result.getTable();
                    Map<String,PTable>tables = Maps.newHashMapWithExpectedSize(1 + table.getIndexes().size());
                    tables.put(table.getName().getString(), table);
                    for (PTable index : table.getIndexes()) {
                        tables.put(index.getName().getString(), index);
                    }
                    PSchema schema = new PSchemaImpl(schemaName,tables);
                    List<TableRef> tableRefs = Lists.newArrayListWithExpectedSize(1 + table.getIndexes().size());
                    tableRefs.add(new TableRef(null, table, schema, ts, false));
                    for (PTable index: table.getIndexes()) {
                        tableRefs.add(new TableRef(null, index, schema, ts, false));
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
            throw new NewerTableAlreadyExistsException(schemaName, tableName, result.getTable());
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
            List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize(2);
            connection.setAutoCommit(false);
            TableName tableNameNode = statement.getTable().getName();
            String schemaName = tableNameNode.getSchemaName();
            String tableName = tableNameNode.getTableName();

            boolean retried = false;
            while (true) {
                ColumnResolver resolver = FromCompiler.getResolver(statement, connection);
                PTable table = resolver.getTables().get(0).getTable();
                PSchema schema = resolver.getTables().get(0).getSchema();
                
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
                
                List<PColumn> columns = Lists.newArrayListWithExpectedSize(1);
                ColumnDef colDef = statement.getColumnDef();
                if (colDef != null && !colDef.isNull() && colDef.isPK()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.NOT_NULLABLE_COLUMN_IN_ROW_KEY)
                        .setColumnName(colDef.getColumnDefName().getColumnName()).build().buildException();
                }
                
                if (statement.getProps().remove(PhoenixDatabaseMetaData.SALT_BUCKETS) != null) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.SALT_ONLY_ON_CREATE_TABLE).setSchemaName(schema.getName())
                    .setTableName(table.getName().getString()).build().buildException();
                }
                boolean isImmutableRows = table.isImmutableRows();
                Boolean isImmutableRowsProp = (Boolean)statement.getProps().remove(PTable.IS_IMMUTABLE_ROWS_PROP_NAME);
                if (isImmutableRowsProp != null) {
                    isImmutableRows = isImmutableRowsProp;
                }
                
                boolean isSalted = table.getBucketNum() != null;
                PreparedStatement colUpsert = connection.prepareStatement(SchemaUtil.isMetaTable(schemaName, tableName) ? INSERT_SYSTEM_COLUMN : INSERT_COLUMN);
                Pair<byte[],Map<String,Object>> family = null;
                if (colDef != null) {
                    PColumn column = newColumn(position, colDef, PrimaryKeyConstraint.EMPTY);
                    columns.add(column);
                    addColumnMutation(schemaName, tableName, column, colUpsert, null, isSalted);
                    // TODO: support setting properties on other families?
                    if (column.getFamilyName() != null) {
                        family = new Pair<byte[],Map<String,Object>>(column.getFamilyName().getBytes(),statement.getProps());
                    }
                    tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                    connection.rollback();
                } else {
                    // Only support setting IMMUTABLE_ROWS=true on ALTER TABLE SET command
                    if (!statement.getProps().isEmpty()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.SET_UNSUPPORTED_PROP_ON_ALTER_TABLE).setSchemaName(schema.getName())
                        .setTableName(table.getName().getString()).build().buildException();
                    }
                }
                
                // Ordinal position is 1-based and we don't count SALT column in ordinal position
                int totalColumnCount = position + (isSalted ? 0 : 1);
                final long seqNum = table.getSequenceNumber() + 1;
                PreparedStatement tableUpsert = connection.prepareStatement(SchemaUtil.isMetaTable(schemaName, tableName) ? MUTATE_SYSTEM_TABLE : MUTATE_TABLE);
                tableUpsert.setString(1, schemaName);
                tableUpsert.setString(2, tableName);
                tableUpsert.setString(3, table.getType().getSerializedValue());
                tableUpsert.setLong(4, seqNum);
                tableUpsert.setInt(5, totalColumnCount);
                if (tableUpsert.getParameterMetaData().getParameterCount() > 5) {
                    tableUpsert.setBoolean(6, isImmutableRows);
                }
                tableUpsert.execute();
                
                tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                connection.rollback();
                // Force the table header row to be first
                Collections.reverse(tableMetaData);
                
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
                    connection.addColumn(schemaName, tableName, columns, result.getMutationTime(), seqNum, isImmutableRows);
                    if (emptyCF != null) {
                        Long scn = connection.getSCN();
                        connection.setAutoCommit(true);
                        // Delete everything in the column. You'll still be able to do queries at earlier timestamps
                        long ts = (scn == null ? result.getMutationTime() : scn);
                        MutationPlan plan = new PostDDLCompiler(connection).compile(Collections.singletonList(new TableRef(null, table, schema, ts, false)), emptyCF, projectCF, null, ts);
                        return connection.getQueryServices().updateData(plan);
                    }
                    return new MutationState(0,connection);
                } catch (ConcurrentTableMutationException e) {
                    if (retried) {
                        throw e;
                    }
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
            TableName tableNameNode = statement.getTable().getName();
            String schemaName = tableNameNode.getSchemaName();
            String tableName = tableNameNode.getTableName();
            boolean retried = false;
            while (true) {
                final ColumnResolver resolver = FromCompiler.getResolver(statement, connection);
                PTable table = resolver.getTables().get(0).getTable();
                ColumnRef columnRef = null;
                try {
                    columnRef = resolver.resolveColumn(null, statement.getColumnRef().getFamilyName(), statement.getColumnRef().getColumnName());
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
                List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize(1 + table.getColumns().size() - columnToDrop.getPosition());
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
                tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                connection.rollback();
                
                final long seqNum = table.getSequenceNumber() + 1;
                PreparedStatement tableUpsert = connection.prepareStatement(MUTATE_TABLE);
                tableUpsert.setString(1, schemaName);
                tableUpsert.setString(2, tableName);
                tableUpsert.setString(3, table.getType().getSerializedValue());
                tableUpsert.setLong(4, seqNum);
                tableUpsert.setInt(5, columnCount);
                tableUpsert.setBoolean(6, table.isImmutableRows());
                tableUpsert.execute();
                
                tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                connection.rollback();
                // Force table header to be first in list
                Collections.reverse(tableMetaData);
                
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
                    connection.removeColumn(schemaName, tableName, familyName, columnToDrop.getName().getString(), result.getMutationTime(), seqNum);
                    // If we have a VIEW, then only delete the metadata, and leave the table data alone
                    if (table.getType() != PTableType.VIEW) {
                        connection.setAutoCommit(true);
                        Long scn = connection.getSCN();
                        // Delete everything in the column. You'll still be able to do queries at earlier timestamps
                        long ts = (scn == null ? result.getMutationTime() : scn);
                        MutationPlan plan = new PostDDLCompiler(connection).compile(Collections.singletonList(tableRef), emptyCF, null, Collections.singletonList(columnToDrop), ts);
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

    public MutationState alterIndex(AlterIndexStatement statement) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            String dataTableName = statement.getTableName();
            connection.setAutoCommit(false);
            final ColumnResolver resolver = FromCompiler.getResolver(statement, connection);
            TableRef indexTableRef = resolver.getTables().get(0);
            PTable index = indexTableRef.getTable();
            String schemaName = indexTableRef.getSchema().getName();
            String indexName = index.getName().getString();
            PreparedStatement tableUpsert = connection.prepareStatement(UPDATE_INDEX_STATE);
            tableUpsert.setString(1, schemaName);
            tableUpsert.setString(2, indexName);
            tableUpsert.setString(3, statement.getIndexState().getSerializedValue());
            tableUpsert.execute();
            List<Mutation> tableMetadata = connection.getMutationState().toMutations().next().getSecond();
            connection.rollback();

            MetaDataMutationResult result = connection.getQueryServices().updateIndexState(tableMetadata, dataTableName);
            MutationCode code = result.getMutationCode();
            if (code == MutationCode.TABLE_NOT_FOUND) {
                throw new TableNotFoundException(schemaName,indexName);
            }
            if (code == MutationCode.TABLE_ALREADY_EXISTS) {
                if (result.getTable() != null) { // To accommodate connection-less update of index state
                    connection.addTable(schemaName, result.getTable());
                }
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
}
