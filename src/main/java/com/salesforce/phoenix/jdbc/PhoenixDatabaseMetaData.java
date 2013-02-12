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
package com.salesforce.phoenix.jdbc;

import java.sql.*;
import java.util.*;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;
import com.salesforce.phoenix.compile.*;
import com.salesforce.phoenix.coprocessor.MetaDataProtocol;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.expression.BaseTerminalExpression;
import com.salesforce.phoenix.expression.RowKeyColumnExpression;
import com.salesforce.phoenix.expression.function.SqlTypeNameFunction;
import com.salesforce.phoenix.iterate.*;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.query.Scanner;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.schema.tuple.SingleKeyValueTuple;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.*;


/**
 * 
 * JDBC DatabaseMetaData implementation of Phoenix reflecting read-only nature of driver.
 * Supported metadata methods include:
 * {@link #getTables(String, String, String, String[])}
 * Other ResultSet methods return an empty result set.
 * 
 * @author jtaylor
 * @since 0.1
 */
public class PhoenixDatabaseMetaData implements DatabaseMetaData, com.salesforce.phoenix.jdbc.Jdbc7Shim.DatabaseMetaData {
    public static final int FAMILY_NAME_INDEX = 3;
    public static final int COLUMN_NAME_INDEX = 2;
    public static final int TABLE_NAME_INDEX = 1;
    public static final int SCHEMA_NAME_INDEX = 0;

    public static final String TYPE_SCHEMA = "SYSTEM";
    public static final String TYPE_TABLE = "TABLE";
    public static final String TYPE_SCHEMA_AND_TABLE = TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"";
    public static final byte[] TYPE_TABLE_BYTES = TYPE_TABLE.getBytes();
    public static final byte[] TYPE_SCHEMA_BYTES = TYPE_SCHEMA.getBytes();
    public static final byte[] TYPE_TABLE_NAME = SchemaUtil.getTableName(TYPE_SCHEMA_BYTES, TYPE_TABLE_BYTES);
    
    public static final String TABLE_NAME_NAME = "TABLE_NAME";
    public static final String TABLE_TYPE_NAME = "TABLE_TYPE";
    public static final byte[] TABLE_TYPE_BYTES = Bytes.toBytes(TABLE_TYPE_NAME);
    
    public static final String TABLE_CAT_NAME = "TABLE_CAT";
    public static final String TABLE_CATALOG_NAME = "TABLE_CATALOG";
    public static final String TABLE_SCHEM_NAME = "TABLE_SCHEM";
    public static final String REMARKS_NAME = "REMARKS";
    public static final String TYPE_CAT_NAME = "TYPE_CAT";
    public static final String TYPE_SCHEM_NAME = "TYPE_SCHEM";
    public static final String TYPE_NAME_NAME = "TYPE_NAME";
    public static final String SELF_REFERENCING_COL_NAME_NAME = "SELF_REFERENCING_COL_NAME";
    public static final String REF_GENERATION_NAME = "REF_GENERATION";
    public static final String PK_NAME = "PK_NAME";
    public static final byte[] PK_NAME_BYTES = Bytes.toBytes(PK_NAME);
    public static final String TABLE_SEQ_NUM = "TABLE_SEQ_NUM";
    public static final byte[] TABLE_SEQ_NUM_BYTES = Bytes.toBytes(TABLE_SEQ_NUM);
    public static final String COLUMN_COUNT = "COLUMN_COUNT";
    public static final byte[] COLUMN_COUNT_BYTES = Bytes.toBytes(COLUMN_COUNT);
    
    public static final String COLUMN_NAME = "COLUMN_NAME";
    public static final String DATA_TYPE = "DATA_TYPE";
    public static final String TYPE_NAME = "TYPE_NAME";
    public static final String COLUMN_SIZE = "COLUMN_SIZE";
    public static final String BUFFER_LENGTH = "BUFFER_LENGTH";
    public static final String DECIMAL_DIGITS = "DECIMAL_DIGITS";
    public static final String NUM_PREC_RADIX = "NUM_PREC_RADIX";
    public static final String NULLABLE = "NULLABLE";
    public static final String COLUMN_DEF = "COLUMN_DEF";
    public static final String SQL_DATA_TYPE = "SQL_DATA_TYPE";
    public static final String SQL_DATETIME_SUB = "SQL_DATETIME_SUB";
    public static final String CHAR_OCTET_LENGTH = "CHAR_OCTET_LENGTH";
    public static final String ORDINAL_POSITION = "ORDINAL_POSITION";
    public static final String IS_NULLABLE = "IS_NULLABLE";
    public static final String SCOPE_CATALOG = "SCOPE_CATALOG";
    public static final String SCOPE_SCHEMA = "SCOPE_SCHEMA";
    public static final String SCOPE_TABLE = "SCOPE_TABLE";
    public static final String SOURCE_DATA_TYPE = "SOURCE_DATA_TYPE";
    public static final String IS_AUTOINCREMENT = "IS_AUTOINCREMENT";

    public static final String TABLE_FAMILY = QueryConstants.DEFAULT_COLUMN_FAMILY_NAME.getString();
    public static final byte[] TABLE_FAMILY_BYTES = QueryConstants.DEFAULT_COLUMN_FAMILY_NAME.getBytes();
    
    private static final int ROW_LIMIT = 1000000;
    private static final Scanner EMPTY_SCANNER = new WrappedScanner(new MaterializedResultIterator(Collections.<Tuple>emptyList()), new RowProjector(Collections.<ColumnProjector>emptyList()));
    
    private final PhoenixConnection connection;
    private final ResultSet emptyResultSet;
    
    PhoenixDatabaseMetaData(PhoenixConnection connection) throws SQLException {
        this.emptyResultSet = new PhoenixResultSet(EMPTY_SCANNER, new PhoenixStatement(connection));
        this.connection = connection;
    }
    
    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        return emptyResultSet;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "Catalog";
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return emptyResultSet;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return emptyResultSet;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException {
        return emptyResultSet;
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        StringBuilder buf = new StringBuilder("select " + 
                TABLE_CAT_NAME + "," + // use this column for column family name
                TABLE_SCHEM_NAME + "," +
                TABLE_NAME_NAME + " ," +
                COLUMN_NAME + "," +
                DATA_TYPE + "," +
                SqlTypeNameFunction.NAME + "(" + DATA_TYPE + ") AS " + TYPE_NAME + "," +
                COLUMN_SIZE + "," +
                BUFFER_LENGTH + "," +
                DECIMAL_DIGITS + "," +
                NUM_PREC_RADIX + "," +
                NULLABLE + "," +
                COLUMN_DEF + "," +
                SQL_DATA_TYPE + "," +
                SQL_DATETIME_SUB + "," +
                CHAR_OCTET_LENGTH + "," +
                ORDINAL_POSITION + "," +
                "CASE " + NULLABLE + " WHEN " + DatabaseMetaData.attributeNoNulls +  " THEN '" + Boolean.FALSE.toString() + "' WHEN " + DatabaseMetaData.attributeNullable + " THEN '" + Boolean.TRUE.toString() + "' END AS " + IS_NULLABLE + "," +
                SCOPE_CATALOG + "," +
                SCOPE_SCHEMA + "," +
                SCOPE_TABLE + "," +
                SOURCE_DATA_TYPE + "," +
                IS_AUTOINCREMENT +
                " from " + TYPE_SCHEMA_AND_TABLE + 
                " where");
        String conjunction = " ";
        if (schemaPattern != null) {
            buf.append(conjunction + TABLE_SCHEM_NAME + (schemaPattern.length() == 0 ? " is null" : " like '" + SchemaUtil.normalizeIdentifier(schemaPattern) + "'" ));
            conjunction = " and ";
        }
        if (tableNamePattern != null) {
            buf.append(conjunction + TABLE_NAME_NAME + " like '" + SchemaUtil.normalizeIdentifier(tableNamePattern) + "'" );
            conjunction = " and ";
        }
        if (catalog != null) { // if null, will pick up all columns
            if (catalog.length() == 0) { // will pick up only PK columns
                buf.append(conjunction + TABLE_CAT_NAME + " is null");
            } else { // will pick up only KV columns
                buf.append(conjunction + TABLE_CAT_NAME + " like '" + SchemaUtil.normalizeIdentifier(catalog) + "'" );
            }
            conjunction = " and ";
        }
        if (columnNamePattern != null) {
            buf.append(conjunction + COLUMN_NAME + " like '" + SchemaUtil.normalizeIdentifier(columnNamePattern) + "'" );
        } else {
            buf.append(conjunction + COLUMN_NAME + " is not null" );
        }
        buf.append(" order by " + TABLE_SCHEM_NAME + "," + TABLE_NAME_NAME + "," + ORDINAL_POSITION);
        buf.append(" limit " + ROW_LIMIT);
        Statement stmt = connection.createStatement();
        return stmt.executeQuery(buf.toString());
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
            String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "Phoenix";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return "1.0";
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return connection.getTransactionIsolation();
    }

    @Override
    public int getDriverMajorVersion() {
        return Integer.parseInt(connection.getClientInfo(PhoenixEmbeddedDriver.MAJOR_VERSION_PROP));
    }

    @Override
    public int getDriverMinorVersion() {
        return Integer.parseInt(connection.getClientInfo(PhoenixEmbeddedDriver.MINOR_VERSION_PROP));
    }

    @Override
    public String getDriverName() throws SQLException {
        return connection.getClientInfo(PhoenixEmbeddedDriver.DRIVER_NAME_PROP);
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return connection.getClientInfo(PhoenixEmbeddedDriver.MAJOR_VERSION_PROP) + "." + connection.getClientInfo(PhoenixEmbeddedDriver.MINOR_VERSION_PROP);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
            String columnNamePattern) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "'";
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        return emptyResultSet;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 4000;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 200;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 1;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 1;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        // Catalogs are not supported for schemas
        if (catalog != null && catalog.length() > 0) {
            return emptyResultSet;
        }
        if (table == null || table.length() == 0) {
            return emptyResultSet;
        }
        final int keySeqPosition = 4;
        final int pkNamePosition = 5;
        StringBuilder buf = new StringBuilder("select " + 
                TABLE_CAT_NAME + "," + // use this column for column family name
                TABLE_SCHEM_NAME + "," +
                TABLE_NAME_NAME + " ," +
                COLUMN_NAME + "," +
                "null as KEY_SEQ," +
                "PK_NAME" +
                " from " + TYPE_SCHEMA_AND_TABLE + 
                " where ");
        buf.append(TABLE_SCHEM_NAME + (schema == null || schema.length() == 0 ? " is null" : " = '" + SchemaUtil.normalizeIdentifier(schema) + "'" ));
        buf.append(" and " + TABLE_NAME_NAME + " = '" + SchemaUtil.normalizeIdentifier(table) + "'" );
        buf.append(" and " + TABLE_CAT_NAME + " is null" );
        buf.append(" order by " + ORDINAL_POSITION);
        buf.append(" limit " + ROW_LIMIT);
        // Dynamically replaces the KEY_SEQ with an expression that gets incremented after each next call.
        Statement stmt = connection.createStatement(new PhoenixStatementFactory() {

            @Override
            public PhoenixStatement newStatement(PhoenixConnection connection) {
                final byte[] unsetValue = new byte[0];
                final ImmutableBytesWritable pkNamePtr = new ImmutableBytesWritable(unsetValue);
                final byte[] rowNumberHolder = new byte[PDataType.INTEGER.getMaxLength()];
                return new PhoenixStatement(connection) {
                    @Override
                    protected PhoenixResultSet newResultSet(Scanner scanner) throws SQLException {
                        RowProjector projector = scanner.getProjection();
                        List<ColumnProjector> columns = new ArrayList<ColumnProjector>(projector.getColumnProjectors());
                        ColumnProjector column = columns.get(keySeqPosition);
                        
                        columns.set(keySeqPosition, new ExpressionProjector(column.getName(), column.getTableName(), 
                                new BaseTerminalExpression() {
                                    @Override
                                    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
                                        ptr.set(rowNumberHolder);
                                        return true;
                                    }

                                    @Override
                                    public PDataType getDataType() {
                                        return PDataType.INTEGER;
                                    }
                                },
                                column.isCaseSensitive())
                        );
                        column = columns.get(pkNamePosition);
                        columns.set(pkNamePosition, new ExpressionProjector(column.getName(), column.getTableName(), 
                                new BaseTerminalExpression() {
                                    @Override
                                    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
                                        if (pkNamePtr.get() == unsetValue) {
                                            KeyValue kv = tuple.getValue(TABLE_FAMILY_BYTES, PK_NAME_BYTES);
                                            if (kv == null) {
                                                ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
                                                pkNamePtr.set(ByteUtil.EMPTY_BYTE_ARRAY);
                                            } else {
                                                ptr.set(kv.getBuffer(),kv.getValueOffset(),kv.getValueLength());
                                                pkNamePtr.set(kv.getBuffer(),kv.getValueOffset(),kv.getValueLength());
                                            }
                                        } else {
                                            ptr.set(pkNamePtr.get(),pkNamePtr.getOffset(),pkNamePtr.getLength());
                                        }
                                        return true;
                                    }

                                    @Override
                                    public PDataType getDataType() {
                                        return PDataType.VARCHAR;
                                    }
                                },
                                column.isCaseSensitive())
                        );
                        final RowProjector newProjector = new RowProjector(columns);
                        Scanner delegate = new DelegateScanner(scanner) {
                            @Override
                            public RowProjector getProjection() {
                                return newProjector;
                            }
                            @Override
                            public ResultIterator iterator() throws SQLException {
                                return new DelegateResultIterator(super.iterator()) {
                                    private int rowCount = 0;

                                    @Override
                                    public Tuple next() throws SQLException {
                                        // Ignore first row, since it's the table row
                                        PDataType.INTEGER.toBytes(rowCount++, rowNumberHolder, 0);
                                        return super.next();
                                    }
                                };
                            }

                        };
                        return new PhoenixResultSet(delegate, this);
                    }
                    
                };
            }
            
        });
        ResultSet rs = stmt.executeQuery(buf.toString());
        if (rs.next()) { // Skip table row - we just use that to get the PK_NAME// Skip table row - we just use that to get the PK_NAME
            rs.getString(pkNamePosition+1); // Hack to cause the statement to cache this value
        }
        return rs;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        return emptyResultSet;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return connection.getHoldability();
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "";
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return DatabaseMetaData.sqlStateSQL99;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "schema";
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        // Catalogs are not supported for schemas
        if (catalog != null && catalog.length() > 0) {
            return emptyResultSet;
        }
        StringBuilder buf = new StringBuilder("select " + 
                "null " + TABLE_CATALOG_NAME + "," + // no catalog for tables
                TABLE_SCHEM_NAME +
                " from " + TYPE_SCHEMA_AND_TABLE + 
                " where " + COLUMN_NAME + " is null");
        if (schemaPattern != null) {
            buf.append(" and " + TABLE_SCHEM_NAME + " like '" + SchemaUtil.normalizeIdentifier(schemaPattern) + "'");
        }
        buf.append(" group by " + TABLE_SCHEM_NAME);
        buf.append(" limit " + ROW_LIMIT); // limit to prevent parallelization: we don't need it here
        Statement stmt = connection.createStatement();
        return stmt.executeQuery(buf.toString());
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "";
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return emptyResultSet;
    }

    private static final Integer TABLE_TYPE_MAX_LENGTH = 1;
    private static final PDatum TABLE_TYPE_DATUM = new PDatum() {
        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public PDataType getDataType() {
            return PDataType.CHAR;
        }

        @Override
        public Integer getMaxLength() {
            return TABLE_TYPE_MAX_LENGTH;
        }
    };
    private static final RowProjector TABLE_TYPE_ROW_PROJECTOR = new RowProjector(Arrays.<ColumnProjector>asList(
            new ExpressionProjector(TABLE_TYPE_NAME, TYPE_SCHEMA_AND_TABLE, 
                    new RowKeyColumnExpression(TABLE_TYPE_DATUM,
                            new RowKeyValueAccessor(Collections.<PDatum>singletonList(TABLE_TYPE_DATUM), 0)), false)
            ));
    private static final Collection<Tuple> TABLE_TYPE_TUPLES = Lists.newArrayListWithExpectedSize(PTableType.values().length);
    static {
        for (PTableType tableType : PTableType.values()) {
            TABLE_TYPE_TUPLES.add(new SingleKeyValueTuple(KeyValueUtil.newKeyValue(PDataType.CHAR.toBytes(tableType.getSerializedValue()), TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES, MetaDataProtocol.MIN_TABLE_TIMESTAMP, ByteUtil.EMPTY_BYTE_ARRAY)));
        }
    }
    private static final Scanner TABLE_TYPE_SCANNER = new WrappedScanner(new MaterializedResultIterator(TABLE_TYPE_TUPLES),TABLE_TYPE_ROW_PROJECTOR);
    @Override
    public ResultSet getTableTypes() throws SQLException {
        return new PhoenixResultSet(TABLE_TYPE_SCANNER, new PhoenixStatement(connection));
    }

    /**
     * We support either:
     * 1) A non null tableNamePattern to find an exactly match with a table name, in which case either a single
     *    row would be returned in the ResultSet (if found) or no rows would be returned (if not
     *    found).
     * 2) A null tableNamePattern, in which case the ResultSet returned would have one row per
     *    table.
     * Note that catalog and schemaPattern must be null or an empty string and types must be null
     * or "TABLE".  Otherwise, no rows will be returned.
     */
    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        // Catalogs are not supported for schemas
        if (catalog != null && catalog.length() > 0) {
            return emptyResultSet;
        }
        StringBuilder buf = new StringBuilder("select " + 
                "null " + TABLE_CAT_NAME + "," + // no catalog for tables
                TABLE_SCHEM_NAME + "," +
                TABLE_NAME_NAME + " ," +
                TABLE_TYPE_NAME + "," +
                REMARKS_NAME + " ," +
                TYPE_NAME + "," +
                SELF_REFERENCING_COL_NAME_NAME + "," +
                REF_GENERATION_NAME +
                " from " + TYPE_SCHEMA_AND_TABLE + 
                " where " + COLUMN_NAME + " is null");
        if (schemaPattern != null) {
            buf.append(" and " + TABLE_SCHEM_NAME + (schemaPattern.length() == 0 ? " is null" : " like '" + SchemaUtil.normalizeIdentifier(schemaPattern) + "'" ));
        }
        if (tableNamePattern != null) {
            buf.append(" and " + TABLE_NAME_NAME + " like '" + SchemaUtil.normalizeIdentifier(tableNamePattern) + "'" );
        }
        if (types != null && types.length > 0) {
            buf.append(" and " + TABLE_TYPE_NAME + " IN (");
            for (String type : types) {
                buf.append('\'');
                buf.append(type);
                buf.append('\'');
                buf.append(',');
            }
            buf.setCharAt(buf.length()-1, ')');
        }
        buf.append(" order by " + TABLE_TYPE_NAME + "," + TABLE_SCHEM_NAME + "," + TABLE_NAME_NAME);
        buf.append(" limit " + ROW_LIMIT);
        Statement stmt = connection.createStatement();
        return stmt.executeQuery(buf.toString());
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return emptyResultSet;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        return emptyResultSet;
    }

    @Override
    public String getURL() throws SQLException {
        return connection.getURL();
    }

    @Override
    public String getUserName() throws SQLException {
        return ""; // FIXME: what should we return here?
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false; // FIXME?
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        // TODO: review
        return type ==  ResultSet.TYPE_FORWARD_ONLY && concurrency == Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        // TODO
        return holdability == connection.getHoldability();
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return level == connection.getTransactionIsolation();
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!iface.isInstance(this)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CLASS_NOT_UNWRAPPABLE)
                .setMessage(this.getClass().getName() + " not unwrappable from " + iface.getName())
                .build().buildException();
        }
        return (T)this;
    }


    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        return this.emptyResultSet;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }
}
