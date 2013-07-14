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
package com.salesforce.phoenix.compile;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;

import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.*;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.util.SchemaUtil;

/**
 * Validates FROM clause and builds a ColumnResolver for resolving column references
 * 
 * @author jtaylor
 * @since 0.1
 */
public class FromCompiler {
    private static final ColumnResolver EMPTY_TABLE_RESOLVER = new ColumnResolver() {

        @Override
        public List<TableRef> getTables() {
            return Collections.emptyList();
        }

        @Override
        public ColumnRef resolveColumn(String schemaName, String tableName, String colName) throws SQLException {
            throw new UnsupportedOperationException();
        }
    };

    public static ColumnResolver getResolver(final CreateTableStatement statement, final PhoenixConnection connection)
            throws SQLException {
        return EMPTY_TABLE_RESOLVER;
    }

    /**
     * Iterate through the nodes in the FROM clause to build a column resolver used to lookup a column given the name
     * and alias.
     * 
     * @param statement
     *            the select statement
     * @return the column resolver
     * @throws SQLException
     * @throws SQLFeatureNotSupportedException
     *             if unsupported constructs appear in the FROM clause. Currently only a single table name is supported.
     * @throws TableNotFoundException
     *             if table name not found in schema
     */
    public static ColumnResolver getResolver(SelectStatement statement, PhoenixConnection connection)
            throws SQLException {
        List<TableNode> fromNodes = statement.getFrom();
        if (fromNodes.size() > 1) { throw new SQLFeatureNotSupportedException("Joins not supported"); }
        MultiTableColumnResolver visitor = new MultiTableColumnResolver(connection);
        for (TableNode node : fromNodes) {
            node.accept(visitor);
        }
        return visitor;
    }

    public static ColumnResolver getResolver(SingleTableSQLStatement statement, PhoenixConnection connection,
            List<ColumnDef> dyn_columns) throws SQLException {
        SingleTableColumnResolver visitor = new SingleTableColumnResolver(connection, statement.getTable());
        return visitor;
    }

    public static ColumnResolver getResolver(SingleTableSQLStatement statement, PhoenixConnection connection)
            throws SQLException {
        return getResolver(statement, connection, Collections.<ColumnDef>emptyList());
    }

    private static class SingleTableColumnResolver extends BaseColumnResolver {
    	private final List<TableRef> tableRefs;
    	
        public SingleTableColumnResolver(PhoenixConnection connection, NamedTableNode table) throws SQLException {
            super(connection);
            TableName tableNameNode = table.getName();
            String schemaName = tableNameNode.getSchemaName();
            String tableName = tableNameNode.getTableName();
            SQLException sqlE = null;
            long timeStamp = QueryConstants.UNSET_TIMESTAMP;
            TableRef tableRef;
            boolean retry = true;
            while (true) {
                try {
                    if (connection.getAutoCommit()) {
                        timeStamp = Math.abs(client.updateCache(schemaName, tableName));
                    }
                    PSchema theSchema = connection.getPMetaData().getSchema(schemaName);
                    PTable theTable = theSchema.getTable(tableName);
                    // If dynamic columns have been specified add them to the table declaration
                    if (!table.getDynamicColumns().isEmpty()) {
                        theTable = this.addDynamicColumns(table.getDynamicColumns(), theTable);
                    }
                    tableRef = new TableRef(null, theTable, theSchema, timeStamp, !table.getDynamicColumns().isEmpty());
                    break;
                } catch (SchemaNotFoundException e) {
                    sqlE = new TableNotFoundException(schemaName, tableName);
                } catch (TableNotFoundException e) {
                    sqlE = new TableNotFoundException(schemaName, tableName);
                }
                if (retry && client.updateCache(schemaName, tableName) < 0) {
                    retry = false;
                    continue;
                }
                throw sqlE;
            }
            tableRefs = ImmutableList.of(tableRef);
        }

		@Override
		public List<TableRef> getTables() {
			return tableRefs;
		}

		@Override
		public ColumnRef resolveColumn(String schemaName, String tableName,
				String colName) throws SQLException {
			TableRef tableRef = tableRefs.get(0);
        	PColumn column = tableName == null ? 
        			tableRef.getTable().getColumn(colName) : 
        			tableRef.getTable().getColumnFamily(tableName).getColumn(colName);
            return new ColumnRef(tableRef, column.getPosition());
		}

    }

    private static abstract class BaseColumnResolver implements ColumnResolver {
        protected final PhoenixConnection connection;
        protected final MetaDataClient client;
        
        private BaseColumnResolver(PhoenixConnection connection) {
        	this.connection = connection;
            this.client = new MetaDataClient(connection);
        }

        protected PTable addDynamicColumns(List<ColumnDef> dynColumns, PTable theTable)
                throws SQLException {
            if (!dynColumns.isEmpty()) {
                List<PColumn> allcolumns = new ArrayList<PColumn>();
                allcolumns.addAll(theTable.getColumns());
                int position = allcolumns.size();
                PName defaultFamilyName = new PNameImpl(SchemaUtil.getEmptyColumnFamily(theTable.getColumnFamilies()));
                for (ColumnDef dynColumn : dynColumns) {
                    PName familyName = defaultFamilyName;
                    PName name = new PNameImpl(dynColumn.getColumnDefName().getColumnName());
                    String family = dynColumn.getColumnDefName().getFamilyName();
                    if (family != null) {
                        theTable.getColumnFamily(family); // Verifies that column family exists
                        familyName = new PNameImpl(family);
                    }
                    allcolumns.add(new PColumnImpl(name, familyName, dynColumn.getDataType(), dynColumn.getMaxLength(),
                            dynColumn.getScale(), dynColumn.isNull(), position, dynColumn.getColumnModifier()));
                    position++;
                }
                theTable = PTableImpl.makePTable(theTable, allcolumns);
            }
            return theTable;
        }
    }
    
    private static class MultiTableColumnResolver extends BaseColumnResolver implements TableNodeVisitor {
        private final ListMultimap<Key, TableRef> tableMap;
        private final List<TableRef> tables;

        private MultiTableColumnResolver(PhoenixConnection connection) {
        	super(connection);
            tableMap = ArrayListMultimap.<Key, TableRef> create();
            tables = Lists.newArrayList();
        }

        @Override
        public List<TableRef> getTables() {
            return tables;
        }

        @Override
        public void visit(BindTableNode boundTableNode) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public void visit(JoinTableNode joinNode) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @SuppressWarnings("serial")
        private static final class Key extends Pair<String, String> {
            private Key(String schemaName, String tableName) {
                super(schemaName, tableName);
            }
        }

        private TableRef createTableRef(String alias, String schemaName, String tableName,
                List<ColumnDef> dynamicColumnDefs) throws SQLException {
            long timeStamp = Math.abs(client.updateCache(schemaName, tableName));
            PSchema theSchema = null;
            try {
                theSchema = connection.getPMetaData().getSchema(schemaName);
            } catch (SchemaNotFoundException e) { // Rethrow with more info
                throw new TableNotFoundException(schemaName, tableName);
            }
            PTable theTable = theSchema.getTable(tableName);

            // If dynamic columns have been specified add them to the table declaration
            if (!dynamicColumnDefs.isEmpty()) {
                theTable = this.addDynamicColumns(dynamicColumnDefs, theTable);
            }
            TableRef tableRef = new TableRef(alias, theTable, theSchema, timeStamp, !dynamicColumnDefs.isEmpty());
            return tableRef;
        }


        @Override
        public void visit(NamedTableNode namedTableNode) throws SQLException {
            String tableName = namedTableNode.getName().getTableName();
            String schemaName = namedTableNode.getName().getSchemaName();

            String alias = namedTableNode.getAlias();
            List<ColumnDef> dynamicColumnDefs = namedTableNode.getDynamicColumns();

            TableRef tableRef = createTableRef(alias, schemaName, tableName, dynamicColumnDefs);
            PSchema theSchema = tableRef.getSchema();
            PTable theTable = tableRef.getTable();

            if (alias != null) {
                tableMap.put(new Key(null, alias), tableRef);
            }

            tableMap.put(new Key(null, theTable.getName().getString()), tableRef);
            tableMap.put(new Key(theSchema.getName(), theTable.getName().getString()), tableRef);
            tables.add(tableRef);
        }

        @Override
        public void visit(DerivedTableNode subselectNode) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        private static class ColumnFamilyRef {
            private final TableRef tableRef;
            private final PColumnFamily family;

            ColumnFamilyRef(TableRef tableRef, PColumnFamily family) {
                this.tableRef = tableRef;
                this.family = family;
            }

            public TableRef getTableRef() {
                return tableRef;
            }

            public PColumnFamily getFamily() {
                return family;
            }
        }

        private TableRef resolveTable(String schemaName, String tableName) throws SQLException {
            Key key = new Key(schemaName, tableName);
            List<TableRef> tableRefs = tableMap.get(key);
            if (tableRefs.size() == 0) {
                throw new TableNotFoundException(schemaName, tableName);
            } else if (tableRefs.size() > 1) {
                throw new AmbiguousTableException(tableName);
            } else {
                return tableRefs.get(0);
            }
        }

        private ColumnFamilyRef resolveColumnFamily(String tableName, String cfName) throws SQLException {
            if (tableName == null) {
                ColumnFamilyRef theColumnFamilyRef = null;
                Iterator<TableRef> iterator = tables.iterator();
                while (iterator.hasNext()) {
                    TableRef tableRef = iterator.next();
                    try {
                        PColumnFamily columnFamily = tableRef.getTable().getColumnFamily(cfName);
                        if (theColumnFamilyRef != null) { throw new TableNotFoundException(cfName); }
                        theColumnFamilyRef = new ColumnFamilyRef(tableRef, columnFamily);
                    } catch (ColumnFamilyNotFoundException e) {}
                }
                if (theColumnFamilyRef != null) { return theColumnFamilyRef; }
                throw new TableNotFoundException(cfName);
            } else {
                TableRef tableRef = resolveTable(null, tableName);
                PColumnFamily columnFamily = tableRef.getTable().getColumnFamily(cfName);
                return new ColumnFamilyRef(tableRef, columnFamily);
            }
        }

        @Override
        public ColumnRef resolveColumn(String schemaName, String tableName, String colName) throws SQLException {
            if (tableName == null) {
                int theColumnPosition = -1;
                TableRef theTableRef = null;
                Iterator<TableRef> iterator = tables.iterator();
                while (iterator.hasNext()) {
                    TableRef tableRef = iterator.next();
                    try {
                        PColumn column = tableRef.getTable().getColumn(colName);
                        if (theTableRef != null) { throw new AmbiguousColumnException(colName); }
                        theTableRef = tableRef;
                        theColumnPosition = column.getPosition();
                    } catch (ColumnNotFoundException e) {

                    }
                }
                if (theTableRef != null) { return new ColumnRef(theTableRef, theColumnPosition); }
                throw new ColumnNotFoundException(colName);
            } else {
                try {
                    TableRef tableRef = resolveTable(schemaName, tableName);
                    PColumn column = tableRef.getTable().getColumn(colName);
                    return new ColumnRef(tableRef, column.getPosition());
                } catch (TableNotFoundException e) {
                    // Try using the tableName as a columnFamily reference instead
                    ColumnFamilyRef cfRef = resolveColumnFamily(schemaName, tableName);
                    PColumn column = cfRef.getFamily().getColumn(colName);
                    return new ColumnRef(cfRef.getTableRef(), column.getPosition());
                }
            }
        }

    }
}
