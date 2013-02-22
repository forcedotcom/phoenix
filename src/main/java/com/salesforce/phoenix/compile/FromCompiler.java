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
package com.salesforce.phoenix.compile;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.*;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.*;


/**
 * 
 * Validates FROM clause and builds a ColumnResolver for resolving column references
 *
 * @author jtaylor
 * @since 0.1
 */
public class FromCompiler {
    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();
    private static final ColumnResolver EMPTY_TABLE_RESOLVER = new ColumnResolver() {

        @Override
        public List<TableRef> getTables() {
            return Collections.emptyList();
        }

        @Override
        public ColumnRef resolveColumn(ColumnParseNode node) throws SQLException {
            throw new UnsupportedOperationException();
        }
        
    };

    public static ColumnResolver getResolver(final CreateTableStatement statement, final PhoenixConnection connection) throws SQLException {
        return EMPTY_TABLE_RESOLVER;
    }


    // TODO: commonize with one for upsert
    public static ColumnResolver getResolver(DropColumnStatement statement, PhoenixConnection connection) throws SQLException {
        TableName tableName = statement.getTableName();
        NamedTableNode tableNode =  FACTORY.namedTable(null, tableName);
        FromClauseVisitor visitor = new DDLFromClauseVisitor(connection);
        tableNode.accept(visitor);
        return visitor;
    }

    /**
     * Iterate through the nodes in the FROM clause to build a column resolver used to
     * lookup a column given the name and alias.
     * @param statement the select statement
     * @return the column resolver
     * @throws SQLException 
     * @throws SQLFeatureNotSupportedException if unsupported constructs appear in the FROM
     * clause. Currently only a single table name is supported.
     * @throws TableNotFoundException if table name not found in schema
     */
    public static ColumnResolver getResolver(SelectStatement statement, PhoenixConnection connection) throws SQLException {
        List<TableNode> fromNodes = statement.getFrom();
        if (fromNodes.size() > 1) {
            throw new SQLFeatureNotSupportedException("Joins not supported");
        }
        FromClauseVisitor visitor = new SelectFromClauseVisitor(connection);
        for (TableNode node : fromNodes) {
            node.accept(visitor);
        }
        return visitor;
    }
    
    public static ColumnResolver getResolver(MutationStatement statement, PhoenixConnection connection) throws SQLException {
        TableName intoNodeName = statement.getTable();
        NamedTableNode intoNode =  FACTORY.namedTable(null, intoNodeName);
        FromClauseVisitor visitor = new DMLFromClauseVisitor(connection);
        intoNode.accept(visitor);
        return visitor;
    }
    
    private static class SelectFromClauseVisitor extends FromClauseVisitor {
        private final MetaDataClient client;

        public SelectFromClauseVisitor(PhoenixConnection connection) {
            super(connection);
            client = new MetaDataClient(connection);
        }
        
        @Override
        protected TableRef createTableRef(String alias, String schemaName, String tableName) throws SQLException {
            long timeStamp = Math.abs(client.updateCache(schemaName, tableName));
            PSchema theSchema = null;
            try {
                theSchema = connection.getPMetaData().getSchema(schemaName);
            } catch (SchemaNotFoundException e) { // Rethrow with more info
                throw new TableNotFoundException(schemaName, tableName);
            }
            PTable theTable = theSchema.getTable(tableName);
            TableRef tableRef = new TableRef(alias, theTable, theSchema, timeStamp);
            return tableRef;
        }
        
    }
    
    private static class DDLFromClauseVisitor extends FromClauseVisitor {
        public DDLFromClauseVisitor(PhoenixConnection connection) {
            super(connection);
        }
        
        @Override
        protected TableRef createTableRef(String alias, String schemaName, String tableName) throws SQLException {
            PSchema theSchema = null;
            try {
                theSchema = connection.getPMetaData().getSchema(schemaName);
            } catch (SchemaNotFoundException e) { // Rethrow with more info
                throw new TableNotFoundException(schemaName, tableName);
            }
            PTable theTable = theSchema.getTable(tableName);
            TableRef tableRef = new TableRef(alias, theTable, theSchema, HConstants.LATEST_TIMESTAMP);
            return tableRef;
        }
    }
    
    private static class DMLFromClauseVisitor extends FromClauseVisitor {
        private MetaDataClient client;

        public DMLFromClauseVisitor(PhoenixConnection connection) {
            super(connection);
        }
        
        private MetaDataClient getMetaDataClient() {
            if (client == null) {
                client = new MetaDataClient(connection);
            }
            return client;
        }
        
        @Override
        protected TableRef createTableRef(String alias, String schemaName, String tableName) throws SQLException {
            SQLException sqlE = null;
            long timeStamp = QueryConstants.UNSET_TIMESTAMP;
            while (true) {
                boolean retry = !connection.getAutoCommit();
                try {
                    if (connection.getAutoCommit()) {
                        timeStamp = Math.abs(getMetaDataClient().updateCache(schemaName, tableName));
                    }
                    PSchema theSchema = connection.getPMetaData().getSchema(schemaName);
                    PTable theTable = theSchema.getTable(tableName);
                    TableRef tableRef = new TableRef(alias, theTable, theSchema, timeStamp);
                    return tableRef;
                } catch (SchemaNotFoundException e) {
                    sqlE = new TableNotFoundException(schemaName, tableName);
                } catch (TableNotFoundException e) {
                    sqlE = e;
                }
                if (retry && getMetaDataClient().updateCache(schemaName, tableName) < 0) {
                    retry = false;
                    continue;
                }
                break;
            }
            throw sqlE;
        }
        
    }

    private static abstract class FromClauseVisitor implements TableNodeVisitor, ColumnResolver {
        private final ListMultimap<Key,TableRef> tableMap;
        private final List<TableRef> tables;
        protected final PhoenixConnection connection;
        
        private FromClauseVisitor(PhoenixConnection connection) {
            this.connection = connection;
            tableMap = ArrayListMultimap.<Key,TableRef>create();
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
		private static final class Key extends Pair<String,String> {
            private Key(String schemaName, String tableName) {
                super(schemaName,tableName);
            }
        }
        
        protected abstract TableRef createTableRef(String alias, String schemaName, String tableName) throws SQLException;
        
        @Override
        public void visit(NamedTableNode namedTableNode) throws SQLException {
            String tableName = namedTableNode.getName().getTableName();
            String schemaName = namedTableNode.getName().getSchemaName();
            
            String alias = namedTableNode.getAlias();
            TableRef tableRef = createTableRef(alias, schemaName, tableName);
            PSchema theSchema = tableRef.getSchema();
            PTable theTable = tableRef.getTable();
            if (alias != null) {
                tableMap.put(new Key(null,alias), tableRef);
            }
            tableMap.put(new Key(null, theTable.getName().getString()), tableRef);
            tableMap.put(new Key(theSchema.getName(),theTable.getName().getString()), tableRef);
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
        
        private ColumnFamilyRef resolveColumnFamily(String cfName, String tableName) throws SQLException {
            if (tableName == null) {
                ColumnFamilyRef theColumnFamilyRef = null;
                Iterator<TableRef> iterator = tables.iterator();
                while (iterator.hasNext()) {
                    TableRef tableRef = iterator.next();
                    try {
                        PColumnFamily columnFamily = tableRef.getTable().getColumnFamily(cfName);
                        if (theColumnFamilyRef != null) {
                            throw new TableNotFoundException(cfName);
                        }
                        theColumnFamilyRef = new ColumnFamilyRef(tableRef, columnFamily);
                    } catch (ColumnFamilyNotFoundException e) {
                    }
                }
                if (theColumnFamilyRef != null) {
                    return theColumnFamilyRef;
                }
                throw new TableNotFoundException(cfName);
            } else {
                TableRef tableRef = resolveTable(null, tableName);
                PColumnFamily columnFamily = tableRef.getTable().getColumnFamily(cfName);
                return new ColumnFamilyRef(tableRef, columnFamily);
            }
        }
        
        @Override
        public ColumnRef resolveColumn(ColumnParseNode node) throws SQLException {
            TableName tableName = node.getTableName();
            if (tableName == null) {
                int theColumnPosition = -1;
                TableRef theTableRef = null;
                Iterator<TableRef> iterator = tables.iterator();
                while (iterator.hasNext()) {
                    TableRef tableRef = iterator.next();
                    try {
                        PColumn column = tableRef.getTable().getColumn(node.getName());
                        if (theTableRef != null) {
                            throw new AmbiguousColumnException(node.getName());
                        }
                        theTableRef = tableRef;
                        theColumnPosition = column.getPosition();
                    } catch (ColumnNotFoundException e) {
                        
                    }
                }
                if (theTableRef != null) {
                    return new ColumnRef(theTableRef, theColumnPosition);
                }
                throw new ColumnNotFoundException(node.getName());
            } else {
                try {
                    TableRef tableRef = resolveTable(tableName.getSchemaName(), tableName.getTableName());
                    PColumn column = tableRef.getTable().getColumn(node.getName());
                    return new ColumnRef(tableRef, column.getPosition());
                } catch (TableNotFoundException e) {
                    // Try using the tableName as a columnFamily reference instead
                    ColumnFamilyRef cfRef = resolveColumnFamily(tableName.getTableName(), tableName.getSchemaName());
                    PColumn column = cfRef.getFamily().getColumn(node.getName());
                    return new ColumnRef(cfRef.getTableRef(), column.getPosition());
                }
            }
        }
    }
}
