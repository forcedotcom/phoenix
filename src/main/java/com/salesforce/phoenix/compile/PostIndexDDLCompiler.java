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

import java.sql.*;
import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;

import com.salesforce.phoenix.execute.MutationState;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.jdbc.PhoenixParameterMetaData;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.util.IndexUtil;
import com.salesforce.phoenix.util.MetaDataUtil;


/**
 * Class that compiles plan to generate initial data values after a DDL command for
 * index table.
 */
public class PostIndexDDLCompiler implements PostOpCompiler {
    private final PhoenixConnection connection;
    private final PTable dataTable;

    public PostIndexDDLCompiler(PhoenixConnection connection, PTable dataTable) {
        this.connection = connection;
        this.dataTable = dataTable;
    }

    @Override
    public MutationPlan compile(final TableRef tableRef, final byte[] emptyCF, final List<PColumn> deleteList,
            final long timestamp) throws SQLException {
        return new MutationPlan() {

            @Override
            public PhoenixConnection getConnection() {
                return connection;
            }

            @Override
            public ParameterMetaData getParameterMetaData() {
                return PhoenixParameterMetaData.EMPTY_PARAMETER_META_DATA;
            }

            @Override
            public ExplainPlan getExplainPlan() throws SQLException {
                return ExplainPlan.EMPTY_PLAN;
            }

            @Override
            public MutationState execute() throws SQLException {
                /*
                 * Handles:
                 * 1) Populate a newly created table with contents.
                 * 2) Activate the index by setting the INDEX_STATE to 
                 */
                // NOTE: For first version, we would use a upsert/select to populate the new index table and
                //   returns synchronously. Creating an index on an existing table with large amount of data
                //   will as a result take a very very long time.
                //   In the long term, we should change this to an asynchronous process to populate the index
                //   that would allow the user to easily monitor the process of index creation.
                StringBuilder indexColumns = new StringBuilder();
                StringBuilder dataColumns = new StringBuilder();
                PTable indexTable = tableRef.getTable();
                for (PColumn col: dataTable.getColumns()) {
                    String indexColName = IndexUtil.getIndexColumnName(col);
                    try {
                        indexTable.getColumn(indexColName);
                        if (col.getFamilyName() != null) {
                            dataColumns.append('"').append(col.getFamilyName()).append("\".");
                        }
                        dataColumns.append('"').append(col.getName()).append("\",");
                        indexColumns.append('"').append(indexColName).append("\",");
                    } catch (ColumnNotFoundException e) {
                        // Catch and ignore - means that this data column is not in the index
                    }
                }
                dataColumns.setLength(dataColumns.length()-1);
                indexColumns.setLength(indexColumns.length()-1);
                String schemaName = tableRef.getSchema().getName();
                String tableName = tableRef.getTable().getName().getString();
                
                StringBuilder updateStmtStr = new StringBuilder();
                updateStmtStr.append("UPSERT INTO ").append(schemaName.length() == 0 ? "" : '"' + schemaName + "\".").append('"').append(tableName).append("\"(")
                    .append(indexColumns).append(") SELECT ").append(dataColumns).append(" FROM ")
                    .append(schemaName.length() == 0 ? "" : '"' + schemaName + "\".").append('"').append(dataTable.getName().getString()).append('"');
                PreparedStatement updateStmt = connection.prepareStatement(updateStmtStr.toString());
                boolean wasAutoCommit = connection.getAutoCommit();
                int rowsUpdated = 0;
                try {
                    connection.setAutoCommit(true);
                    updateStmt.execute();
                    rowsUpdated = updateStmt.getUpdateCount();
                } finally {
                    if (!wasAutoCommit) connection.setAutoCommit(false);
                }
                
                List<Mutation> tableMetadata = MetaDataUtil.getIndexStateMutations(connection, schemaName, tableName, PIndexState.ACTIVE);
                connection.getQueryServices().updateIndexState(tableMetadata);
                 
                // Return number of rows built for index
                return new MutationState(rowsUpdated, connection);
            }
        };
    }

}
