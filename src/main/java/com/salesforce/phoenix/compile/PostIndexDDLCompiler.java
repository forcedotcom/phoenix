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

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import com.salesforce.phoenix.execute.MutationState;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.jdbc.PhoenixParameterMetaData;
import com.salesforce.phoenix.parse.CreateIndexStatement;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.MetaDataClient;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PIndexState;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.TableRef;


/**
 * Class that compiles plan to generate initial data values after a DDL command for
 * index table.
 */
public class PostIndexDDLCompiler implements PostOpCompiler {
    private final PhoenixConnection connection;

    public PostIndexDDLCompiler(PhoenixConnection connection) {
        this.connection = connection;
    }

    public MutationPlan compile(final CreateIndexStatement stmt, final PTable indexTable) {
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
                StringBuilder columns = new StringBuilder();
                for (PColumn col: indexTable.getColumns()) {
                    columns.append(col.getName()).append(",");
                }
                columns.deleteCharAt(columns.length()-1);
                
                StringBuilder updateStmtStr = new StringBuilder();
                updateStmtStr.append("UPSERT INTO ").append(getFullIndexName(stmt)).append("(")
                    .append(columns).append(") SELECT ").append(columns).append(" FROM ")
                    .append(stmt.getTableName().toString());
                PreparedStatement updateStmt = connection.prepareStatement(updateStmtStr.toString());
                updateStmt.execute();
                MetaDataClient.updateIndexState(connection, stmt.getTableName().getSchemaName(), 
                        stmt.getTableName().getTableName(), stmt.getIndexName().getName(), PIndexState.ACTIVE);
                
                // Did not change anything on the original table.
                return new MutationState(0, connection);
            }
        };
    }

    private static String getFullIndexName(CreateIndexStatement stmt) {
        return stmt.getTableName().getSchemaName() == null ? stmt.getIndexName().getName()
                : stmt.getTableName().getSchemaName() + QueryConstants.NAME_SEPARATOR + stmt.getIndexName().getName();
    }

    @Override
    public MutationPlan compile(List<TableRef> tableRefs, byte[] emptyCF,
            List<PColumn> deleteList) {
        // TODO Auto-generated method stub
        return null;
    }

}
