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

import static com.salesforce.phoenix.query.QueryConstants.*;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;

import com.salesforce.phoenix.execute.MutationState;
import com.salesforce.phoenix.iterate.ParallelIterators.ParallelIteratorFactory;
import com.salesforce.phoenix.iterate.*;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.schema.tuple.SingleKeyValueTuple;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.KeyValueUtil;

/**
 * Factory class used to instantiate an iterator to handle mutations made during a parallel scan.
 */
public abstract class MutatingParallelIteratorFactory implements ParallelIteratorFactory {
    protected final PhoenixConnection connection;
    protected final TableRef tableRef;

    protected MutatingParallelIteratorFactory(PhoenixConnection connection, TableRef tableRef) {
        this.connection = connection;
        this.tableRef = tableRef;
    }
    
    /**
     * Method that does the actual mutation work
     */
    abstract protected MutationState mutate(PhoenixConnection connection, ResultIterator iterator) throws SQLException;
    
    @Override
    public PeekingResultIterator newIterator(ResultIterator iterator) throws SQLException {
        // Clone the connection as it's not thread safe and will be operated on in parallel
        final PhoenixConnection connection = new PhoenixConnection(this.connection);
        MutationState state = mutate(connection, iterator);
        long totalRowCount = state.getUpdateCount();
        if (connection.getAutoCommit()) {
            connection.getMutationState().join(state);
            connection.commit();
            ConnectionQueryServices services = connection.getQueryServices();
            int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
            state = new MutationState(maxSize, connection, totalRowCount);
        }
        final MutationState finalState = state;
        byte[] value = PDataType.LONG.toBytes(totalRowCount);
        KeyValue keyValue = KeyValueUtil.newKeyValue(UNGROUPED_AGG_ROW_KEY, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, AGG_TIMESTAMP, value, 0, value.length);
        final Tuple tuple = new SingleKeyValueTuple(keyValue);
        return new PeekingResultIterator() {
            private boolean done = false;
            
            @Override
            public Tuple next() throws SQLException {
                if (done) {
                    return null;
                }
                done = true;
                return tuple;
            }

            @Override
            public void explain(List<String> planSteps) {
            }

            @Override
            public void close() throws SQLException {
                try {
                    // Join the child mutation states in close, since this is called in a single threaded manner
                    // after the parallel results have been processed.
                    if (!connection.getAutoCommit()) {
                        MutatingParallelIteratorFactory.this.connection.getMutationState().join(finalState);
                    }
                } finally {
                    connection.close();
                }
            }

            @Override
            public Tuple peek() throws SQLException {
                return done ? null : tuple;
            }
        };
    }
}
