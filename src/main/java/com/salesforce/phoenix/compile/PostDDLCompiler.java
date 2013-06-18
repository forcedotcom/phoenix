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
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import com.salesforce.phoenix.execute.AggregatePlan;
import com.salesforce.phoenix.execute.MutationState;
import com.salesforce.phoenix.expression.function.CountAggregateFunction;
import com.salesforce.phoenix.iterate.ResultIterator;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.jdbc.PhoenixParameterMetaData;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.query.Scanner;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ScanUtil;


/**
 * 
 * Class that compiles plan to update data values after a DDL command
 * executes.
 *
 * @author jtaylor
 * @since 0.1
 */
public class PostDDLCompiler  {
    private static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();
    private final PhoenixConnection connection;
    
    public PostDDLCompiler(PhoenixConnection connection) {
        this.connection = connection;
    }

    public MutationPlan compile(final List<TableRef> tableRefs, final long timeStamp) throws SQLException {
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
                SQLException sqlE = null;
                /*
                 * Handles:
                 * 1) deletion of all rows for a DROP TABLE and delete all rows for its associated index table.
                 */
                List<AliasedNode> select = Collections.<AliasedNode>singletonList(
                        NODE_FACTORY.aliasedNode(null, 
                                NODE_FACTORY.function(CountAggregateFunction.NORMALIZED_NAME, LiteralParseNode.STAR)));
                Scan scan = new Scan();
                scan.setAttribute(UngroupedAggregateRegionObserver.UNGROUPED_AGG, QueryConstants.TRUE);
                scan.setAttribute(UngroupedAggregateRegionObserver.DELETE_AGG, QueryConstants.TRUE);
                ColumnResolver resolver = new ColumnResolver() {
                    @Override
                    public List<TableRef> getTables() {
                        return tableRefs;
                    }
                    @Override
                    public ColumnRef resolveColumn(ColumnParseNode node) throws SQLException {
                        throw new UnsupportedOperationException();
                    }
                };
                StatementContext context = new StatementContext(connection, resolver, Collections.<Object>emptyList(), 0, scan);
                ScanUtil.setTimeRange(scan, timeStamp);
                RowProjector projector = ProjectionCompiler.getRowProjector(context, select, false, GroupBy.EMPTY_GROUP_BY, OrderBy.EMPTY_ORDER_BY, null);
                long totalMutationCount = 0;
                for (TableRef tableRef: tableRefs) {
                    QueryPlan plan = new AggregatePlan(context, tableRef, projector, null, GroupBy.EMPTY_GROUP_BY, false, null, OrderBy.EMPTY_ORDER_BY);
                    Scanner scanner = plan.getScanner();
                    ResultIterator iterator = scanner.iterator();
                    try {
                        Tuple row = iterator.next();
                        ImmutableBytesWritable ptr = context.getTempPtr();
                        totalMutationCount += (Long)projector.getColumnProjector(0).getValue(row, PDataType.LONG, ptr);
                    } catch (SQLException e) {
                        sqlE = e;
                    } finally {
                        try {
                            iterator.close();
                        } catch (SQLException e) {
                            if (sqlE == null) {
                                sqlE = e;
                            } else {
                                sqlE.setNextException(e);
                            }
                        } finally {
                            if (sqlE != null) {
                                throw sqlE;
                            }
                        }
                    }
                }
                final long count = totalMutationCount;
                return new MutationState(1, connection) {
                    @Override
                    public long getUpdateCount() {
                        return count;
                    }
                };
            }
        };
    }

    public MutationPlan compile(final TableRef tableRef, final byte[] emptyCF, final List<PColumn> deleteList, final long timeStamp) throws SQLException {
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
                SQLException sqlE = null;
                if (deleteList == null && emptyCF == null) {
                    return new MutationState(0, connection);
                }
                /*
                 * Handles:
                 * 1) deletion of all rows for a DROP TABLE
                 * 2) deletion of all column values for a ALTER TABLE DROP COLUMN
                 * 3) updating the necessary rows to have an empty KV
                 */
                List<AliasedNode> select = Collections.<AliasedNode>singletonList(
                        NODE_FACTORY.aliasedNode(null, 
                                NODE_FACTORY.function(CountAggregateFunction.NORMALIZED_NAME, LiteralParseNode.STAR)));
                Scan scan = new Scan();
                scan.setAttribute(UngroupedAggregateRegionObserver.UNGROUPED_AGG, QueryConstants.TRUE);
                final List<TableRef> tableRefs = Collections.singletonList(tableRef);
                ColumnResolver resolver = new ColumnResolver() {
                    @Override
                    public List<TableRef> getTables() {
                        return tableRefs;
                    }
                    @Override
                    public ColumnRef resolveColumn(ColumnParseNode node) throws SQLException {
                        throw new UnsupportedOperationException();
                    }
                };
                StatementContext context = new StatementContext(connection, resolver, Collections.<Object>emptyList(), 0, scan);
                ScanUtil.setTimeRange(scan, timeStamp);
                if (emptyCF != null) {
                    scan.setAttribute(UngroupedAggregateRegionObserver.EMPTY_CF, emptyCF);
                }
                if (deleteList != null) {
                    if (deleteList.isEmpty()) {
                        scan.setAttribute(UngroupedAggregateRegionObserver.DELETE_AGG, QueryConstants.TRUE);
                    } else {
                        PColumn column = deleteList.get(0);
                        if (emptyCF == null) {
                            scan.addColumn(column.getFamilyName().getBytes(), column.getName().getBytes());
                        }
                        scan.setAttribute(UngroupedAggregateRegionObserver.DELETE_CF, column.getFamilyName().getBytes());
                        scan.setAttribute(UngroupedAggregateRegionObserver.DELETE_CQ, column.getName().getBytes());
                    }
                }
                RowProjector projector = ProjectionCompiler.getRowProjector(context, select, false, GroupBy.EMPTY_GROUP_BY, OrderBy.EMPTY_ORDER_BY, null);
                QueryPlan plan = new AggregatePlan(context, tableRef, projector, null, GroupBy.EMPTY_GROUP_BY, false, null, OrderBy.EMPTY_ORDER_BY);
                Scanner scanner = plan.getScanner();
                ResultIterator iterator = scanner.iterator();
                try {
                    Tuple row = iterator.next();
                    ImmutableBytesWritable ptr = context.getTempPtr();
                    final long mutationCount = (Long)projector.getColumnProjector(0).getValue(row, PDataType.LONG, ptr);
                    return new MutationState(1, connection) {
                        @Override
                        public long getUpdateCount() {
                            return mutationCount;
                        }
                    };
                } catch (SQLException e) {
                    sqlE = e;
                } finally {
                    try {
                        iterator.close();
                    } catch (SQLException e) {
                        if (sqlE == null) {
                            sqlE = e;
                        } else {
                            sqlE.setNextException(e);
                        }
                    } finally {
                        if (sqlE != null) {
                            throw sqlE;
                        }
                    }
                }
                return null; // Impossible
            }
        };
    }
}
