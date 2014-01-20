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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Iterators;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.execute.MutationState;
import com.salesforce.phoenix.expression.AndExpression;
import com.salesforce.phoenix.expression.ComparisonExpression;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.IsNullExpression;
import com.salesforce.phoenix.expression.KeyValueColumnExpression;
import com.salesforce.phoenix.expression.RowKeyColumnExpression;
import com.salesforce.phoenix.expression.visitor.TraverseNoExpressionVisitor;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.jdbc.PhoenixStatement;
import com.salesforce.phoenix.parse.CreateTableStatement;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.query.DelegateConnectionQueryServices;
import com.salesforce.phoenix.schema.MetaDataClient;
import com.salesforce.phoenix.schema.PMetaData;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.PTable.ViewType;
import com.salesforce.phoenix.schema.PTableType;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.ByteUtil;


public class CreateTableCompiler {
    private final PhoenixStatement statement;
    
    public CreateTableCompiler(PhoenixStatement statement) {
        this.statement = statement;
    }

    public MutationPlan compile(final CreateTableStatement create) throws SQLException {
        final PhoenixConnection connection = statement.getConnection();
        ColumnResolver resolver = FromCompiler.getResolver(create, connection);
        PTableType type = create.getTableType();
        PhoenixConnection connectionToBe = connection;
        PTable parentToBe = null;
        ViewType viewTypeToBe = null;
        Scan scan = new Scan();
        final StatementContext context = new StatementContext(statement, resolver, statement.getParameters(), scan);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
        ParseNode whereNode = create.getWhereClause();
        Expression where = null;
        if (type == PTableType.VIEW) {
            TableRef tableRef = resolver.getTables().get(0);
            parentToBe = tableRef.getTable();
            viewTypeToBe = parentToBe.getViewType() == ViewType.MAPPED ? ViewType.MAPPED : ViewType.UPDATABLE;
            if (whereNode != null) {
                if (whereNode.isStateless()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.VIEW_WHERE_IS_CONSTANT)
                        .build().buildException();
                }
                whereNode = StatementNormalizer.normalize(whereNode, resolver);
                where = whereNode.accept(expressionCompiler);
                if (expressionCompiler.isAggregate()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.AGGREGATE_IN_WHERE).build().buildException();
                }
                if (viewTypeToBe != ViewType.MAPPED) {
                    Long scn = connection.getSCN();
                    connectionToBe = scn != null ? connection :
                        // If we haved no SCN on our connection, freeze the SCN at when
                        // the base table was resolved to prevent any race condition on
                        // the error checking we do for the base table. The only potential
                        // issue is if the base table lives on a different region server
                        // than the new table will, then we're relying here on the system
                        // clocks being in sync.
                        new PhoenixConnection(
                            // When the new table is created, we still want to cache it
                            // on our connection.
                            new DelegateConnectionQueryServices(connection.getQueryServices()) {
                                @Override
                                public PMetaData addTable(PTable table) throws SQLException {
                                    return connection.addTable(table);
                                }
                            },
                            connection, tableRef.getTimeStamp());
                    ViewWhereExpressionVisitor visitor = new ViewWhereExpressionVisitor();
                    where.accept(visitor);
                    viewTypeToBe = visitor.isUpdatable() ? ViewType.UPDATABLE : ViewType.READ_ONLY;
                }
            }
        }
        final Expression viewExpression = where;
        final ViewType viewType = viewTypeToBe;
        List<ParseNode> splitNodes = create.getSplitNodes();
        final byte[][] splits = new byte[splitNodes.size()][];
        ImmutableBytesWritable ptr = context.getTempPtr();
        for (int i = 0; i < splits.length; i++) {
            ParseNode node = splitNodes.get(i);
            if (node.isStateless()) {
                Expression expression = node.accept(expressionCompiler);
                if (expression.evaluate(null, ptr)) {;
                    splits[i] = ByteUtil.copyKeyBytesIfNecessary(ptr);
                    continue;
                }
            }
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.SPLIT_POINT_NOT_CONSTANT)
                .setMessage("Node: " + node).build().buildException();
        }
        final MetaDataClient client = new MetaDataClient(connectionToBe);
        final PTable parent = parentToBe;
        
        return new MutationPlan() {

            @Override
            public ParameterMetaData getParameterMetaData() {
                return context.getBindManager().getParameterMetaData();
            }

            @Override
            public MutationState execute() throws SQLException {
                try {
                    return client.createTable(create, splits, parent, viewExpression, viewType);
                } finally {
                    if (client.getConnection() != connection) {
                        client.getConnection().close();
                    }
                }
            }

            @Override
            public ExplainPlan getExplainPlan() throws SQLException {
                return new ExplainPlan(Collections.singletonList("CREATE TABLE"));
            }

            @Override
            public PhoenixConnection getConnection() {
                return connection;
            }
            
        };
    }
    
    private static class ViewWhereExpressionVisitor extends TraverseNoExpressionVisitor<Boolean> {
        private boolean isUpdatable = true;

        public boolean isUpdatable() {
            return isUpdatable;
        }

        @Override
        public Boolean defaultReturn(Expression node, List<Boolean> l) {
            // We only hit this if we're trying to traverse somewhere
            // in which we don't have a visitLeave that returns non null
            isUpdatable = false;
            return null;
        }

        @Override
        public Iterator<Expression> visitEnter(AndExpression node) {
            return node.getChildren().iterator();
        }

        @Override
        public Boolean visitLeave(AndExpression node, List<Boolean> l) {
            return l.isEmpty() ? null : Boolean.TRUE;
        }

        @Override
        public Iterator<Expression> visitEnter(ComparisonExpression node) {
            return node.getFilterOp() == CompareOp.EQUAL && node.getChildren().get(1).isStateless() && node.getChildren().get(1).isDeterministic() ? Iterators.singletonIterator(node.getChildren().get(0)) : super.visitEnter(node);
        }

        @Override
        public Boolean visitLeave(ComparisonExpression node, List<Boolean> l) {
            return l.isEmpty() ? null : Boolean.TRUE;
        }

        @Override
        public Iterator<Expression> visitEnter(IsNullExpression node) {
            return node.isNegate() ? super.visitEnter(node) : node.getChildren().iterator();
        }
        
        @Override
        public Boolean visitLeave(IsNullExpression node, List<Boolean> l) {
            return l.isEmpty() ? null : Boolean.TRUE;
        }
        
        @Override
        public Boolean visit(RowKeyColumnExpression node) {
            return Boolean.TRUE;
        }

        @Override
        public Boolean visit(KeyValueColumnExpression node) {
            return Boolean.TRUE;
        }
        
    }
}
