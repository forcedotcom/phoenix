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

import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.execute.MutationState;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.CreateTableStatement;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.schema.MetaDataClient;


public class CreateTableCompiler {
    private final PhoenixConnection connection;
    
    public CreateTableCompiler(PhoenixConnection connection) {
        this.connection = connection;
    }

    public MutationPlan compile(final CreateTableStatement statement, List<Object> binds) throws SQLException {
        final ColumnResolver resolver = FromCompiler.getResolver(statement, connection);
        Scan scan = new Scan();
        final StatementContext context = new StatementContext(statement, connection, resolver, binds, scan);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
        List<ParseNode> splitNodes = statement.getSplitNodes();
        final byte[][] splits = new byte[splitNodes.size()][];
        for (int i = 0; i < splits.length; i++) {
            ParseNode node = splitNodes.get(i);
            if (!node.isConstant()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.SPLIT_POINT_NOT_CONSTANT)
                    .setMessage("Node: " + node).build().buildException();
            }
            LiteralExpression expression = (LiteralExpression)node.accept(expressionCompiler);
            splits[i] = expression.getBytes();
        }
        final MetaDataClient client = new MetaDataClient(connection);
        
        return new MutationPlan() {

            @Override
            public ParameterMetaData getParameterMetaData() {
                return context.getBindManager().getParameterMetaData();
            }

            @Override
            public MutationState execute() throws SQLException {
                return client.createTable(statement, splits);
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
}
