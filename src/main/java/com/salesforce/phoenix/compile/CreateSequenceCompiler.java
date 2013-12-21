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

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.execute.MutationState;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.jdbc.PhoenixStatement;
import com.salesforce.phoenix.parse.CreateSequenceStatement;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.schema.MetaDataClient;
import com.salesforce.phoenix.schema.PDataType;


public class CreateSequenceCompiler {
    private final PhoenixStatement statement;

    public CreateSequenceCompiler(PhoenixStatement statement) {
        this.statement = statement;
    }

    public MutationPlan compile(final CreateSequenceStatement sequence) throws SQLException {
        ParseNode startsWithNode = sequence.getStartWith();
        ParseNode incrementByNode = sequence.getIncrementBy();
        if (!startsWithNode.isConstant()) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.STARTS_WITH_MUST_BE_CONSTANT)
            .setSchemaName(sequence.getSequenceName().getSchemaName())
            .setTableName(sequence.getSequenceName().getTableName()).build().buildException();
        }
        if (!incrementByNode.isConstant()) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INCREMENT_BY_MUST_BE_CONSTANT)
            .setSchemaName(sequence.getSequenceName().getSchemaName())
            .setTableName(sequence.getSequenceName().getTableName()).build().buildException();
        }
        
        final PhoenixConnection connection = statement.getConnection();
        final ColumnResolver resolver = FromCompiler.EMPTY_TABLE_RESOLVER;
        
        final StatementContext context = new StatementContext(sequence, connection, resolver, statement.getParameters(), new Scan());
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
        Expression startsWithExpr = startsWithNode.accept(expressionCompiler);
        ImmutableBytesWritable ptr = context.getTempPtr();
        startsWithExpr.evaluate(null, ptr);
        if (ptr.getLength() == 0 || !startsWithExpr.getDataType().isCoercibleTo(PDataType.LONG)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.STARTS_WITH_MUST_BE_CONSTANT)
            .setSchemaName(sequence.getSequenceName().getSchemaName())
            .setTableName(sequence.getSequenceName().getTableName()).build().buildException();
        }
        final long startsWith = (Long)PDataType.LONG.toObject(ptr, startsWithExpr.getDataType());

        Expression incrementByExpr = incrementByNode.accept(expressionCompiler);
        incrementByExpr.evaluate(null, ptr);
        if (ptr.getLength() == 0 || !incrementByExpr.getDataType().isCoercibleTo(PDataType.LONG)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INCREMENT_BY_MUST_BE_CONSTANT)
            .setSchemaName(sequence.getSequenceName().getSchemaName())
            .setTableName(sequence.getSequenceName().getTableName()).build().buildException();
        }
        final long incrementBy = (Long)PDataType.LONG.toObject(ptr, incrementByExpr.getDataType());
        
        final MetaDataClient client = new MetaDataClient(connection);        
        return new MutationPlan() {           

            @Override
            public MutationState execute() throws SQLException {
                return client.createSequence(sequence, startsWith, incrementBy);
            }

            @Override
            public ExplainPlan getExplainPlan() throws SQLException {
                return new ExplainPlan(Collections.singletonList("CREATE SEQUENCE"));
            }

            @Override
            public PhoenixConnection getConnection() {
                return connection;
            }

            @Override
            public ParameterMetaData getParameterMetaData() {                
                return context.getBindManager().getParameterMetaData();
            }

        };
    }
}