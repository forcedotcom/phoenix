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
import com.salesforce.phoenix.parse.BindParseNode;
import com.salesforce.phoenix.parse.CreateSequenceStatement;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.query.QueryServicesOptions;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.MetaDataClient;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PDatum;


public class CreateSequenceCompiler {
    private final PhoenixStatement statement;

    public CreateSequenceCompiler(PhoenixStatement statement) {
        this.statement = statement;
    }
    
    private static class LongDatum implements PDatum {

        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public PDataType getDataType() {
            return PDataType.LONG;
        }

        @Override
        public Integer getByteSize() {
            return PDataType.LONG.getByteSize();
        }

        @Override
        public Integer getMaxLength() {
            return null;
        }

        @Override
        public Integer getScale() {
            return null;
        }

        @Override
        public ColumnModifier getColumnModifier() {
            return null;
        }
        
    }
    private static class IntegerDatum implements PDatum {

        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public PDataType getDataType() {
            return PDataType.INTEGER;
        }

        @Override
        public Integer getByteSize() {
            return PDataType.INTEGER.getByteSize();
        }

        @Override
        public Integer getMaxLength() {
            return null;
        }

        @Override
        public Integer getScale() {
            return null;
        }

        @Override
        public ColumnModifier getColumnModifier() {
            return null;
        }
        
    }
    private static final PDatum LONG_DATUM = new LongDatum();
    private static final PDatum INTEGER_DATUM = new IntegerDatum();

    public MutationPlan compile(final CreateSequenceStatement sequence) throws SQLException {
        ParseNode startsWithNode = sequence.getStartWith();
        ParseNode incrementByNode = sequence.getIncrementBy();
        if (!startsWithNode.isStateless()) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.STARTS_WITH_MUST_BE_CONSTANT)
            .setSchemaName(sequence.getSequenceName().getSchemaName())
            .setTableName(sequence.getSequenceName().getTableName()).build().buildException();
        }
        if (!incrementByNode.isStateless()) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INCREMENT_BY_MUST_BE_CONSTANT)
            .setSchemaName(sequence.getSequenceName().getSchemaName())
            .setTableName(sequence.getSequenceName().getTableName()).build().buildException();
        }
        ParseNode cacheNode = sequence.getCacheSize();
        if (cacheNode != null && !cacheNode.isStateless()) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CACHE_MUST_BE_NON_NEGATIVE_CONSTANT)
            .setSchemaName(sequence.getSequenceName().getSchemaName())
            .setTableName(sequence.getSequenceName().getTableName()).build().buildException();
        }
        
        final PhoenixConnection connection = statement.getConnection();
        final ColumnResolver resolver = FromCompiler.EMPTY_TABLE_RESOLVER;
        
        final StatementContext context = new StatementContext(statement, resolver, statement.getParameters(), new Scan());
        if (startsWithNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode)startsWithNode, LONG_DATUM);
        }
        if (incrementByNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode)incrementByNode, LONG_DATUM);
        }
        if (cacheNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode)cacheNode, INTEGER_DATUM);
        }
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
        
        int cacheSizeValue = connection.getQueryServices().getProps().getInt(QueryServices.SEQUENCE_CACHE_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_SEQUENCE_CACHE_SIZE);
        if (cacheNode != null) {
            Expression cacheSizeExpr = cacheNode.accept(expressionCompiler);
            cacheSizeExpr.evaluate(null, ptr);
            if (ptr.getLength() != 0 && (!cacheSizeExpr.getDataType().isCoercibleTo(PDataType.INTEGER) || (cacheSizeValue = (Integer)PDataType.INTEGER.toObject(ptr)) < 0)) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CACHE_MUST_BE_NON_NEGATIVE_CONSTANT)
                .setSchemaName(sequence.getSequenceName().getSchemaName())
                .setTableName(sequence.getSequenceName().getTableName()).build().buildException();
            }
        }
        final int cacheSize = Math.max(1, cacheSizeValue);
        

        final MetaDataClient client = new MetaDataClient(connection);        
        return new MutationPlan() {           

            @Override
            public MutationState execute() throws SQLException {
                return client.createSequence(sequence, startsWith, incrementBy, cacheSize);
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