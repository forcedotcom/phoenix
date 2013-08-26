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
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.expression.*;
import com.salesforce.phoenix.expression.visitor.KeyValueExpressionVisitor;
import com.salesforce.phoenix.filter.*;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.parse.HintNode.Hint;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.util.*;


/**
 *
 * Class to build the filter of a scan
 *
 * @author jtaylor
 * @since 0.1
 */
public class WhereCompiler {
    protected static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();

    private WhereCompiler() {
    }

    /**
     * Pushes where clause filter expressions into scan by building and setting a filter.
     * @param context the shared context during query compilation
     * @param statement TODO
     * @throws SQLException if mismatched types are found, bind value do not match binds,
     * or invalid function arguments are encountered.
     * @throws SQLFeatureNotSupportedException if an unsupported expression is encountered.
     * @throws ColumnNotFoundException if column name could not be resolved
     * @throws AmbiguousColumnException if an unaliased column name is ambiguous across multiple tables
     */
    public static Expression compile(StatementContext context, FilterableStatement statement) throws SQLException {
        return compileWhereClause(context, statement, Sets.<Expression>newHashSet());
    }

    /**
     * Used for testing to get access to the expressions that were used to form the start/stop key of the scan
     * @param statement TODO
     */
    public static Expression compileWhereClause(StatementContext context, FilterableStatement statement,
            Set<Expression> extractedNodes) throws SQLException {
        ParseNode where = statement.getWhere();
        if (where == null) {
            return null;
        }
        WhereExpressionCompiler whereCompiler = new WhereExpressionCompiler(context);
        Expression expression = where.accept(whereCompiler);
        if (whereCompiler.isAggregate()) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.AGGREGATE_IN_WHERE).build().buildException();
        }
        expression = WhereOptimizer.pushKeyExpressionsToScan(context, statement, expression, extractedNodes);
        setScanFilter(context, statement, expression, whereCompiler.disambiguateWithFamily);

        return expression;
    }

    private static class WhereExpressionCompiler extends ExpressionCompiler {
        private boolean disambiguateWithFamily;

        WhereExpressionCompiler(StatementContext context) {
            super(context);
        }

        @Override
        protected ColumnRef resolveColumn(ColumnParseNode node) throws SQLException {
            ColumnRef ref = super.resolveColumn(node);
            PTable table = ref.getTable();
            // Track if we need to compare KeyValue during filter evaluation
            // using column family. If the column qualifier is enough, we
            // just use that.
            try {
                if (!SchemaUtil.isPKColumn(ref.getColumn())) {
                    table.getColumn(ref.getColumn().getName().getString());
                }
            } catch (AmbiguousColumnException e) {
                disambiguateWithFamily = true;
            }
            return ref;
        }
    }

    private static final class Counter {
        public enum Count {NONE, SINGLE, MULTIPLE};
        private Count count = Count.NONE;
        private KeyValueColumnExpression column;

        public void increment(KeyValueColumnExpression column) {
            switch (count) {
                case NONE:
                    count = Count.SINGLE;
                    this.column = column;
                    break;
                case SINGLE:
                    count = column.equals(this.column) ? Count.SINGLE : Count.MULTIPLE;
                    break;
                case MULTIPLE:
                    break;

            }
        }
        public Count getCount() {
            return count;
        }
    }

    /**
     * Sets the start/stop key range based on the whereClause expression.
     * @param context the shared context during query compilation
     * @param whereClause the final where clause expression.
     */
    private static void setScanFilter(StatementContext context, FilterableStatement statement, Expression whereClause, boolean disambiguateWithFamily) {
        Filter filter = null;
        Scan scan = context.getScan();
        assert scan.getFilter() == null;

        if (LiteralExpression.FALSE_EXPRESSION == whereClause) {
            context.setScanRanges(ScanRanges.NOTHING);
        } else if (whereClause != null && whereClause != LiteralExpression.TRUE_EXPRESSION) {
            final Counter counter = new Counter();
            whereClause.accept(new KeyValueExpressionVisitor() {

                @Override
                public Iterator<Expression> defaultIterator(Expression node) {
                    // Stop traversal once we've found multiple KeyValue columns
                    if (counter.getCount() == Counter.Count.MULTIPLE) {
                        return Iterators.emptyIterator();
                    }
                    return super.defaultIterator(node);
                }

                @Override
                public Void visit(KeyValueColumnExpression expression) {
                    counter.increment(expression);
                    return null;
                }
            });
            switch (counter.getCount()) {
            case NONE:
                PTable table = context.getResolver().getTables().get(0).getTable();
                byte[] essentialCF = table.getType() == PTableType.VIEW 
                        ? ByteUtil.EMPTY_BYTE_ARRAY 
                        : SchemaUtil.getEmptyColumnFamily(table.getColumnFamilies());
                filter = new RowKeyComparisonFilter(whereClause, essentialCF);
                break;
            case SINGLE:
                filter = disambiguateWithFamily ? new SingleCFCQKeyValueComparisonFilter(whereClause) : new SingleCQKeyValueComparisonFilter(whereClause);
                break;
            case MULTIPLE:
                filter = disambiguateWithFamily ? new MultiCFCQKeyValueComparisonFilter(whereClause) : new MultiCQKeyValueComparisonFilter(whereClause);
                break;
            }
        }

        scan.setFilter(filter);
        ScanRanges scanRanges = context.getScanRanges();
        boolean forcedSkipScan = statement.getHint().hasHint(Hint.SKIP_SCAN);
        boolean forcedRangeScan = statement.getHint().hasHint(Hint.RANGE_SCAN);
        if (forcedSkipScan || (scanRanges.useSkipScanFilter() && !forcedRangeScan)) {
            ScanUtil.andFilterAtBeginning(scan, scanRanges.getSkipScanFilter());
        }
    }
}
