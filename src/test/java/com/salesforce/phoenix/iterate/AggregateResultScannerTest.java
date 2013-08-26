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
package com.salesforce.phoenix.iterate;

import static com.salesforce.phoenix.query.QueryConstants.*;
import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.io.*;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.salesforce.phoenix.compile.AggregationManager;
import com.salesforce.phoenix.compile.StatementContext;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.KeyValueColumnExpression;
import com.salesforce.phoenix.expression.aggregator.ClientAggregators;
import com.salesforce.phoenix.expression.function.SingleAggregateFunction;
import com.salesforce.phoenix.expression.function.SumAggregateFunction;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.query.BaseConnectionlessQueryTest;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.schema.tuple.SingleKeyValueTuple;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.AssertResults;



public class AggregateResultScannerTest extends BaseConnectionlessQueryTest {
    private final static byte[] A = Bytes.toBytes("a");
    private final static byte[] B = Bytes.toBytes("b");

    @Test
    public void testAggregatingMergeSort() throws Throwable {
        Tuple[] results1 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, PDataType.LONG.toBytes(1L))),
            };
        Tuple[] results2 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, PDataType.LONG.toBytes(1L)))
            };
        Tuple[] results3 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, PDataType.LONG.toBytes(1L))),
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, PDataType.LONG.toBytes(1L))),
            };
        Tuple[] results4 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, PDataType.LONG.toBytes(1L))),
            };
        final List<PeekingResultIterator>results = new ArrayList<PeekingResultIterator>(Arrays.asList(new PeekingResultIterator[] {
                new MaterializedResultIterator(Arrays.asList(results1)), 
                new MaterializedResultIterator(Arrays.asList(results2)), 
                new MaterializedResultIterator(Arrays.asList(results3)), 
                new MaterializedResultIterator(Arrays.asList(results4))}));

        Tuple[] expectedResults = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, PDataType.LONG.toBytes(3L))),
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, PDataType.LONG.toBytes(2L))),
            };

        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        StatementContext context = new StatementContext(SelectStatement.COUNT_ONE, pconn, null, Collections.emptyList(), new Scan());
        AggregationManager aggregationManager = context.getAggregationManager();
        SumAggregateFunction func = new SumAggregateFunction(Arrays.<Expression>asList(new KeyValueColumnExpression(new PLongColumn() {
            @Override
            public PName getName() {
                return SINGLE_COLUMN_NAME;
            }
            @Override
            public PName getFamilyName() {
                return SINGLE_COLUMN_FAMILY_NAME;
            }
            @Override
            public int getPosition() {
                return 0;
            }
            
            @Override
            public ColumnModifier getColumnModifier() {
            	return null;
            }
            
            @Override
            public void readFields(DataInput arg0) throws IOException {
            }
            @Override
            public void write(DataOutput arg0) throws IOException {
            }
        })), null);
        aggregationManager.setAggregators(new ClientAggregators(Collections.<SingleAggregateFunction>singletonList(func), 1));
        ResultIterators iterators = new ResultIterators() {

            @Override
            public List<PeekingResultIterator> getIterators() throws SQLException {
                return results;
            }

            @Override
            public int size() {
                return results.size();
            }

            @Override
            public void explain(List<String> planSteps) {
            }
            
        };
        ResultIterator scanner = new GroupedAggregatingResultIterator(new MergeSortRowKeyResultIterator(iterators), aggregationManager.getAggregators());
        AssertResults.assertResults(scanner, expectedResults);
    }
}
