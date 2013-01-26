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
package phoenix.iterate;

import static phoenix.query.QueryConstants.SINGLE_COLUMN;
import static phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;

import java.sql.SQLException;
import java.util.*;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import phoenix.schema.tuple.SingleKeyValueTuple;
import phoenix.schema.tuple.Tuple;
import phoenix.util.AssertResults;

public class MergeSortResultIteratorTest {
    private final static byte[] A = Bytes.toBytes("a");
    private final static byte[] B = Bytes.toBytes("b");

    @Test
    public void testMergeSort() throws Throwable {
        Tuple[] results1 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
            };
        Tuple[] results2 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1)))
            };
        Tuple[] results3 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
            };
        final List<PeekingResultIterator>results = new ArrayList<PeekingResultIterator>(Arrays.asList(new PeekingResultIterator[] {new MaterializedResultIterator(Arrays.asList(results1)), new MaterializedResultIterator(Arrays.asList(results2)), new MaterializedResultIterator(Arrays.asList(results3))}));

        Tuple[] expectedResults = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
            };

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
        ResultIterator scanner = new MergeSortResultIterator(iterators);
        AssertResults.assertResults(scanner, expectedResults);
    }

}
