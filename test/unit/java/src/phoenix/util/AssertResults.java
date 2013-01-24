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
package phoenix.util;

import static org.junit.Assert.assertTrue;

import java.util.*;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import phoenix.iterate.ResultIterator;
import phoenix.schema.tuple.Tuple;

/**
 * 
 * Utility class to assert that a scan returns the expected results
 *
 * @author jtaylor
 * @since 0.1
 */
public class AssertResults {
    public static final AssertingIterator NONE = new NoopAssertingIterator();
    
    private AssertResults() {    
    }
    
    public static void assertResults(ResultIterator scanner, Tuple[] results) throws Exception {
        assertResults(scanner,new ResultAssertingIterator(Arrays.asList(results).iterator()));
    }
    
    public static void assertUnorderedResults(ResultIterator scanner, Tuple[] results) throws Exception {
        assertResults(scanner,new UnorderedResultAssertingIterator(Arrays.asList(results)));
    }
    
    public static void assertResults(ResultIterator scanner, AssertingIterator iterator) throws Exception {
        try {
            for (Tuple result = scanner.next(); result != null; result = scanner.next()) {
                iterator.assertNext(result);
            }
            iterator.assertDone();
        } finally {
            scanner.close();
        }
    }
    
    public static interface AssertingIterator {
        public void assertNext(Tuple result) throws Exception;
        public void assertDone() throws Exception;
    }
    
    /**
     * 
     * Use to iterate through results without checking the values against 
     *
     * @author jtaylor
     * @since 0.1
     */
    private static final class NoopAssertingIterator implements AssertingIterator {
        @Override
        public void assertDone() throws Exception {
        }

        @Override
        public void assertNext(Tuple result) throws Exception {
        }
    }
    
    public static class ResultAssertingIterator implements AssertingIterator {
        private final Iterator<Tuple> expectedResults;
        
        public ResultAssertingIterator(Iterator<Tuple> expectedResults) {
            this.expectedResults = expectedResults;
        }
        
        @Override
        public void assertDone() {
            assertTrue(!expectedResults.hasNext());
        }

        @Override
        public void assertNext(Tuple result) throws Exception {
            assertTrue(expectedResults.hasNext());
            Tuple expected = expectedResults.next();
            TestUtil.compareTuples(expected, result);
        }
    }

    public static class UnorderedResultAssertingIterator implements AssertingIterator {
        private final ImmutableBytesWritable tempPtr = new ImmutableBytesWritable();
        private final Map<ImmutableBytesWritable, Tuple> expectedResults;
        
        public UnorderedResultAssertingIterator(Collection<Tuple> expectedResults) {
            this.expectedResults = new HashMap<ImmutableBytesWritable,Tuple>(expectedResults.size());
            for (Tuple result : expectedResults) {
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                result.getKey(ptr);
                this.expectedResults.put(ptr,result);
            }
        }
        
        @Override
        public void assertDone() {
            assertTrue(expectedResults.isEmpty());
        }

        @Override
        public void assertNext(Tuple result) throws Exception {
            result.getKey(tempPtr);
            Tuple expected = expectedResults.remove(tempPtr);
            TestUtil.compareTuples(expected, result);
        }
    }
    
}
