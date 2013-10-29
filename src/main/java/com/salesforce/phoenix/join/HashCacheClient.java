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
package com.salesforce.phoenix.join;

import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.xerial.snappy.Snappy;

import com.salesforce.phoenix.cache.ServerCacheClient;
import com.salesforce.phoenix.cache.ServerCacheClient.ServerCache;
import com.salesforce.phoenix.compile.ScanRanges;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.ExpressionType;
import com.salesforce.phoenix.iterate.ResultIterator;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.query.QueryServicesOptions;
import com.salesforce.phoenix.query.Scanner;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ServerUtil;
import com.salesforce.phoenix.util.TrustedByteArrayOutputStream;
import com.salesforce.phoenix.util.TupleUtil;

/**
 * 
 * Client for adding cache of one side of a join to region servers
 * 
 * @author jtaylor
 * @since 0.1
 */
public class HashCacheClient  {
    private final ServerCacheClient serverCache;
    /**
     * Construct client used to create a serialized cached snapshot of a table and send it to each region server
     * for caching during hash join processing.
     * @param connection the client connection
     */
    public HashCacheClient(PhoenixConnection connection) {
        serverCache = new ServerCacheClient(connection);
    }

    /**
     * Send the results of scanning through the scanner to all
     * region servers for regions of the table that will use the cache
     * that intersect with the minMaxKeyRange.
     * @param scanner scanner for the table or intermediate results being cached
     * @return client-side {@link ServerCache} representing the added hash cache
     * @throws SQLException 
     * @throws MaxServerCacheSizeExceededException if size of hash cache exceeds max allowed
     * size
     */
    public ServerCache addHashCache(ScanRanges keyRanges, Scanner scanner, List<Expression> onExpressions, TableRef cacheUsingTableRef) throws SQLException {
        /**
         * Serialize and compress hashCacheTable
         */
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        serialize(ptr, scanner, onExpressions);
        return serverCache.addServerCache(keyRanges, ptr, new HashCacheFactory(), cacheUsingTableRef);
    }
    
    private void serialize(ImmutableBytesWritable ptr, Scanner scanner, List<Expression> onExpressions) throws SQLException {
        long maxSize = serverCache.getConnection().getQueryServices().getProps().getLong(QueryServices.MAX_SERVER_CACHE_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MAX_SERVER_CACHE_SIZE);
        long estimatedSize = Math.min(scanner.getEstimatedSize(), maxSize);
        if (estimatedSize > Integer.MAX_VALUE) {
            throw new IllegalStateException("Estimated size(" + estimatedSize + ") must not be greater than Integer.MAX_VALUE(" + Integer.MAX_VALUE + ")");
        }
        ResultIterator iterator = scanner.iterator();
        try {
            TrustedByteArrayOutputStream baOut = new TrustedByteArrayOutputStream((int)estimatedSize);
            DataOutputStream out = new DataOutputStream(baOut);
            // Write onExpressions first, for hash key evaluation along with deserialization
            out.writeInt(onExpressions.size());
            for (Expression expression : onExpressions) {
                WritableUtils.writeVInt(out, ExpressionType.valueOf(expression).ordinal());
                expression.write(out);                
            }
            int exprSize = baOut.size() + Bytes.SIZEOF_INT;
            out.writeInt(exprSize);
            int nRows = 0;
            out.writeInt(nRows); // In the end will be replaced with total number of rows            
            for (Tuple result = iterator.next(); result != null; result = iterator.next()) {
                TupleUtil.write(result, out);
                if (baOut.size() > maxSize) {
                    throw new MaxServerCacheSizeExceededException("Size of hash cache (" + baOut.size() + " bytes) exceeds the maximum allowed size (" + maxSize + " bytes)");
                }
                nRows++;
            }
            TrustedByteArrayOutputStream sizeOut = new TrustedByteArrayOutputStream(Bytes.SIZEOF_INT);
            DataOutputStream dataOut = new DataOutputStream(sizeOut);
            try {
                dataOut.writeInt(nRows);
                dataOut.flush();
                byte[] cache = baOut.getBuffer();
                // Replace number of rows written above with the correct value.
                System.arraycopy(sizeOut.getBuffer(), 0, cache, exprSize, sizeOut.size());
                // Reallocate to actual size plus compressed buffer size (which is allocated below)
                int maxCompressedSize = Snappy.maxCompressedLength(baOut.size());
                byte[] compressed = new byte[maxCompressedSize]; // size for worst case
                int compressedSize = Snappy.compress(baOut.getBuffer(), 0, baOut.size(), compressed, 0);
                // Last realloc to size of compressed buffer.
                ptr.set(compressed,0,compressedSize);
            } finally {
                dataOut.close();
            }
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        } finally {
            iterator.close();
        }
    }
}
