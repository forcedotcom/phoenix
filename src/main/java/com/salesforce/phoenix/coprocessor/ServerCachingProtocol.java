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
package com.salesforce.phoenix.coprocessor;

import java.io.Closeable;
import java.sql.SQLException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.io.Writable;

import com.salesforce.phoenix.memory.MemoryManager.MemoryChunk;

/**
 * 
 * EndPoint coprocessor to send a cache to a region server.
 * Used for:
 * a) hash joins, to send the smaller side of the join to each region server
 * b) secondary indexes, to send the necessary meta data to each region server
 * @author jtaylor
 * @since 0.1
 */
public interface ServerCachingProtocol extends CoprocessorProtocol {
    public static interface ServerCacheFactory extends Writable {
        public Closeable newCache(ImmutableBytesWritable cachePtr, MemoryChunk chunk) throws SQLException;
    }
    /**
     * Add the cache to the region server cache.  
     * @param tenantId the tenantId or null if not applicable
     * @param cacheId unique identifier of the cache
     * @param cache binary representation of cache
     * @return true on success and otherwise throws
     * @throws SQLException 
     */
    public boolean addServerCache(byte[] tenantId, byte[] cacheId, ImmutableBytesWritable cachePtr, ServerCacheFactory elementFactory) throws SQLException;
    /**
     * Remove the cache from the region server cache.  Called upon completion of
     * the operation when cache is no longer needed.
     * @param tenantId the tenantId or null if not applicable
     * @param cacheId unique identifier of the cache
     * @return true on success and otherwise throws
     * @throws SQLException 
     */
    public boolean removeServerCache(byte[] tenantId, byte[] cacheId) throws SQLException;
}