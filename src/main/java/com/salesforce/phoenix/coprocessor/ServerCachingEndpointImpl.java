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

import java.sql.SQLException;

import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.cache.GlobalCache;
import com.salesforce.phoenix.cache.TenantCache;




/**
 * 
 * Server-side implementation of {@link ServerCachingProtocol}
 *
 * @author jtaylor
 * @since 0.1
 */
public class ServerCachingEndpointImpl extends BaseEndpointCoprocessor implements ServerCachingProtocol {

    @Override
    public boolean addServerCache(byte[] tenantId, byte[] cacheId, ImmutableBytesWritable cachePtr, ServerCacheFactory cacheFactory) throws SQLException {
        TenantCache tenantCache = GlobalCache.getTenantCache((RegionCoprocessorEnvironment)this.getEnvironment(), tenantId == null ? null : new ImmutableBytesPtr(tenantId));
        tenantCache.addServerCache(new ImmutableBytesPtr(cacheId), cachePtr, cacheFactory);
        return true;
    }

    @Override
    public boolean removeServerCache(byte[] tenantId, byte[] cacheId) throws SQLException {
        TenantCache tenantCache = GlobalCache.getTenantCache((RegionCoprocessorEnvironment)this.getEnvironment(), tenantId == null ? null : new ImmutableBytesPtr(tenantId));
        tenantCache.removeServerCache(new ImmutableBytesPtr(cacheId));
        return true;
    }
}
