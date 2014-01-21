/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc. All rights reserved. Redistribution and use in source
 * and binary forms, with or without modification, are permitted provided that the following
 * conditions are met: Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. Redistributions in binary form must reproduce
 * the above copyright notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution. Neither the name of
 * Salesforce.com nor the names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission. THIS SOFTWARE IS PROVIDED
 * BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.phoenix.coprocessor;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import com.salesforce.phoenix.protobuf.ProtobufUtil;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.cache.GlobalCache;
import com.salesforce.phoenix.cache.TenantCache;
import com.salesforce.phoenix.coprocessor.ServerCachingProtocol.ServerCacheFactory;
import com.salesforce.phoenix.coprocessor.generated.ServerCachingProtos.AddServerCacheRequest;
import com.salesforce.phoenix.coprocessor.generated.ServerCachingProtos.AddServerCacheResponse;
import com.salesforce.phoenix.coprocessor.generated.ServerCachingProtos.RemoveServerCacheRequest;
import com.salesforce.phoenix.coprocessor.generated.ServerCachingProtos.RemoveServerCacheResponse;
import com.salesforce.phoenix.coprocessor.generated.ServerCachingProtos.ServerCachingService;

public class ServerCachingEndpointImpl extends ServerCachingService implements CoprocessorService,
    Coprocessor {

    private static final Log LOG = LogFactory.getLog(ServerCachingEndpointImpl.class);

  private RegionCoprocessorEnvironment env;

  @Override
  public void addServerCache(RpcController controller, AddServerCacheRequest request,
      RpcCallback<AddServerCacheResponse> done) {
    ImmutableBytesPtr tenantId = null;
    if (request.hasTenantId()) {
      tenantId = new ImmutableBytesPtr(request.getTenantId().toByteArray());
    }
    TenantCache tenantCache = GlobalCache.getTenantCache(this.env, tenantId);
    ImmutableBytesWritable cachePtr =
    		com.salesforce.phoenix.protobuf.ProtobufUtil.toImmutableBytesWritable(request.getCachePtr());

    try {
      @SuppressWarnings("unchecked")
      Class<ServerCacheFactory> serverCacheFactoryClass =
          (Class<ServerCacheFactory>) Class.forName(request.getCacheFactory().getClassName());
      ServerCacheFactory cacheFactory = serverCacheFactoryClass.newInstance();
      tenantCache.addServerCache(new ImmutableBytesPtr(request.getCacheId().toByteArray()),
        cachePtr, cacheFactory);
    } catch (Throwable e) {
      ProtobufUtil.setControllerException(controller, new IOException(e));
    }
    AddServerCacheResponse.Builder responseBuilder = AddServerCacheResponse.newBuilder();
    responseBuilder.setReturn(true);
    AddServerCacheResponse result = responseBuilder.build();
    done.run(result);
  }

  @Override
  public void removeServerCache(RpcController controller, RemoveServerCacheRequest request,
      RpcCallback<RemoveServerCacheResponse> done) {
    ImmutableBytesPtr tenantId = null;
    if (request.hasTenantId()) {
      tenantId = new ImmutableBytesPtr(request.getTenantId().toByteArray());
    }
    TenantCache tenantCache = GlobalCache.getTenantCache(this.env, tenantId);
    try {
      tenantCache.removeServerCache(new ImmutableBytesPtr(request.getCacheId().toByteArray()));
    } catch (SQLException e) {
      ProtobufUtil.setControllerException(controller, new IOException(e));
    }
    RemoveServerCacheResponse.Builder responseBuilder = RemoveServerCacheResponse.newBuilder();
    responseBuilder.setReturn(true);
    RemoveServerCacheResponse result = responseBuilder.build();
    done.run(result);
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment) env;
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
  }

  @Override
  public void stop(CoprocessorEnvironment arg0) throws IOException {
    // nothing to do
  }

  @Override
  public Service getService() {
    return this;
  }
}
