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
package com.salesforce.phoenix.query;

import java.util.concurrent.ExecutorService;

import com.salesforce.phoenix.job.JobManager;
import com.salesforce.phoenix.memory.GlobalMemoryManager;
import com.salesforce.phoenix.memory.MemoryManager;
import com.salesforce.phoenix.optimize.QueryOptimizer;
import com.salesforce.phoenix.util.ReadOnlyProps;



/**
 * 
 * Base class for QueryService implementors.
 *
 * @author jtaylor
 * @since 0.1
 */
public abstract class BaseQueryServicesImpl implements QueryServices {
    private final ExecutorService executor;
    private final MemoryManager memoryManager;
    private final ReadOnlyProps props;
    private final QueryOptimizer queryOptimizer;
    
    public BaseQueryServicesImpl(QueryServicesOptions options) {
        this.executor =  JobManager.createThreadPoolExec(
                options.getKeepAliveMs(), 
                options.getThreadPoolSize(), 
                options.getQueueSize());
        this.memoryManager = new GlobalMemoryManager(
                Runtime.getRuntime().totalMemory() * options.getMaxMemoryPerc() / 100,
                options.getMaxMemoryWaitMs());
        this.props = options.getProps();
        this.queryOptimizer = new QueryOptimizer(this);
    }
    
    @Override
    public ExecutorService getExecutor() {
        return executor;
    }

    @Override
    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    @Override
    public final ReadOnlyProps getProps() {
        return props;
    }

    @Override
    public void close() {
    }

    @Override
    public QueryOptimizer getOptimizer() {
        return queryOptimizer;
    }   
}
