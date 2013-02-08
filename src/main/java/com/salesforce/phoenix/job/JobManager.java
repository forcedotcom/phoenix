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
package com.salesforce.phoenix.job;

import java.util.concurrent.*;

/**
 * 
 * Thread pool executor that executes scans in parallel
 *
 * @author jtaylor
 * @since 0.1
 */
@SuppressWarnings("rawtypes")
public class JobManager<T> extends AbstractRoundRobinQueue<T> {
    public JobManager(int maxSize) {
        super(maxSize, true); // true -> new producers move to front of queue; this reduces latency.
    }

	@Override
    protected Object extractProducer(T o) {
        return ((JobFutureTask)o).getJobId();
    }        

    public static interface JobRunnable<T> extends Runnable {
        public Object getJobId();
    }

    public static ThreadPoolExecutor createThreadPoolExec(int keepAliveMs, int size, int queueSize) {
        BlockingQueue<Runnable> queue;
        if (queueSize == 0) {
            queue = new SynchronousQueue<Runnable>(); // Specialized for 0 length.
        } else {
            queue = new JobManager<Runnable>(queueSize);
        }
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        // For thread pool, set core threads = max threads -- we don't ever want to exceed core threads, but want to go up to core threads *before* using the queue.
        ThreadPoolExecutor exec = new ThreadPoolExecutor(size, size, keepAliveMs, TimeUnit.MILLISECONDS, queue, threadFactory) {
            @Override
            protected <T> RunnableFuture<T> newTaskFor(Callable<T> call) {
                // Override this so we can create a JobFutureTask so we can extract out the parentJobId (otherwise, in the default FutureTask, it is private). 
                return new JobFutureTask<T>(call);
            }
    
            @Override
            protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
                return new JobFutureTask<T>((JobRunnable)runnable, value);
            }
            
        };
        
        exec.allowCoreThreadTimeOut(true); // ... and allow core threads to time out.  This just keeps things clean when idle, and is nice for ftests modes, etc., where we'd especially like these not to linger.
        return exec;
    }

    /**
     * Subclasses FutureTask for the sole purpose of providing {@link #getCallable()}, which is used to extract the producer in the {@link JobBasedRoundRobinQueue}
     */
    static class JobFutureTask<T> extends FutureTask<T> {
        private final Object jobId;
        
        public JobFutureTask(JobRunnable r, T t) {
            super(r, t);
            this.jobId = r.getJobId();
        }
        
        public JobFutureTask(Callable<T> c) {
            super(c);
            // FIXME: this fails when executor used by hbase
            if (c instanceof JobCallable) {
                this.jobId = ((JobCallable<T>) c).getJobId();
            } else {
                this.jobId = this;
            }
        }
        
        public Object getJobId() {
            return jobId;
        }
    }


    /**
     * Delegating callable implementation that preserves the parentJobId and sets up thread tracker stuff before delegating to the actual command. 
     */
    public static interface JobCallable<T> extends Callable<T> {
        public Object getJobId();
    }
}

