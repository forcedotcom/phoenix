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
package com.salesforce.phoenix.memory;

import org.apache.http.annotation.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * Global memory manager to track course grained memory usage across all requests.
 *
 * @author jtaylor
 * @since 0.1
 */
public class GlobalMemoryManager implements MemoryManager {
    private static final Logger logger = LoggerFactory.getLogger(GlobalMemoryManager.class);
    
    private final Object sync = new Object();
    private final long maxMemoryBytes;
    private final int maxWaitMs;
    @GuardedBy("sync")
    private volatile long usedMemoryBytes;
    
    public GlobalMemoryManager(long maxBytes, int maxWaitMs) {
        if (maxBytes <= 0) {
            throw new IllegalStateException("Total number of available bytes (" + maxBytes + ") must be greater than zero");
        }
        if (maxWaitMs < 0) {
            throw new IllegalStateException("Maximum wait time (" + maxWaitMs + ") must be greater than or equal to zero");
        }
        this.maxMemoryBytes = maxBytes;
        this.maxWaitMs = maxWaitMs;
        this.usedMemoryBytes = 0;
    }
    
    @Override
    public long getAvailableMemory() {
        synchronized(sync) {
            return maxMemoryBytes - usedMemoryBytes;
        }
    }

    @Override
    public long getMaxMemory() {
        return maxMemoryBytes;
    }


    // TODO: Work on fairness: One big memory request can cause all others to block here.
    private long allocateBytes(long minBytes, long reqBytes) {
        if (minBytes < 0 || reqBytes < 0) {
            throw new IllegalStateException("Minimum requested bytes (" + minBytes + ") and requested bytes (" + reqBytes + ") must be greater than zero");
        }
        if (minBytes > maxMemoryBytes) { // No need to wait, since we'll never have this much available
            throw new InsufficientMemoryException("Requested memory of " + minBytes + " bytes is larger than global pool of " + maxMemoryBytes + " bytes.");
        }
        long startTimeMs = System.currentTimeMillis(); // Get time outside of sync block to account for waiting for lock
        long nBytes;
        synchronized(sync) {
            while (usedMemoryBytes + minBytes > maxMemoryBytes) { // Only wait if minBytes not available
                try {
                    long remainingWaitTimeMs = maxWaitMs - (System.currentTimeMillis() - startTimeMs);
                    if (remainingWaitTimeMs <= 0) { // Ran out of time waiting for some memory to get freed up
                        throw new InsufficientMemoryException("Requested memory of " + minBytes + " bytes could not be allocated from remaining memory of " + usedMemoryBytes + " bytes from global pool of " + maxMemoryBytes + " bytes after waiting for " + maxWaitMs + "ms.");
                    }
                    sync.wait(remainingWaitTimeMs);
                } catch (InterruptedException ie) {
                    throw new RuntimeException("Interrupted allocation of " + minBytes + " bytes", ie);
                }
            }
            // Allocate at most reqBytes, but at least minBytes
            nBytes = Math.min(reqBytes, maxMemoryBytes - usedMemoryBytes);
            if (nBytes < minBytes) {
                throw new IllegalStateException("Allocated bytes (" + nBytes + ") should be at least the minimum requested bytes (" + minBytes + ")");
            }
            usedMemoryBytes += nBytes;
        }
        return nBytes;
    }

    @Override
    public MemoryChunk allocate(long minBytes, long reqBytes) {
        long nBytes = allocateBytes(minBytes, reqBytes);
        return newMemoryChunk(nBytes);
    }

    @Override
    public MemoryChunk allocate(long nBytes) {
        return allocate(nBytes,nBytes);
    }

    protected MemoryChunk newMemoryChunk(long sizeBytes) {
        return new GlobalMemoryChunk(sizeBytes);
    }
    
    private class GlobalMemoryChunk implements MemoryChunk {
        private volatile long size;

        private GlobalMemoryChunk(long size) {
            if (size < 0) {
                throw new IllegalStateException("Size of memory chunk must be greater than zero, but instead is " + size);
            }
            this.size = size;
        }

        @Override
        public long getSize() {
            synchronized(sync) {
                return size; // TODO: does this need to be synchronized?
            }
        }
        
        @Override
        public void resize(long nBytes) {
            if (nBytes < 0) {
                throw new IllegalStateException("Number of bytes to resize to must be greater than zero, but instead is " + nBytes);
            }
            synchronized(sync) {
                long nAdditionalBytes = (nBytes - size);
                if (nAdditionalBytes < 0) {
                    usedMemoryBytes += nAdditionalBytes;
                    size = nBytes;
                    sync.notifyAll();
                } else {
                    allocateBytes(nAdditionalBytes, nAdditionalBytes);
                    size = nBytes;
                }
            }
        }
        
        /**
         * Check that MemoryChunk has previously been closed.
         */
        @Override
        protected void finalize() throws Throwable {
            try {
                close();
                if (size > 0) {
                    logger.warn("Orphaned chunk of " + size + " bytes found during finalize");
                }
                // TODO: log error here, but we can't use SFDC logging
                // because this runs in an hbase coprocessor.
                // Create a gack-like API (talk with GridForce or HBase folks)
            } finally {
                super.finalize();
            }
        }
        
        @Override
        public void close() {
            synchronized(sync) {
                usedMemoryBytes -= size;
                size = 0;
                sync.notifyAll();
            }
        }
    }
}
 
