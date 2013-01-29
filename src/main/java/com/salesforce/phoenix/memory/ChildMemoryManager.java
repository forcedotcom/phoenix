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
import org.apache.http.annotation.ThreadSafe;

/**
 * 
 * Child memory manager that delegates through to global memory manager,
 * but enforces that at most a threshold percentage is used by this
 * memory manager.  No blocking is done if the threshold is exceeded,
 * but the standard blocking will be done by the global memory manager.
 *
 * @author jtaylor
 * @since 0.1
 */
@ThreadSafe
public class ChildMemoryManager extends DelegatingMemoryManager {
    private final Object sync = new Object();
    private final int maxPercOfTotal;
    @GuardedBy("sync")
    private long allocatedBytes;
    
    public ChildMemoryManager(MemoryManager mm, int maxPercOfTotal) {
        super(mm);
        if (mm instanceof ChildMemoryManager) {
            throw new IllegalStateException("ChildMemoryManager cannot delegate to another ChildMemoryManager");
        }
        this.maxPercOfTotal = maxPercOfTotal;
        if (maxPercOfTotal <= 0 || maxPercOfTotal > 100) {
            throw new IllegalArgumentException("Max percentage of total memory (" + maxPercOfTotal + "%) must be greater than zero and less than or equal to 100");
        }
    }


    private long adjustAllocation(long minBytes, long reqBytes) {
        assert(reqBytes >= minBytes);
        long availBytes = getAvailableMemory();
        // Check if this memory managers percentage of allocated bytes exceeds its allowed maximum
        if (minBytes > availBytes) {
            throw new InsufficientMemoryException("Attempt to allocate more memory than the max allowed of " + maxPercOfTotal + "%");
        }
        // Revise reqBytes down to available memory if necessary
        return Math.min(reqBytes,availBytes);
    }
    
    @Override
    public MemoryChunk allocate(long minBytes, long nBytes) {
        synchronized (sync) {
            nBytes = adjustAllocation(minBytes, nBytes);
            final MemoryChunk chunk = super.allocate(minBytes, nBytes);
            allocatedBytes += chunk.getSize();
            // Instantiate delegate chunk to track allocatedBytes correctly
            return new MemoryChunk() {
                @Override
                public void close() {
                    synchronized (sync) {
                        allocatedBytes -= chunk.getSize();
                        chunk.close();
                    }
                }
    
                @Override
                public long getSize() {
                    return chunk.getSize();
                }
    
                @Override
                public void resize(long nBytes) {
                    synchronized (sync) {
                        long size = getSize();
                        long deltaBytes = nBytes - size;
                        if (deltaBytes > 0) {
                            adjustAllocation(deltaBytes,deltaBytes); // Throw if too much memory
                        }
                        chunk.resize(nBytes);
                        allocatedBytes += deltaBytes;
                    }
                }
            };
        }
    }

    @Override
    public long getAvailableMemory() {
        synchronized (sync) {
            long availBytes = getMaxMemory() - allocatedBytes;
            // Sanity check (should never happen)
            if (availBytes < 0) {
                throw new IllegalStateException("Available memory has become negative: " + availBytes + " bytes.  Allocated memory: " + allocatedBytes + " bytes.");
            }
            return availBytes;
        }
    }
    
    @Override
    public long getMaxMemory() {
        return maxPercOfTotal  * super.getMaxMemory() / 100;
    }
}
