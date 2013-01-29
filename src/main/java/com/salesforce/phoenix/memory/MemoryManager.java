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

import java.io.Closeable;

/**
 * 
 * Memory manager used to track memory usage.  Either throttles
 * memory usage by blocking when the max memory is reached or
 * allocates up to a maximum without blocking.
 * 
 * @author jtaylor
 * @since 0.1
 */
public interface MemoryManager {
    /**
     * Get the total amount of memory (in bytes) that may be allocated.
     */
    long getMaxMemory();
    
    /**
     * Get the amount of available memory (in bytes) not yet allocated.
     */
    long getAvailableMemory();
    
    /**
     * Allocate up to reqBytes of memory, dialing the amount down to 
     * minBytes if full amount is not available.  If minBytes is not
     * available, then this call will block for a configurable amount
     * of time and throw if minBytes does not become available.
     * @param minBytes minimum number of bytes required
     * @param reqBytes requested number of bytes.  Must be greater
     * than or equal to minBytes
     * @return MemoryChunk that was allocated
     * @throws InsufficientMemoryException if unable to allocate minBytes
     *  during configured amount of time
     */
    MemoryChunk allocate(long minBytes, long reqBytes);

    /**
     * Equivalent to calling {@link #allocate(long, long)} where
     * minBytes and reqBytes being the same.
     */
    MemoryChunk allocate(long nBytes);
    
    /**
     * 
     * Chunk of allocated memory.  To reclaim the memory, call {@link #close()}
     *
     * @author jtaylor
     * @since 0.1
     */
    public static interface MemoryChunk extends Closeable {
        /**
         * Get the size in bytes of the allocated chunk.
         */
        long getSize();
        
        /**
         * Free up the memory associated with this chunk
         */
        @Override
        void close();
        
        /**
         * Resize an already allocated memory chunk up or down to a
         * new amount.  If decreasing allocation, this call will not block.
         * If increasing allocation, and nBytes is not available,  then
         * this call will block for a configurable amount of time and
         * throw if nBytes does not become available.  Most commonly
         * used to adjust the allocation of a memory buffer that was
         * originally sized for the worst case scenario.
         * @param nBytes new number of bytes required for this chunk
         * @throws InsufficientMemoryException if unable to allocate minBytes
         *  during configured amount of time
         */
        void resize(long nBytes); 
    }
}
