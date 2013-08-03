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
package com.salesforce.phoenix.iterate;

import java.io.*;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.iterate.ParallelIterators.ParallelIteratorFactory;
import com.salesforce.phoenix.memory.MemoryManager;
import com.salesforce.phoenix.memory.MemoryManager.MemoryChunk;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.query.QueryServicesOptions;
import com.salesforce.phoenix.schema.tuple.ResultTuple;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.*;



/**
 * 
 * Result iterator that spools the results of a scan to disk once an in-memory threshold has been reached.
 * If the in-memory threshold is not reached, the results are held in memory with no disk writing perfomed.
 *
 * @author jtaylor
 * @since 0.1
 */
public class SpoolingResultIterator implements PeekingResultIterator {
    private final PeekingResultIterator spoolFrom;
    
    public static class SpoolingResultIteratorFactory implements ParallelIteratorFactory {
        private final QueryServices services;
        
        public SpoolingResultIteratorFactory(QueryServices services) {
            this.services = services;
        }
        @Override
        public PeekingResultIterator newIterator(ResultIterator scanner) throws SQLException {
            return new SpoolingResultIterator(scanner, services);
        }
        
    }

    public SpoolingResultIterator(ResultIterator scanner, QueryServices services) throws SQLException {
        this (scanner, services.getMemoryManager(), services.getProps().getInt(QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, QueryServicesOptions.DEFAULT_SPOOL_THRESHOLD_BYTES));
    }
    
    /**
    * Create a result iterator by iterating through the results of a scan, spooling them to disk once
    * a threshold has been reached. The scanner passed in is closed prior to returning.
    * @param scanner the results of a table scan
    * @param mm memory manager tracking memory usage across threads.
    * @param thresholdBytes the requested threshold.  Will be dialed down if memory usage (as determined by
    *  the memory manager) is exceeded.
    * @throws SQLException
    */
    SpoolingResultIterator(ResultIterator scanner, MemoryManager mm, int thresholdBytes) throws SQLException {
        boolean success = false;
        boolean usedOnDiskIterator = false;
        final MemoryChunk chunk = mm.allocate(0, thresholdBytes);
        File tempFile = null;
        try {
            // Can't be bigger than int, since it's the max of the above allocation
            int size = (int)chunk.getSize();
            tempFile = File.createTempFile("ResultSpooler",".bin");
            DeferredFileOutputStream spoolTo = new DeferredFileOutputStream(size, tempFile) {
                @Override
                protected void thresholdReached() throws IOException {
                    super.thresholdReached();
                    chunk.close();
                }
            };
            DataOutputStream out = new DataOutputStream(spoolTo);
            int maxSize = 0;
            for (Tuple result = scanner.next(); result != null; result = scanner.next()) {
                int length = TupleUtil.write(result, out);
                maxSize = Math.max(length, maxSize);
            }
            spoolTo.close();
            if (spoolTo.isInMemory()) {
                byte[] data = spoolTo.getData();
                chunk.resize(data.length);
                spoolFrom = new InMemoryResultIterator(data, chunk);
            } else {
                spoolFrom = new OnDiskResultIterator(maxSize, spoolTo.getFile());
                usedOnDiskIterator = true;
            }
            success = true;
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        } finally {
            try {
                scanner.close();
            } finally {
                try {
                    if (!usedOnDiskIterator) {
                        tempFile.delete();
                    }
                } finally {
                    if (!success) {
                        chunk.close();
                    }
                }
            }
        }
    }

    @Override
    public Tuple peek() throws SQLException {
        return spoolFrom.peek();
    }

    @Override
    public Tuple next() throws SQLException {
        return spoolFrom.next();
    }
    
    @Override
    public void close() throws SQLException {
        spoolFrom.close();
    }

    /**
     * 
     * Backing result iterator if it was not necessary to spool results to disk.
     *
     * @author jtaylor
     * @since 0.1
     */
    private static class InMemoryResultIterator implements PeekingResultIterator {
        private final MemoryChunk memoryChunk;
        private final byte[] bytes;
        private Tuple next;
        private int offset;
        
        private InMemoryResultIterator(byte[] bytes, MemoryChunk memoryChunk) throws SQLException {
            this.bytes = bytes;
            this.memoryChunk = memoryChunk;
            advance();
        }

        private Tuple advance() throws SQLException {
            if (offset >= bytes.length) {
                return next = null;
            }
            int resultSize = ByteUtil.vintFromBytes(bytes, offset);
            offset += WritableUtils.getVIntSize(resultSize);
            ImmutableBytesWritable value = new ImmutableBytesWritable(bytes,offset,resultSize);
            offset += resultSize;
            Tuple result = new ResultTuple(new Result(value));
            return next = result;
        }
        
        @Override
        public Tuple peek() throws SQLException {
            return next;
        }

        @Override
        public Tuple next() throws SQLException {
            Tuple current = next;
            advance();
            return current;
        }
        
        @Override
        public void close() {
            memoryChunk.close();
        }

        @Override
        public void explain(List<String> planSteps) {
        }
    }
    
    /**
     * 
     * Backing result iterator if results were spooled to disk
     *
     * @author jtaylor
     * @since 0.1
     */
    private static class OnDiskResultIterator implements PeekingResultIterator {
        private final File file;
        private DataInputStream spoolFrom;
        private Tuple next;
        private int maxSize;
        private int bufferIndex;
        private byte[][] buffers = new byte[2][];
        private boolean isClosed;
        
        private OnDiskResultIterator (int maxSize, File file) {
            this.file = file;
            this.maxSize = maxSize;
        }
        
        private synchronized void init() throws IOException {
            if (spoolFrom == null) {
                spoolFrom = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
                // We need two so that we can have a current and a next without them stomping on each other
                buffers[0] = new byte[maxSize];
                buffers[1] = new byte[maxSize];
                advance();
            }
        }
    
        private synchronized void reachedEnd() throws IOException {
            next = null;
            isClosed = true;
            try {
                if (spoolFrom != null) {
                    spoolFrom.close();
                }
            } finally {
                file.delete();
            }
        }
        
        private synchronized Tuple advance() throws IOException {
            if (isClosed) {
                return next;
            }
            int length;
            try {
                length = WritableUtils.readVInt(spoolFrom);
            } catch (EOFException e) {
                reachedEnd();
                return next;
            }
            int totalBytesRead = 0;
            int offset = 0;
            // Alternate between buffers so that the current one is not affected by advancing
            bufferIndex = (bufferIndex + 1) % 2;
            byte[] buffer = buffers [bufferIndex];
            while(totalBytesRead < length) {
                int bytesRead = spoolFrom.read(buffer, offset, length);
                if (bytesRead == -1) {
                    reachedEnd();
                    return next;
                }
                offset += bytesRead;
                totalBytesRead += bytesRead;
            }
            next = new ResultTuple(new Result(new ImmutableBytesWritable(buffer,0,length)));
            return next;
        }
        
        @Override
        public synchronized Tuple peek() throws SQLException {
            try {
                init();
                return next;
            } catch (IOException e) {
                throw ServerUtil.parseServerException(e);
            }
        }
    
        @Override
        public synchronized Tuple next() throws SQLException {
            try {
                init();
                Tuple current = next;
                advance();
                return current;
            } catch (IOException e) {
                throw ServerUtil.parseServerException(e);
            }
        }
        
        @Override
        public synchronized void close() throws SQLException {
            try {
                if (!isClosed) {
                    reachedEnd();
                }
            } catch (IOException e) {
                throw ServerUtil.parseServerException(e);
            }
        }

        @Override
        public void explain(List<String> planSteps) {
        }
    }

    @Override
    public void explain(List<String> planSteps) {
    }
}
