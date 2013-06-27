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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.*;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.MinMaxPriorityQueue;
import com.salesforce.phoenix.iterate.OrderedResultIterator.ResultEntry;
import com.salesforce.phoenix.schema.tuple.ResultTuple;
import com.salesforce.phoenix.schema.tuple.Tuple;

public class MappedByteBufferSortedQueue extends AbstractQueue<ResultEntry> {
    private Comparator<ResultEntry> comparator;
    private final int thresholdBytes;
    private List<MappedByteBufferPriorityQueue> queues = new ArrayList<MappedByteBufferPriorityQueue>();
    private MappedByteBufferPriorityQueue currentQueue = null;
    private int currentIndex = 0;
    MinMaxPriorityQueue<IndexedResultEntry> mergedQueue = null;

    public MappedByteBufferSortedQueue(Comparator<ResultEntry> comparator,
            int thresholdBytes) throws IOException {
        this.comparator = comparator;
        this.thresholdBytes = thresholdBytes;
        this.currentQueue = new MappedByteBufferPriorityQueue(0,
                thresholdBytes, comparator);
        this.queues.add(currentQueue);
    }

    @Override
    public boolean offer(ResultEntry e) {
        try {
            boolean isFlush = this.currentQueue.writeResult(e);
            if (isFlush) {
                currentIndex++;
                currentQueue = new MappedByteBufferPriorityQueue(currentIndex,
                        thresholdBytes, comparator);
                queues.add(currentQueue);
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

        return true;
    }

    @Override
    public ResultEntry poll() {
        if (mergedQueue == null) {
            mergedQueue = MinMaxPriorityQueue.<ResultEntry> orderedBy(
                    comparator).maximumSize(queues.size()).create();
            for (MappedByteBufferPriorityQueue queue : queues) {
                try {
                    IndexedResultEntry next = queue.getNextResult();
                    if (next != null) {
                        mergedQueue.add(next);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (!mergedQueue.isEmpty()) {
            IndexedResultEntry re = mergedQueue.pollFirst();
            if (re != null) {
                IndexedResultEntry next = null;
                try {
                    next = queues.get(re.getIndex()).getNextResult();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (next != null) {
                    mergedQueue.add(next);
                }
                return re;
            }
        }
        return null;
    }

    @Override
    public ResultEntry peek() {
        if (mergedQueue == null) {
            mergedQueue = MinMaxPriorityQueue.<ResultEntry> orderedBy(
                    comparator).maximumSize(queues.size()).create();
            for (MappedByteBufferPriorityQueue queue : queues) {
                try {
                    IndexedResultEntry next = queue.getNextResult();
                    if (next != null) {
                        mergedQueue.add(next);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (!mergedQueue.isEmpty()) {
            IndexedResultEntry re = mergedQueue.peekFirst();
            if (re != null) {
                return re;
            }
        }
        return null;
    }

    @Override
    public Iterator<ResultEntry> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        int size = 0;
        for (MappedByteBufferPriorityQueue queue : queues) {
            size += queue.size();
        }
        return size;
    }
    
    public long getByteSize() {
        return currentQueue.getInMemByteSize();
    }

    public void close() {
        if (queues != null) {
            for (MappedByteBufferPriorityQueue queue : queues) {
                queue.close();
            }
        }
    }

    private static class IndexedResultEntry extends ResultEntry {
        private int index;

        public IndexedResultEntry(int index, ResultEntry resultEntry) {
            super(resultEntry.sortKeys, resultEntry.result);
            this.index = index;
        }

        public int getIndex() {
            return this.index;
        }
    }

    private static class MappedByteBufferPriorityQueue {
        private static final long DEFAULT_MAPPING_SIZE = 1024;
        
        private int thresholdBytes;
        private long totalResultSize = 0;
        private int maxResultSize = 0;
        private long mappingSize = 0;
        private long writeIndex = 0;
        private long readIndex = 0;
        private MappedByteBuffer writeBuffer;
        private MappedByteBuffer readBuffer;
        private FileChannel fc;
        private RandomAccessFile af;
        private File file;
        private boolean isClosed = false;
        MinMaxPriorityQueue<ResultEntry> results = null;
        private boolean flushBuffer = false;
        private int index;
        private int flushedCount;

        public MappedByteBufferPriorityQueue(int index, int thresholdBytes,
                Comparator<ResultEntry> comparator) throws IOException {
            this.index = index;
            this.thresholdBytes = thresholdBytes;
            results = MinMaxPriorityQueue.<ResultEntry> orderedBy(comparator)
                    .create();
        }
        
        public int size() {
            if (flushBuffer)
                return flushedCount;
            return results.size();
        }
        
        public long getInMemByteSize() {
            if (flushBuffer)
                return 0;
            return totalResultSize;
        }

        private List<KeyValue> toKeyValues(ResultEntry entry) {
            Tuple result = entry.getResult();
            int size = result.size();
            List<KeyValue> kvs = new ArrayList<KeyValue>(size);
            for (int i = 0; i < size; i++) {
                kvs.add(result.getValue(i));
            }
            return kvs;
        }

        private int sizeof(List<KeyValue> kvs) {
            int size = Bytes.SIZEOF_INT; // totalLen

            for (KeyValue kv : kvs) {
                size += kv.getLength();
                size += Bytes.SIZEOF_INT; // kv.getLength
            }

            return size;
        }

        private int sizeof(ImmutableBytesWritable[] sortKeys) {
            int size = Bytes.SIZEOF_INT;
            if (sortKeys != null) {
                for (ImmutableBytesWritable sortKey : sortKeys) {
                    if (sortKey != null) {
                        size += sortKey.getLength();
                    }
                    size += Bytes.SIZEOF_INT;
                }
            }
            return size;
        }

        public boolean writeResult(ResultEntry entry) throws IOException {
            if (flushBuffer)
                throw new IOException("Results already flushed");
            
            int sortKeySize = sizeof(entry.sortKeys);
            int resultSize = sizeof(toKeyValues(entry)) + sortKeySize;
            results.add(entry);
            maxResultSize = Math.max(maxResultSize, resultSize);
            totalResultSize += resultSize;
            if (totalResultSize >= thresholdBytes) {
                this.file = File.createTempFile(UUID.randomUUID().toString(), null);
                this.af = new RandomAccessFile(file, "rw");
                this.fc = af.getChannel();
                mappingSize = Math.min(Math.max(maxResultSize, DEFAULT_MAPPING_SIZE), totalResultSize);
                writeBuffer = fc.map(MapMode.READ_WRITE, writeIndex, mappingSize);
                
                for (int i = 0; i < results.size(); i++) {                
                    int totalLen = 0;
                    ResultEntry re = results.pollFirst();
                    List<KeyValue> keyValues = toKeyValues(re);
                    for (KeyValue kv : keyValues) {
                        totalLen += (kv.getLength() + Bytes.SIZEOF_INT);
                    }
                    writeBuffer.putInt(totalLen);
                    for (KeyValue kv : keyValues) {
                        writeBuffer.putInt(kv.getLength());
                        writeBuffer.put(kv.getBuffer(), kv.getOffset(), kv
                                .getLength());
                    }
                    ImmutableBytesWritable[] sortKeys = re.sortKeys;
                    writeBuffer.putInt(sortKeys.length);
                    for (ImmutableBytesWritable sortKey : sortKeys) {
                        if (sortKey != null) {
                            writeBuffer.putInt(sortKey.getLength());
                            writeBuffer.put(sortKey.get(), sortKey.getOffset(),
                                    sortKey.getLength());
                        } else {
                            writeBuffer.putInt(0);
                        }
                    }
                    // buffer close to exhausted, re-map.
                    if (mappingSize - writeBuffer.position() < maxResultSize) {
                        writeIndex += writeBuffer.position();
                        writeBuffer = fc.map(MapMode.READ_WRITE, writeIndex, mappingSize);
                    }
                }
                writeBuffer.putInt(-1); // end
                flushedCount = results.size();
                results.clear();
                flushBuffer = true;
            }
            return flushBuffer;
        }

        public IndexedResultEntry getNextResult() throws IOException {
            if (isClosed)
                return null;
            
            if (!flushBuffer) {
                ResultEntry re = results.poll();
                if (re == null) {
                    reachedEnd();
                    return null;
                }
                return new IndexedResultEntry(index, re);
            }
            
            if (readBuffer == null) {
                readBuffer = this.fc.map(MapMode.READ_ONLY, readIndex, mappingSize);
            }
            
            int length = readBuffer.getInt();
            if (length < 0) {
                reachedEnd();
                return null;
            }
            
            byte[] rb = new byte[length];
            readBuffer.get(rb);
            Result result = new Result(new ImmutableBytesWritable(rb));
            ResultTuple rt = new ResultTuple(result);
            int sortKeySize = readBuffer.getInt();
            ImmutableBytesWritable[] sortKeys = new ImmutableBytesWritable[sortKeySize];
            for (int i = 0; i < sortKeySize; i++) {
                int contentLength = readBuffer.getInt();
                if (contentLength > 0) {
                    byte[] sortKeyContent = new byte[contentLength];
                    readBuffer.get(sortKeyContent);
                    sortKeys[i] = new ImmutableBytesWritable(sortKeyContent);
                } else {
                    sortKeys[i] = null;
                }
            }
            // buffer close to exhausted, re-map.
            if (mappingSize - readBuffer.position() < maxResultSize) {
                readIndex += readBuffer.position();
                readBuffer = fc.map(MapMode.READ_ONLY, readIndex, mappingSize);
            }
            
            return new IndexedResultEntry(index, new ResultEntry(sortKeys, rt));
        }

        private void reachedEnd() {
            this.isClosed = true;
            if (this.fc != null) {
                try {
                    this.fc.close();
                } catch (IOException ignored) {
                }
                this.fc = null;
            }
            if (this.af != null) {
                try {
                    this.af.close();
                } catch (IOException ignored) {
                }
                this.af = null;
            }
            if (this.file != null) {
                file.delete();
                file = null;
            }
        }

        public void close() {
            if (!isClosed) {
                this.reachedEnd();
            }
        }
    }
}
