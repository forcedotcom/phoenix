package com.salesforce.phoenix.iterate;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.MinMaxPriorityQueue;
import com.salesforce.phoenix.iterate.OrderedResultIterator.ResultEntry;
import com.salesforce.phoenix.schema.tuple.ResultTuple;
import com.salesforce.phoenix.schema.tuple.Tuple;

public class MappedByteBufferSortedQueue extends AbstractQueue<ResultEntry> {
  private int flushSize = 10000; //TODO make this number configurable.
  private Comparator<ResultEntry> comparator;
  private List<MappedByteBufferPriorityQueue> queues = new ArrayList<MappedByteBufferPriorityQueue>();
  private MappedByteBufferPriorityQueue currentQueue = null;
  private int currentIndex = 0;
  MinMaxPriorityQueue<IndexedResultEntry> mergedQueue = null;
  
  public MappedByteBufferSortedQueue(Comparator<ResultEntry> comparator) throws IOException {
    this.comparator = comparator;
    this.currentQueue = new MappedByteBufferPriorityQueue(0, getFileName(), flushSize, comparator);
  }

  private String getFileName(){
    return "/tmp/" + UUID.randomUUID().toString();
  }
  
  @Override
  public boolean offer(ResultEntry e) {
    try {
      boolean isFlush = this.currentQueue.writeResult(e);
      if(isFlush) {
        queues.add(currentQueue);
        currentIndex++;
        currentQueue = new MappedByteBufferPriorityQueue(currentIndex, getFileName(), flushSize, comparator);
        currentQueue.writeResult(e);
      }
    } catch (IOException e1) {
      return false;
    }
    
    return true;
  }

  @Override
  public ResultEntry poll() {
    if(mergedQueue==null) {
      mergedQueue = MinMaxPriorityQueue.<ResultEntry> orderedBy(comparator)
          .maximumSize(queues.size()).create();
      for(MappedByteBufferPriorityQueue queue: queues) {
        try {
          mergedQueue.add(queue.getNextResult());
        } catch (IOException e) {
          return null;
        }
      }
    }
    if(!mergedQueue.isEmpty()) {
      IndexedResultEntry re = mergedQueue.pollFirst();
      if(re!=null) {
        IndexedResultEntry next = null;
        try {
          next = queues.get(re.getIndex()).getNextResult();
        } catch (IOException e) {
          return null;
        }
         if(next != null) {
           mergedQueue.add(next);
         }
         return re;
      }
    }
    return null;
  }

  @Override
  public ResultEntry peek() {
    if(mergedQueue==null) {
      mergedQueue = MinMaxPriorityQueue.<ResultEntry> orderedBy(comparator)
          .maximumSize(queues.size()).create();
      for(MappedByteBufferPriorityQueue queue: queues) {
        try {
          mergedQueue.add(queue.getNextResult());
        } catch (IOException e) {
          return null;
        }
      }
    }
    if(!mergedQueue.isEmpty()) {
      IndexedResultEntry re = mergedQueue.peekFirst();
      if(re!=null) {
         return re;
      }
    }
    return null;
  }

  @Override
  public Iterator<ResultEntry> iterator() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int size() {
    int size = 0;
    for(MappedByteBufferPriorityQueue queue : queues) {
      size += queue.results.size();
    }
    return size;
  }
  
  public void close() {
    if (queues != null) {
      for (MappedByteBufferPriorityQueue queue : queues) {
        queue.close();
      }
    }
  }
  
  private static class IndexedResultEntry extends ResultEntry{
    private ResultEntry resultEntry;
    private int index;
    
    public IndexedResultEntry(int index, ResultEntry resultEntry) {
      super(resultEntry.sortKeys, resultEntry.result);
      this.index = index;
      this.resultEntry = resultEntry;
    }
    
    @SuppressWarnings("unused")
    public ResultEntry getResultEntry(){
      return this.resultEntry;
    }
    public int getIndex(){
      return this.index;
    }
  }

  private static class MappedByteBufferPriorityQueue {
    int mappingSize;
    long allReadSize = 0;
    long allWriteSize = 0;
    long writeIndex = 0, readIndex = 0;
    private MappedByteBuffer writeBuffer, readBuffer;
    private FileChannel fc;
    private RandomAccessFile af;
    private File file;
    private boolean stop = false;
    MinMaxPriorityQueue<ResultEntry> results = null;
    private boolean flushBuffer = false;
//    private ImmutableBytesWritable[] sortKeys;
    private int index;

    public MappedByteBufferPriorityQueue(int index, String fileName, int resultMappingSize, Comparator<ResultEntry> comparator) throws IOException {
      this.index = index;
      this.file = new File(fileName);
      this.af = new RandomAccessFile(file, "rw");
      this.fc = af.getChannel();
      this.mappingSize = resultMappingSize;
      results = MinMaxPriorityQueue.<ResultEntry>orderedBy(comparator).create();
      writeBuffer = fc.map(MapMode.READ_WRITE, this.writeIndex,
          this.mappingSize);
      readBuffer = fc.map(MapMode.READ_ONLY, this.readIndex, this.mappingSize);
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
    
    private int sizeof(ImmutableBytesWritable[]sortKeys) {
      int size = Bytes.SIZEOF_INT;
      if(sortKeys!=null) {
        for(ImmutableBytesWritable sortKey: sortKeys) {
          size += sortKey.getLength();
          size += Bytes.SIZEOF_INT;
        }
      }
      return size;
    }
    
    public boolean writeResult(ResultEntry entry) throws IOException {
      int sortKeySize = sizeof(entry.sortKeys);
      int resultSize = sizeof(toKeyValues(entry)) + sortKeySize;
      if (resultSize >= mappingSize) {
        throw new IOException("Result is too large, buffer size is["
            + mappingSize + "], and result size is[" + resultSize + "].");
      }
      if (allWriteSize + resultSize >= writeIndex + mappingSize) {
        writeBuffer.force();
        writeIndex = allWriteSize;
        writeBuffer = fc.map(MapMode.READ_WRITE, writeIndex, mappingSize);
        for(int i=0;i<results.size();i++) {
          int totalLen = 0;
          ResultEntry re = results.pollFirst();
          List<KeyValue> keyValues = toKeyValues(re);
          for (KeyValue kv : keyValues) {
            totalLen = kv.getLength() + Bytes.SIZEOF_INT;
          }
          writeBuffer.putInt(totalLen);
          allWriteSize += Bytes.SIZEOF_INT;
          for (KeyValue kv : keyValues) {
            writeBuffer.putInt(kv.getLength());
            allWriteSize += Bytes.SIZEOF_INT;
            writeBuffer.put(kv.getBuffer(), kv.getOffset(), kv.getLength());
            allWriteSize += kv.getLength();
          }
          ImmutableBytesWritable[] sortKeys = re.sortKeys;
          writeBuffer.putInt(sortKeySize);
          allWriteSize += Bytes.SIZEOF_INT;
          for(ImmutableBytesWritable sortKey: sortKeys){
            writeBuffer.putInt(sortKey.getLength());
            allWriteSize += Bytes.SIZEOF_INT;
            writeBuffer.put(sortKey.get(), sortKey.getOffset(), sortKey.getLength());
            allWriteSize += sortKey.getLength();
          }
        }
        flushBuffer = true;
      } else {
        results.add(entry);
      }
      return flushBuffer;
    }

    public IndexedResultEntry getNextResult() throws IOException {
      if (allReadSize != 0 && allReadSize == allWriteSize && stop) {
        return null;
      }
      if(flushBuffer) {
        if (readBuffer == null
            || allReadSize + Bytes.SIZEOF_INT >= readIndex + mappingSize) {
          readIndex = allReadSize;
          readBuffer = this.fc.map(MapMode.READ_WRITE, readIndex,
              mappingSize);
        }
        int length = readBuffer.getInt();
        allReadSize += Bytes.SIZEOF_INT;
        long nextResultIndex = allReadSize + length;
        if (nextResultIndex >= readIndex + mappingSize) {
          readIndex = allReadSize;
          readBuffer = this.fc.map(MapMode.READ_WRITE, readIndex,
              mappingSize);
        }
        byte[] rb = new byte[length];
        readBuffer.get(rb);
        this.allReadSize += length;
        Result result = new Result(new ImmutableBytesWritable(rb));
        ResultTuple rt = new ResultTuple(result);
        int sortKeySize = readBuffer.getInt();
        allReadSize += Bytes.SIZEOF_INT;
        ImmutableBytesWritable[]sortKeys = new ImmutableBytesWritable[sortKeySize];
        for (int i = 0; i < sortKeySize; i++) {
          int contentLength = readBuffer.getInt();
          allReadSize += Bytes.SIZEOF_INT;
          byte[]sortKeyContent = new byte[contentLength];
          allReadSize += contentLength;
          readBuffer.get(sortKeyContent);
          sortKeys[i] = new ImmutableBytesWritable(sortKeyContent);
        }
        ResultEntry re = new ResultEntry(sortKeys, rt);
        return new IndexedResultEntry(index, re);
      } else {
        ResultEntry re = results.poll();
        return new IndexedResultEntry(index, re);
      }
    }

    public void stop() {
      this.stop = true;
    }

    public void close() {
      this.stop();
      if (this.fc != null) {
        try {
          this.fc.close();
        } catch (IOException e) {
//          logger.debug("Failed to close FileChannel:" + this.fc);
        }
      }
      if (this.af != null) {
        try {
          this.af.close();
        } catch (IOException e) {

        }
      }
      if (this.file != null) {
        if (file.isFile()) {
          boolean result = file.delete();
          if (!result) {
//            logger.warn("Failed to delete buffer file[" + file + "]");
          } else {
//            logger.debug("Has deleted buffer file[" + file + "]");
          }
        }
      }
    }
  }
}
