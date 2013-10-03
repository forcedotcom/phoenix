package com.salesforce.hbase.index.wal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;

public class IndexedKeyValue extends KeyValue {

  byte[] indexTableName;
  Mutation mutation;
  // optimization check to ensure that batches don't get replayed to the index more than once
  private boolean batchFinished = false;
  
  public IndexedKeyValue() {
  }

  public IndexedKeyValue(byte[] bs, Mutation mutation) {
    this.indexTableName = bs;
    this.mutation = mutation;
  }

  public byte[] getIndexTable() {
    return this.indexTableName;
  }

  public String getIndexTableString() {
    return Bytes.toString(indexTableName);
  }

  public Mutation getMutation() {
    return mutation;
  }

  /**
   * This is a KeyValue that shouldn't actually be replayed, so we always mark it as an
   * {@link HLog#METAFAMILY} so it isn't replayed via the normal replay mechanism
   */
  @Override
  public boolean matchingFamily(final byte[] family) {
    return Bytes.equals(family, HLog.METAFAMILY);
  }

  @Override
  public String toString() {
    return "IndexWrite:\n\ttable: " + indexTableName + "\n\tmutation:" + mutation;
  }

  /**
   * This is a very heavy-weight operation and should only be done when absolutely necessary - it
   * does a full serialization of the underyling mutation to compare the underlying data.
   */
  @Override
  public boolean equals(Object o) {
    if (o instanceof IndexedKeyValue) {
      IndexedKeyValue other = (IndexedKeyValue) o;
      if (Bytes.equals(other.indexTableName, this.indexTableName)) {
        try {
          byte[] current = getBytes(this.mutation);
          byte[] otherMutation = getBytes(other.mutation);
          return Bytes.equals(current, otherMutation);
        } catch (IOException e) {
          throw new IllegalArgumentException("Failed to correctly serialize a mutation!", e);
        }
      }
    }
    return false;
  }
  
  private byte[] getBytes(Mutation m) throws IOException{
    ByteArrayOutputStream bos = null;
    try{
      bos = new ByteArrayOutputStream();
      this.mutation.write(new DataOutputStream(bos));
      bos.flush();
      return bos.toByteArray();
    }finally{
      if(bos != null){
        bos.close();
      }
    }
  }

  @Override
  public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + this.indexTableName.hashCode();
      result = prime * result + this.mutation.hashCode();
      return result;
  }

  @Override
  public void write(DataOutput out) throws IOException{
    KeyValueCodec.write(out, this);
  }

  /**
   * Internal write the underlying data for the entry - this does not do any special prefixing.
   * Writing should be done via {@link KeyValueCodec#write(DataOutput, KeyValue)} to ensure
   * consistent reading/writing of {@link IndexedKeyValue}s.
   * @param out to write data to. Does not close or flush the passed object.
   * @throws IOException if there is a problem writing the underlying data
   */
  void writeData(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.indexTableName);
    out.writeUTF(this.mutation.getClass().getName());
    this.mutation.write(out);
  }

  /**
   * This method shouldn't be used - you should use {@link KeyValueCodec#readKeyValue(DataInput)}
   * instead. Its the complement to {@link #writeData(DataOutput)}.
   */
  @SuppressWarnings("javadoc")
  @Override
  public void readFields(DataInput in) throws IOException {
    this.indexTableName = Bytes.readByteArray(in);
    Class<? extends Mutation> clazz;
    try {
      clazz = Class.forName(in.readUTF()).asSubclass(Mutation.class);
      this.mutation = clazz.newInstance();
      this.mutation.readFields(in);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  public boolean getBatchFinished() {
    return this.batchFinished;
  }

  public void markBatchFinished() {
    this.batchFinished = true;
  }
}