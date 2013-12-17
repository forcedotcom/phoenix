package com.salesforce.phoenix.cache.aggcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.expression.aggregator.Aggregator;
import com.salesforce.phoenix.expression.aggregator.ServerAggregators;
import com.salesforce.phoenix.expression.function.SingleAggregateFunction;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.KeyValueSchema;
import com.salesforce.phoenix.schema.ValueBitSet;
import com.salesforce.phoenix.schema.tuple.SingleKeyValueTuple;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.KeyValueUtil;
import com.salesforce.phoenix.util.TupleUtil;

/**
 * Class servers as an adapter between the in-memory LRU cache and the 
 * Spill data structures. 
 * It takes care of serializing / deserializing the key/value groupby tuples,
 * and spilling them to the correct spill partition
 */
public class SpillManager implements Closeable {
    
    // Wrapper class for DESERIALIZED groupby key/value tuples
    public static class CacheEntry {
        
        protected final ImmutableBytesPtr key;
        protected final Aggregator[] aggs;
        
        public CacheEntry( ImmutableBytesPtr key, 
                            Aggregator[] aggs ) {
            this.key = key;
            this.aggs = aggs;
        }
                
        public ImmutableBytesPtr getKey() {
            return key;
        }

        public Aggregator[] getValue(Configuration conf) {
            return aggs;
        }
                
        public int getKeyLength() {
            return key.getLength();
        }        
    }    

    
    private final ArrayList<SpillMap> spillMaps;
    private final int numSpillFiles;

    private ServerAggregators aggregators;
    private final Configuration conf;

    /**
     * SpillManager takes care of spilling and loading tuples from spilled data structs
     * @param numSpillFiles
     * @param serverAggregators
     */
    public SpillManager(int numSpillFiles, ServerAggregators serverAggregators, Configuration conf) {
        try {           
            spillMaps = Lists.newArrayList();
            this.numSpillFiles = numSpillFiles;
            this.aggregators = serverAggregators;
            this.conf = conf;
            
            // Create a list of spillFiles
            // Each Spillfile only handles up to 2GB data
            for( int i = 0; i < numSpillFiles; i++ ) {
                SpillFile file = SpillFile.createSpillFile();
                spillMaps.add(new SpillMap( file, SpillFile.DEFAULT_PAGE_SIZE ));               
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Could not init the SpillManager");
        }
    }
        
    // serialize a key/value tuple into a byte array
    // WARNING: expensive
    private byte[] serialize(ImmutableBytesPtr key, Aggregator[] aggs, ServerAggregators serverAggs) throws IOException {
                    
        DataOutputStream output = null; 
        ByteArrayOutputStream bai = null;
        try {
            bai = new ByteArrayOutputStream();
            output = new DataOutputStream(bai);
            // key length
            WritableUtils.writeVInt(output, key.getLength());
            // key
            output.write(key.get(), key.getOffset(), key.getLength());
            byte[] aggsByte = serverAggs.toBytes(aggs);                        
            // aggs length
            WritableUtils.writeVInt(output, aggsByte.length);
            // aggs
            output.write(aggsByte);
            
            byte[] data = bai.toByteArray();            
            return data;
        }
        finally {
            
            if(bai != null) {
                bai.close();
                bai = null;
            }            
            if(output != null) {
                output.close();
                output = null;
            }
        }
    }

    /**
     * Helper method to deserialize the key part from a serialized byte array
     * @param data
     * @return
     * @throws IOException
     */
     static ImmutableBytesPtr getKey(byte[] data) throws IOException {
        DataInputStream input = null;
        try {
            input = new DataInputStream(new ByteArrayInputStream(data));
            // key length                
            int keyLength = WritableUtils.readVInt(input);
            byte[] keyBytes = new byte[keyLength];
            // key
            input.readFully(keyBytes, 0, keyLength);
            return new ImmutableBytesPtr(keyBytes, 0, keyLength);
        }
        finally {
            if(input != null) {
                input.close();
            }
        }
    }

    // Instantiate Aggregators form a serialized byte array 
    private Aggregator[] getAggregators (byte[] data) throws IOException {
        DataInputStream input = null;
        try {
            input = new DataInputStream(new ByteArrayInputStream(data));
            // key length                
            int keyLength = WritableUtils.readVInt(input);
            byte[] keyBytes = new byte[keyLength];
            // key
            input.readFully(keyBytes, 0, keyLength);
            ImmutableBytesPtr ptr = new ImmutableBytesPtr(keyBytes, 0, keyLength);
            
            // value length
            int valueLength = WritableUtils.readVInt(input);
            byte[] valueBytes = new byte[valueLength];
            // value bytes
            input.readFully(valueBytes, 0, valueLength);
            KeyValue keyValue = KeyValueUtil.newKeyValue( ptr.get(),ptr.getOffset(), ptr.getLength(), 
                                                          QueryConstants.SINGLE_COLUMN_FAMILY, 
                                                          QueryConstants.SINGLE_COLUMN, QueryConstants.AGG_TIMESTAMP, 
                                                          valueBytes, 0, valueBytes.length);
            Tuple result = new SingleKeyValueTuple(keyValue);
            TupleUtil.getAggregateValue(result, ptr);
            KeyValueSchema schema = aggregators.getValueSchema();
            ValueBitSet tempValueSet = ValueBitSet.newInstance(schema);
            tempValueSet.clear();
            tempValueSet.or(ptr);

            int i = 0, maxOffset = ptr.getOffset() + ptr.getLength();
            SingleAggregateFunction[] funcArray = aggregators.getFunctions();
            Aggregator[] sAggs = new Aggregator[funcArray.length];
            Boolean hasValue;
            schema.iterator(ptr);
            while ((hasValue=schema.next(ptr, i, maxOffset, tempValueSet)) != null) {
                SingleAggregateFunction func = funcArray[i];
                sAggs[i++] = hasValue ? func.newServerAggregator(conf, ptr) : func.newServerAggregator(conf);
            }
            return sAggs;
            
        } finally {
            Closeables.closeQuietly(input);
        }
    }
    
    /**
     * Helper function to deserialize a byte array into
     * a CacheEntry
     * @param bytes
     * @throws IOException
     */
    public CacheEntry toCacheEntry( byte[] bytes ) throws IOException {
        ImmutableBytesPtr key = SpillManager.getKey(bytes);
        Aggregator[] aggs = getAggregators(bytes);
        
        return new CacheEntry(key, aggs);
    }
    
    // Determines the partition, i.e. spillFile the tuple should get spilled to.
    private int getPartition(ImmutableBytesPtr key) {
        // Simple implementation hash mod numFiles
        return Math.abs(key.hashCode()) % numSpillFiles;
    }

    /**
     * Function that spills a key/value groupby tuple into a partition
     * Spilling always triggers a serialize call
     * @param key
     * @param value
     * @throws IOException
     */
    public void spill(ImmutableBytesPtr key, Aggregator[] value) throws IOException{        
        SpillMap spillMap = spillMaps.get( getPartition(key) );
        byte[] data = serialize(key, value, aggregators);
        spillMap.put(key, data);
    }

    /**
     * Function that loads a spilled key/value groupby tuple from one of the spill partitions 
     * into the LRU cache.
     * Loading always involves deserialization
     * @throws IOException
     */
    public Aggregator[] loadEntry(ImmutableBytesPtr key) throws IOException {
        SpillMap spillMap = spillMaps.get( getPartition(key) );
        byte[] data = spillMap.get(key);
        if(data != null) {
            return getAggregators(data);
        }
        return null;
    }
    
    /**
     * Close the attached spillMap
     */
    @Override
    public void close() {
        for(int i = 0; i < spillMaps.size(); i++) {
            Closeables.closeQuietly(spillMaps.get(i).getSpillFile());
        }
    }
    
    /**
     * Function returns an iterator over all spilled Tuples
     */
    public SpillMapIterator newDataIterator() {
        return new SpillMapIterator();
    }

    private final class SpillMapIterator implements Iterator<byte[]> {

        int index = 0;
        Iterator< byte[] >spillIter = spillMaps.get(index).iterator();
        
        @Override
        public boolean hasNext() {
            if(!spillIter.hasNext())
            {
                if(index < (numSpillFiles - 1) ) {
                    // Current spillFile exhausted get iterator over new one
                    spillIter = spillMaps.get(++index).iterator();
                }
            }
            return spillIter.hasNext();
        }

        @Override
        public byte[] next() {
            return spillIter.next();
        }

        @Override
        public void remove() {
            throw new IllegalAccessError("Remove is not supported for this type of iterator");            
        }       
    }
}
