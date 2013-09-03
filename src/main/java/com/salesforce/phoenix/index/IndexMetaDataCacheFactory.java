package com.salesforce.phoenix.index;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.cache.IndexMetaDataCache;
import com.salesforce.phoenix.coprocessor.ServerCachingProtocol.ServerCacheFactory;
import com.salesforce.phoenix.memory.MemoryManager.MemoryChunk;

public class IndexMetaDataCacheFactory implements ServerCacheFactory {
    public IndexMetaDataCacheFactory() {
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
    }

    @Override
    public Closeable newCache (ImmutableBytesWritable cachePtr, final MemoryChunk chunk) throws SQLException {
        final List<IndexMaintainer> maintainers = IndexMaintainer.deserialize(cachePtr);
        return new IndexMetaDataCache() {

            @Override
            public void close() throws IOException {
                chunk.close();
            }

            @Override
            public List<IndexMaintainer> getIndexMaintainers() {
                return maintainers;
            }
        };
    }
}
