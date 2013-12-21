package com.salesforce.phoenix.cache.aggcache;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.MappedByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;

/**
 * Class implements an active spilled partition serialized tuples are first
 * written into an in-memory data structure that represents a single page. As
 * the page fills up, it is written to the current spillFile or spill partition
 * For fast tuple discovery, the class maintains a per page bloom-filter and
 * never de-serializes elements
 */
public class SpillMap extends AbstractMap<ImmutableBytesPtr, byte[]> implements
        Iterable<byte[]> {

    // threshold is typically the page size
    private final int thresholdBytes;
    private final int pageInserts;    
    private MappedByteBufferMap byteMap = null;
    // Keep a list of bloomfilters, one for every page to quickly determine
    // which page determines the requested key
    private final ArrayList<BloomFilter<byte[]>> bFilters;
    private final SpillFile spillFile;

    public SpillMap(SpillFile file, int thresholdBytes, int estValueSize) throws IOException {
        this.thresholdBytes = thresholdBytes;
        this.pageInserts = thresholdBytes / estValueSize;
        this.spillFile = file;
        byteMap = MappedByteBufferMap.newMappedByteBufferMap(thresholdBytes,
                spillFile);
        bFilters = Lists.newArrayList();
        bFilters.add(BloomFilter.create(Funnels.byteArrayFunnel(), pageInserts));
    }

    /**
     * Get a key from the spillable data structures This conducts a linear
     * search through all active pages in the current SpillFile Before doing an
     * actual IO on the page, we check its associated bloomFilter if the key is
     * contained. False positives are possible but compensated for.
     */
    @Override
    public byte[] get(Object key) {
        if (!(key instanceof ImmutableBytesPtr)) {
            // TODO ... work on type safety
        }
        ImmutableBytesPtr ikey = (ImmutableBytesPtr) key;
        byte[] value = null;
        int bucketIndex = 0;

        // Iterate over all pages
        byte[] keyBytes = ikey.copyBytesIfNecessary();
        for (int i = 0; i <= spillFile.getMaxPageId(); i++) {
            // run in loop in case of false positives in bloom filter
            bucketIndex = isKeyinPage(keyBytes, i);
            if (bucketIndex == -1) {
                // key not contained in current bloom filter
                continue;
            }

            if (bucketIndex != byteMap.getCurIndex()) {
                // key contained in page which is not in memory
                // page it in
                if (byteMap.getSize() > 0) {
                    // ensure consistency and flush current memory page to disk
                    byteMap.flushBuffer();
                }
                // load page into memory
                byteMap = MappedByteBufferMap.newMappedByteBufferMap(
                        bucketIndex, thresholdBytes, spillFile);
                byteMap.pageIn();
            }
            // get KV from current queue
            value = byteMap.getPagedInElement(ikey);
            if (value != null) {
                return value;
            }
        }
        return value;
    }

    /**
     * Spill a key First we discover if the key has been spilled before and load
     * it into memory: #ref get() if it was loaded before just replace the old
     * value in the memory page if it was not loaded before try to store it in
     * the current page alternatively if not enough memory available, request
     * new page.
     */
    @Override
    public byte[] put(ImmutableBytesPtr key, byte[] value) {
        // page in element and replace if present
        byte[] spilledValue = get(key);

        MappedByteBufferMap byteMap = ensureSpace(spilledValue, value);        
        byteMap.addElement(spilledValue, key, value);
        addKeyToBloomFilter(byteMap.getCurIndex(), key.copyBytesIfNecessary());

        return value;
    }

    /**
     * Function returns the current spill file
     */
    public SpillFile getSpillFile() {
        return spillFile;
    }

    private int isKeyinPage(byte[] key, int index) {
        // use BloomFilter to determine if key is contained in page
        // prevent expensive IO
        if (bFilters.get(index).mightContain(key)) {
            return index;
        }
        return -1;
    }

    // Funtion checks if current page has enough space to fit the new serialized
    // tuple
    // If not it flushes the current buffer and gets a new page
    // TODO: The code does not ensure that pages are optimally packed.
    // It only tries to fill up the current page as much as possbile, if its
    // exhausted it requests a new page. Instead it would be nice to load the
    // next page
    // that could fit the new value.
    private MappedByteBufferMap ensureSpace(byte[] prevValue, byte[] newValue) {
        if (!byteMap.canFit(prevValue, newValue)) {
            if (prevValue != null) {
                // Element grew in size and does not fit into buffer
                // anymore.
                // TODO InvalidateValue();
                throw new RuntimeException("Element grew to large, not supported yet");
            }
            // Flush current buffer
            byteMap.flushBuffer();
            // Get next page
            byteMap = MappedByteBufferMap.newMappedByteBufferMap(
                    thresholdBytes, spillFile);
            // Create new bloomfilter
            bFilters.add(BloomFilter.create(Funnels.byteArrayFunnel(), pageInserts));
        }
        return byteMap;
    }

    private void addKeyToBloomFilter(int index, byte[] key) {
        BloomFilter<byte[]> bFilter = bFilters.get(index);
        bFilter.put(key);
    }

    /**
     * This inner class represents the currently mapped file region. It uses a
     * Map to represent the current in memory page for easy get() and update()
     * calls on an individual key The class keeps track of the current size of
     * the in memory page and handles flushing and paging in respectively
     */
    private static class MappedByteBufferMap {
        private final int thresholdBytes;
        private long totalResultSize = 0;
        private final MappedByteBuffer buffer;
        private final int bufferIndex;
        // Use a map for in memory page representation
        final Map<ImmutableBytesPtr, byte[]> pageMap = Maps.newConcurrentMap();

        private MappedByteBufferMap(int index, MappedByteBuffer buffer,
                int thresholdBytes) {
            // throws IOException {
            this.bufferIndex = index;
            this.buffer = buffer;
            // size threshold of a page
            this.thresholdBytes = thresholdBytes - Bytes.SIZEOF_INT;
            pageMap.clear();
        }

        // Get the next free new MappedMap
        static MappedByteBufferMap newMappedByteBufferMap(int thresholdBytes,
                SpillFile spillFile) {
            MappedByteBuffer buffer = spillFile.getNextFreePage();
            return new MappedByteBufferMap(spillFile.getMaxPageId(), buffer,
                    thresholdBytes);
        }

        // Random access to new MappedMap
        static MappedByteBufferMap newMappedByteBufferMap(int index,
                int thresholdBytes, SpillFile spillFile) {
            MappedByteBuffer buffer = spillFile.getPage(index);
            return new MappedByteBufferMap(index, buffer, thresholdBytes);
        }

        private boolean canFit(byte[] curValue, byte[] newValue) {
            if (thresholdBytes < newValue.length) {
                // TODO resize page size if single element is too big,
                // Can this ever happen?
                throw new RuntimeException(
                        "page size too small to store a single KV element");
            }

            int resultSize = newValue.length + Bytes.SIZEOF_INT;
            if (curValue != null) {
                // Key existed before
                // Ensure to compensate for potential larger byte[] for agg
                resultSize = Math.max(0, resultSize
                        - (curValue.length + Bytes.SIZEOF_INT));
            }

            if ((thresholdBytes - totalResultSize) <= (resultSize)) {
                // KV does not fit
                return false;
            }
            // KV fits
            return true;
        }

        public int getCurIndex() {
            return bufferIndex;
        }

        public int getSize() {
            return pageMap.size();
        }

        // Flush the current page to the memory mapped byte buffer
        private void flushBuffer() throws BufferOverflowException {
            Collection<byte[]> values = pageMap.values();
            buffer.clear();
            // number of elements
            buffer.putInt(values.size());
            for (byte[] value : values) {
                // element length
                buffer.putInt(value.length);
                // element
                buffer.put(value, 0, value.length);
            }
            // Reset page stats
            pageMap.clear();
            totalResultSize = 0;
        }

        // load memory mapped region into a map for fast element access
        private void pageIn() throws IndexOutOfBoundsException {
            int numElements = buffer.getInt();
            for (int i = 0; i < numElements; i++) {
                int kvSize = buffer.getInt();
                byte[] data = new byte[kvSize];
                buffer.get(data, 0, kvSize);
                try {
                    pageMap.put(SpillManager.getKey(data), data);
                    totalResultSize += (data.length + Bytes.SIZEOF_INT);
                } catch (IOException ioe) {
                    // Error during key access on spilled resource
                    // TODO rework error handling
                    throw new RuntimeException(ioe);
                }
            }
        }

        /**
         * Return a cache element currently page into memory Direct access via
         * mapped page map
         * 
         * @param key
         * @return
         */
        public byte[] getPagedInElement(ImmutableBytesPtr key) {
            return pageMap.get(key);
        }

        /**
         * Inserts / Replaces cache element in the currently loaded page. Direct
         * access via mapped page map
         * 
         * @param key
         * @param value
         */
        public void addElement(byte[] spilledValue, ImmutableBytesPtr key,
                byte[] value) {

            // put Element into map
            pageMap.put(key, value);
            // track current Map size to prevent Buffer overflows
            if (spilledValue != null) {
                // if previous key was present, just add the size difference
                totalResultSize += Math.max(0, value.length
                        - (spilledValue.length));
            } else {
                // Add new size information
                totalResultSize += (value.length + Bytes.SIZEOF_INT);
            }
        }

        /**
         * Returns a value iterator over the pageMap
         */
        public Iterator<byte[]> getPageMapEntries() {
            return pageMap.values().iterator();
        }

        /**
         * Direct access to the current memory mapped buffer Used by Iterator to
         * prevent loading the pageMap first
         * 
         * @return List of all serialized aggregate objects in arbitrary order
         * @throws IndexOutOfBoundsException
         */
        public List<byte[]> getMemBufferElements()
                throws IndexOutOfBoundsException {
            ArrayList<byte[]> elements = Lists.newArrayList();
            int numElements = buffer.getInt();

            for (int i = 0; i < numElements; i++) {
                int kvSize = buffer.getInt();
                byte[] data = new byte[kvSize];
                buffer.get(data, 0, kvSize);
                elements.add(data);
            }
            return elements;
        }
    }

    @Override
    public Iterator<byte[]> iterator() {

        return new Iterator<byte[]>() {
            List<byte[]> bufferEntries;
            int pageIndex = 0;
            final int inMemPageIndex = byteMap.getCurIndex();
            int curBufferIndex = 0;
            int curBufferSize = 0;
            final long highestPageId = spillFile.getMaxPageId();
            final Iterator<byte[]> entriesIter = byteMap.getPageMapEntries();

            @Override
            public boolean hasNext() {
                if (highestPageId == 0) {
                    // single page only, no need to do more IO
                    return entriesIter.hasNext();
                }
                if (inMemPageIndex == pageIndex) {
                    // skip current page, since covered by in memory map already
                    pageIndex++;
                }
                if (pageIndex > highestPageId
                        && curBufferIndex >= curBufferSize) {
                    return false;
                }
                if (curBufferIndex >= curBufferSize) {
                    // get keys from all spilled pages
                    MappedByteBuffer buffer = spillFile.getPage(pageIndex++);
                    byteMap = new MappedByteBufferMap(pageIndex, buffer,
                            thresholdBytes);
                    bufferEntries = byteMap.getMemBufferElements();
                    curBufferSize = bufferEntries.size();
                    curBufferIndex = 0;
                }
                return true;
            }

            @Override
            public byte[] next() {
                if (entriesIter.hasNext()) {
                    // get elements from in memory map first
                    return entriesIter.next();
                }
                return bufferEntries.get(curBufferIndex++);
            }

            @Override
            public void remove() {
                throw new IllegalAccessError(
                        "Iterator does not support removal operation");
            }
        };
    }

    @Override
    public Set<java.util.Map.Entry<ImmutableBytesPtr, byte[]>> entrySet() {
        throw new IllegalAccessError(
                "entrySet is not supported for this type of cache");
    }
}
