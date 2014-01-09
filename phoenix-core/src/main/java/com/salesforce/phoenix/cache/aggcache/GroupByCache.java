package com.salesforce.phoenix.cache.aggcache;

import static com.salesforce.phoenix.query.QueryServices.SPGBY_MAX_CACHE_SIZE_ATTRIB;
import static com.salesforce.phoenix.query.QueryServices.SPGBY_NUM_SPILLFILES_ATTRIB;
import static com.salesforce.phoenix.query.QueryServicesOptions.DEFAULT_SPGBY_CACHE_MAX_SIZE;
import static com.salesforce.phoenix.query.QueryServicesOptions.DEFAULT_SPGBY_NUM_SPILLFILES;

import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.io.Closeables;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.cache.aggcache.SpillManager.CacheEntry;
import com.salesforce.phoenix.expression.aggregator.Aggregator;
import com.salesforce.phoenix.expression.aggregator.ServerAggregators;

/**
 * The main entry point is in GroupedAggregateRegionObserver. It instantiates a GroupByCache and
 * invokes a get() method on it. There is no: "if key not exists -> put into map" case, since the
 * cache is a Loading cache and therefore handles the put under the covers. I tried to implement the
 * final cache element accesses (RegionScanner below) streaming, i.e. there is just an iterator on
 * it and removed the existing result materialization. GroupByCache implements a Guava LoadingCache,
 * which an upper and lower configurable size limit. Optimally it is sized as estMapSize /
 * valueSize, since the upper limit is number and not memory budget based. As long as no eviction
 * happens no spillable data structures are allocated, this only happens as soon as the first
 * element is evicted from the cache. We cannot really make any assumptions on which keys arrive at
 * the map, but I thought the LRU would at least cover the cases where some keys have a slight skew
 * and they should stay memory resident. Once a key gets evicted, the spillManager is instantiated.
 * It basically takes care of spilling an element to disk and does all the SERDE work. It creates a
 * configured number of SpillFiles (spill partition) which are memory mapped files. Each MMFile only
 * works with up to 2GB of spilled data, therefore the SpillManager keeps a list of these and hash
 * distributes the keys within this list. Once an element gets spilled, it is serialized and will
 * only get deserialized again, when it is requested from the client, i.e. loaded back into the LRU
 * cache. The SpillManager holds a single SpillMap object in memory for every spill partition
 * (SpillFile). The SpillMap is an in memory Map representation of a single page of spilled still
 * serialized key/value pairs. To achieve fast key lookup the key is hash partitioned into random
 * pages of the current spill file. The code implements an extendible hashing approach which
 * dynamicall adjusts the hash function, in order to adapt to growing data files and avoiding long
 * chains of overflow buckets. For an excellent discussion of the algorithm please refer to the
 * following online resource:
 * http://db.inf.uni-tuebingen.de/files/teaching/ws1011/db2/db2-hash-indexes.pdf The implementation
 * starts with a global depth of 1 and therefore a directory size of 2 buckets. Only during bucket
 * split and directory doubling more than one page is temporarily kept in memory, until all elements
 * have been redistributed. The current implementation conducts bucket splits as long as an element
 * does not fit onto a page. No overflow chain is created, which might be an alternative. For get
 * requests, each directory entry maintains a bloomFilter to prevent page-in operations in case an
 * element has never been spilled before. The deserialization is only triggered when a key a loaded
 * back into the LRU cache. The aggregators are returned from the LRU cache and the next value is
 * computed. In case the key is not found on any page, the Loader create new aggregators for it.
 */
public class GroupByCache<K extends ImmutableBytesWritable> extends AbstractMap<K, Aggregator[]>
        implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(GroupByCache.class);

    // Min size of 1st level main memory cache in bytes --> lower bound
    private static final int SPGBY_CACHE_MIN_SIZE = 4096; // 4K

    // TODO Generally better to use Collection API with generics instead of
    // array types
    public final LoadingCache<K, Aggregator[]> cache;
    private SpillManager spillManager = null;
    private final ObserverContext<RegionCoprocessorEnvironment> context;
    private int curNumCacheElements;
    private final int estValueSize;
    private final ServerAggregators aggregators;

    /**
     * Instantiates a Loading LRU Cache that stores key / aggregator[] tuples used for group by
     * queries
     * @param estSize
     * @param estValueSize
     * @param aggs
     * @param ctxt
     */
    public GroupByCache(final long estSize, final int estValueSize, ServerAggregators aggs,
            final ObserverContext<RegionCoprocessorEnvironment> ctxt) {
        context = ctxt;
        this.estValueSize = estValueSize;
        curNumCacheElements = 0;
        this.aggregators = aggs;

        Configuration conf = ctxt.getEnvironment().getConfiguration();
        final long maxCacheSizeConf =
                conf.getLong(SPGBY_MAX_CACHE_SIZE_ATTRIB, DEFAULT_SPGBY_CACHE_MAX_SIZE);
        final int numSpillFilesConf =
                conf.getInt(SPGBY_NUM_SPILLFILES_ATTRIB, DEFAULT_SPGBY_NUM_SPILLFILES);

        final int estSizeNum = (int) (estSize / estValueSize);
        final int maxSizeNum = (int) (maxCacheSizeConf / estValueSize);
        final int minSizeNum = (SPGBY_CACHE_MIN_SIZE / estValueSize);

        // use upper and lower bounds for the cache size
        int maxCacheSize = Math.max(minSizeNum, Math.min(maxSizeNum, estSizeNum));

        if (logger.isDebugEnabled()) {
            logger.debug("Instantiating LRU groupby cache of element size: " + maxCacheSize);
        }
        // Cache is element number bounded. Using the max of CACHE_MIN_SIZE and
        // the est MapSize
        cache =
                CacheBuilder.newBuilder().maximumSize(maxCacheSize)
                        .removalListener(new RemovalListener<K, Aggregator[]>() {
                            /*
                             * CacheRemoval listener implementation
                             */
                            @Override
                            public void
                                    onRemoval(RemovalNotification<K, Aggregator[]> notification) {
                                try {
                                    if (spillManager == null) {
                                        // Lazy instantiation of spillable data
                                        // structures
                                        //
                                        // Only create spill data structs if LRU
                                        // cache is too small
                                        spillManager =
                                                new SpillManager(numSpillFilesConf, estValueSize,
                                                        aggregators, context.getEnvironment()
                                                                .getConfiguration());
                                    }
                                    spillManager.spill(notification.getKey(),
                                        notification.getValue());

                                    // keep track of elements in cache
                                    curNumCacheElements--;
                                } catch (IOException ioe) {
                                    // Ensure that we always close and delete the temp files
                                    try {
                                        throw new RuntimeException(ioe);
                                    }
                                    finally {
                                        try {
                                            close();
                                        }
                                        catch(IOException ie) {
                                            // Noop
                                        }                                        
                                    }
                                }
                            }
                        }).build(new CacheLoader<K, Aggregator[]>() {
                            /*
                             * CacheLoader implementation
                             */
                            @Override
                            public Aggregator[] load(K key) throws Exception {

                                Aggregator[] aggs = null;
                                if (spillManager == null) {
                                    // No spill Manager, always assume this key is new!
                                    aggs =
                                            aggregators.newAggregators(context.getEnvironment()
                                                    .getConfiguration());
                                } else {
                                    // Spill manager present, check if key has been
                                    // spilled before
                                    aggs = spillManager.loadEntry(new ImmutableBytesPtr(key));
                                    if (aggs == null) {
                                        // No, key never spilled before, create a new tuple
                                        aggs =
                                                aggregators.newAggregators(context.getEnvironment()
                                                        .getConfiguration());
                                    }
                                }
                                // keep track of elements in cache
                                curNumCacheElements++;
                                return aggs;
                            }
                        });
    }

    /**
     * Size function returns the estimate LRU cache size in bytes
     */
    @Override
    public int size() {
        return curNumCacheElements * estValueSize;
    }

    /**
     * put function of Map interface DO NOT USE Cache takes automatically care of loading
     * non-existing elements
     */
    @Override
    public Aggregator[] put(ImmutableBytesWritable key, Aggregator[] value) {
        // Loading cache implicitly implements a put when a cache hit has
        // occurred
        // Disable the map interface
        throw new IllegalAccessError("put is not supported for a loading cache");
    }

    /**
     * Extract an element from the Cache If element is not present in in-memory cache / or in spill
     * files cache implements an implicit put() of a new key/value tuple and loads it into the cache
     */
    @Override
    public Aggregator[] get(Object key) {
        try {
            // delegate to the cache implementation
            return cache.get((K) key);
        } catch (ExecutionException e) {
            // TODO What's the proper way to surface errors?
            // FIXME using a non-checked exception for now...
            throw new RuntimeException(e);
        }
    }

    /**
     * Return a map view of the cache, READ ONLY
     */
    public Map<? extends ImmutableBytesWritable, Aggregator[]> getAsMap() {
        // Useful for iterators
        return cache.asMap();
    }

    /**
     * This function creates a iterator over the cache and the spilled data structures. The
     * key/value tuples are returned in non-deterministic order.
     */
    public EntryIterator newEntryIterator() {
        return new EntryIterator();
    }

    // Iterator that returns CacheEntries.
    // CacheEntries are either extracted from the LRU cache or from the
    // spillable data structures.
    private final class EntryIterator implements Iterator<Map.Entry<K, Aggregator[]>> {
        final Map<K, Aggregator[]> cacheMap;
        final Iterator<Entry<K, Aggregator[]>> cacheIter;
        final Iterator<byte[]> spilledCacheIter;

        private EntryIterator() {
            cacheMap = cache.asMap();
            cacheIter = cacheMap.entrySet().iterator();
            if (spillManager != null) {
                spilledCacheIter = spillManager.newDataIterator();
            } else {
                spilledCacheIter = null;
            }
        }

        @Override
        public boolean hasNext() {
            return cacheIter.hasNext();
        }

        @Override
        public Map.Entry<K, Aggregator[]> next() {
            if (spilledCacheIter != null && spilledCacheIter.hasNext()) {
                try {
                    byte[] value = spilledCacheIter.next();
                    // Deserialize into a CacheEntry
                    Map.Entry<K, Aggregator[]> spilledEntry = spillManager.toCacheEntry(value);

                    boolean notFound = false;
                    // check against map and return only if not present
                    while (cacheMap.containsKey(spilledEntry.getKey())) {
                        // LRU Cache entries always take precedence,
                        // since they are more up to date
                        if (spilledCacheIter.hasNext()) {
                            value = spilledCacheIter.next();
                            spilledEntry = spillManager.toCacheEntry(value);
                        } else {
                            notFound = true;
                            break;
                        }
                    }
                    if (!notFound) {
                        // Return a spilled entry, this only happens if the
                        // entry was not
                        // found in the LRU cache
                        return spilledEntry;
                    }
                } catch (IOException ioe) {
                    // TODO rework error handling
                    throw new RuntimeException(ioe);
                }
            }
            // Spilled elements exhausted
            // Finally return all elements from LRU cache
            Map.Entry<K, Aggregator[]> entry = cacheIter.next();
            return new CacheEntry<K>(entry.getKey(), entry.getValue());
        }

        /**
         * Remove??? Denied!!!
         */
        @Override
        public void remove() {
            throw new IllegalAccessError("Remove is not supported for this type of iterator");
        }
    }

    /**
     * Get an entrySet for all elements loaded into the Spillable GroupBy
     */
    @Override
    public Set<java.util.Map.Entry<K, Aggregator[]>> entrySet() {
        return new EntrySet();
    }

    private final class EntrySet extends AbstractSet<Map.Entry<K, Aggregator[]>> {

        @Override
        public int size() {
            return curNumCacheElements;
        }

        @Override
        public Iterator<Map.Entry<K, Aggregator[]>> iterator() {
            return new EntryIterator();
        }
    }

    /**
     * Closes cache and releases spill resources
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        // Close spillable resources
        Closeables.closeQuietly(spillManager);
    }
}