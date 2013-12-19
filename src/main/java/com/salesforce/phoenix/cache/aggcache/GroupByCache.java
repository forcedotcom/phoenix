package com.salesforce.phoenix.cache.aggcache;

import static com.salesforce.phoenix.query.QueryServices.SPGBY_MAX_CACHE_SIZE_ATTRIB;
import static com.salesforce.phoenix.query.QueryServices.SPGBY_NUM_SPILLFILES_ATTRIB;
import static com.salesforce.phoenix.query.QueryServicesOptions.DEFAULT_SPGBY_CACHE_MAX_SIZE;
import static com.salesforce.phoenix.query.QueryServicesOptions.DEFAULT_SPGBY_NUM_SPILLFILES;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
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
 * The main entry point is in GroupedAggregateRegionObserver. It instantiates a
 * GroupByCache and invokes a get() method on it. There is no:
 * "if key not exists -> put into map" case, since the cache is a Loading cache
 * and therefore handles the put under the covers. I tried to implement the
 * final cache element accesses (RegionScanner below) streaming, i.e. there is
 * just an iterator on it and removed the existing result materialization.
 * 
 * GroupByCache implements a Guava LoadingCache, which an upper and lower
 * configurable size limit. Optimally it is sized as estMapSize / valueSize,
 * since the upper limit is number and not memory budget based. As long as no
 * eviction happens no spillable data structures are allocated, this only
 * happens as soon as the first element is evicted from the cache. We cannot
 * really make any assumptions on which keys arrive at the map, but I thought
 * the LRU would at least cover the cases where some keys have a slight skew and
 * they should stay memory resident.
 * 
 * Once a key gets evicted, the spillManager is instantiated. It basically takes
 * care of spilling an element to disk and does all the SERDE work. It creates a
 * bunch of SpillFiles (spill partition) which are MemoryMappedFiles. Each
 * MMFile only works with up to 2GB of spilled data, therefore the SpillManager
 * keeps a list of these and hash distributes the keys within this list. Once an
 * element gets spilled, it is serialized and will only get deserialized again,
 * when it is requested from the client, i.e. loaded back into the LRU cache.
 * The SpillManager holds a SpillMap for every spill partition (SpillFile). Each
 * SpillMap has access to all the pages of its SpillFile, it also contains a
 * list of bloomFilters, one for every page of the spillFile. Only a single
 * page, the currently active page stays in memory. The in memory data structure
 * of the page is a HashMap for easy key access purposes. An element evicted
 * form the LRU cache is hashed into the correct SpillMap. the spillMap then
 * determines via the list of BloomFilters on which page in the SpillFile the
 * key resides and loads this page into a HashMap in memory. If the key was
 * never spilled before, the SpillMap tries to fill up the current, in memory
 * residing page with this new key. In case it doesn't fit, the current memory
 * page is flushed to disk and a new page is requested. The key is then written
 * to the new in-memory page. Lastly, the bloomFilter is updated so that this
 * key can be discovered again. Loading a key works similarly, if not present in
 * the LRU cache, the CacheLoader sets in. The key gets hashed into the correct
 * SpillMap, the list of bloomFilters is walked to determined the SpillFile
 * page, the key resides on and this page is loaded first into memory and
 * eventually, into the LRU cache. Only for the last step the deserialization is
 * triggered. The aggregators are returned from the LRU cache and the next value
 * is computed. In case the key is not found on any page, the Loader create new
 * aggregators for it.
 */
public class GroupByCache extends AbstractMap<ImmutableBytesPtr, Aggregator[]> {
    private static final Logger logger = LoggerFactory
            .getLogger(SpillManager.class);

    // Min size of 1st level main memory cache in bytes --> lower bound
    private static final int SPGBY_CACHE_MIN_SIZE = 4096; // 4K

    // TODO Generally better to use Collection API with generics instead of
    // array types
    public final LoadingCache<ImmutableBytesPtr, Aggregator[]> cache;
    private SpillManager spillManager = null;
    private final ObserverContext<RegionCoprocessorEnvironment> context;
    private int curNumCacheElements;
    private final int estValueSize;
    private final ServerAggregators aggregators;

    /**
     * Instantiates a Loading LRU Cache that stores key / aggregator[] tuples
     * used for group by queries
     * 
     * @param estSize
     * @param estValueSize
     * @param aggs
     * @param ctxt
     */
    public GroupByCache(long estSize, int estValueSize, ServerAggregators aggs,
            final ObserverContext<RegionCoprocessorEnvironment> ctxt) {
        context = ctxt;
        this.estValueSize = estValueSize;
        curNumCacheElements = 0;
        this.aggregators = aggs;        
        
        Configuration conf = ctxt.getEnvironment().getConfiguration();
        final long maxCacheSizeConf = conf.getLong(SPGBY_MAX_CACHE_SIZE_ATTRIB, DEFAULT_SPGBY_CACHE_MAX_SIZE);
        final int numSpillFilesConf = conf.getInt(SPGBY_NUM_SPILLFILES_ATTRIB, DEFAULT_SPGBY_NUM_SPILLFILES);
        
        logger.error("conf size" + maxCacheSizeConf);
        int estSizeNum = (int) (estSize / estValueSize);
        int maxSizeNum = (int) ( maxCacheSizeConf / estValueSize);
        int minSizeNum = (SPGBY_CACHE_MIN_SIZE / estValueSize);
        logger.error("estSize: " + estSize);
        logger.error("estValueSize: " + estValueSize);
        logger.error("estValueSize: " + estValueSize);
        logger.error("estSizeNum: " + estSizeNum);
        logger.error("maxSizeNum: " + maxSizeNum);
        logger.error("minSizeNum: " + minSizeNum);

        // use upper and lower bounds for the cache size
        int maxCacheSize = Math.max(minSizeNum,
                Math.min(maxSizeNum, estSizeNum));

        if (logger.isDebugEnabled()) {
            logger.debug("Instantiating LRU groupby cache of element size: "
                    + maxCacheSize);
        }
        // Cache is element number bounded. Using the max of CACHE_MIN_SIZE and
        // the est MapSize
        cache = CacheBuilder                
                .newBuilder()
                .recordStats()
                .maximumSize(maxCacheSize)
                .removalListener(
                        new RemovalListener<ImmutableBytesPtr, Aggregator[]>() {
                            /*
                             * CacheRemoval listener implementation
                             */
                            @Override
                            public void onRemoval(
                                    RemovalNotification<ImmutableBytesPtr, Aggregator[]> notification) {
                                try {
                                    if (spillManager == null) {
                                        // Lazy instantiation of spillable data
                                        // structures
                                        //
                                        // Only create spill data structs if LRU
                                        // cache is too small
                                        spillManager = new SpillManager(
                                                numSpillFilesConf,
                                                aggregators, context
                                                        .getEnvironment()
                                                        .getConfiguration());
                                    }
                                    spillManager.spill(notification.getKey(),
                                            notification.getValue());
                                    // keep track of elements in cache
                                    curNumCacheElements--;
                                } catch (IOException ioe) {
                                    // TODO rework error handling
                                    throw new RuntimeException(ioe);
                                }
                            }
                        })
                .build(new CacheLoader<ImmutableBytesPtr, Aggregator[]>() {
                    /*
                     * CacheLoader implementation
                     */
                    @Override
                    public Aggregator[] load(ImmutableBytesPtr key)
                            throws Exception {

                        Aggregator[] aggs = null;
                        if (spillManager == null) {
                            // No spill Manager, always assume this key is new!
                            aggs = aggregators.newAggregators(context
                                    .getEnvironment().getConfiguration());
                        } else {
                            // Spill manager present, check if key has been
                            // spilled before
                            aggs = spillManager.loadEntry(key);
                            if (aggs == null) {
                                // No, key never spilled before, create a new tuple
                                aggs = aggregators.newAggregators(context
                                        .getEnvironment().getConfiguration());
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
     * put function of Map interface DO NOT USE Cache takes automatically care
     * of loading non-existing elements
     */
    @Override
    public Aggregator[] put(ImmutableBytesPtr key, Aggregator[] value) {
        // Loading cache implicitly implements a put when a cache hit has
        // occurred
        // Disable the map interface
        throw new IllegalAccessError("put is not supported for a loading cache");
    }

    /**
     * Extract an element from the Cache If element is not present in in-memory
     * cache / or in spill files cache implements an implicit put() of a new
     * key/value tuple and loads it into the cache
     */
    @Override
    public Aggregator[] get(Object key) {
        try {
            // delegate to the cache implementation
            return cache.get((ImmutableBytesPtr) key);
        } catch (ExecutionException e) {
            // TODO What's the proper way to surface errors?
            // FIXME using a non-checked exception for now...
            throw new RuntimeException(e);
        }
    }

    /**
     * Return a map view of the cache, READ ONLY
     */
    public Map<ImmutableBytesPtr, Aggregator[]> getAsMap() {
        // Useful for iterators
        return cache.asMap();
    }

    /**
     * This function creates a iterator over the cache and the spilled data
     * structures. The key/value tuples are returned in non-deterministic order.
     */
    public EntryIterator newEntryIterator() {
        return new EntryIterator();
    }

    // Iterator that returns CacheEntries.
    // CacheEntries are either extracted from the LRU cache or from the
    // spillable data
    // structures.
    private final class EntryIterator implements Iterator<CacheEntry> {

        final Map<ImmutableBytesPtr, Aggregator[]> cacheMap;
        final Iterator<Map.Entry<ImmutableBytesPtr, Aggregator[]>> cacheIter;
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
        public CacheEntry next() {
            if (spilledCacheIter != null && spilledCacheIter.hasNext()) {
                try {
                    byte[] value = spilledCacheIter.next();
                    // Deserialize into a CacheEntry
                    CacheEntry spilledEntry = spillManager.toCacheEntry(value);

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
            Map.Entry<ImmutableBytesPtr, Aggregator[]> entry = cacheIter.next();
            CacheEntry ce = new SpillManager.CacheEntry(entry.getKey(),
                    entry.getValue());
            return ce;
        }

        /**
         * Remove??? Denied!!!
         */
        @Override
        public void remove() {
            throw new IllegalAccessError(
                    "Remove is not supported for this type of iterator");
        }
    }

    /**
     * DO NOT USE, instead use the EntryIterator
     */
    @Override
    public Set<java.util.Map.Entry<ImmutableBytesPtr, Aggregator[]>> entrySet() {
        throw new IllegalAccessError(
                "entrySet is not supported for this type of cache");
    }

    /**
     * Closes cache and releases spill resources
     * 
     * @throws IOException
     */
    public void close() throws IOException {
        // Close spillable resources
        Closeables.closeQuietly(spillManager);
    }
}