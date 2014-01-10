package com.salesforce.phoenix.cache.aggcache;

import static com.salesforce.phoenix.query.QueryConstants.AGG_TIMESTAMP;
import static com.salesforce.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static com.salesforce.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;
import static com.salesforce.phoenix.query.QueryServices.GROUPBY_MAX_CACHE_SIZE_ATTRIB;
import static com.salesforce.phoenix.query.QueryServices.GROUPBY_SPILL_FILES_ATTRIB;
import static com.salesforce.phoenix.query.QueryServicesOptions.DEFAULT_GROUPBY_MAX_CACHE_MAX;
import static com.salesforce.phoenix.query.QueryServicesOptions.DEFAULT_GROUPBY_SPILL_FILES;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
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
import com.salesforce.phoenix.coprocessor.BaseRegionScanner;
import com.salesforce.phoenix.coprocessor.GroupByCache;
import com.salesforce.phoenix.expression.aggregator.Aggregator;
import com.salesforce.phoenix.expression.aggregator.ServerAggregators;
import com.salesforce.phoenix.util.KeyValueUtil;

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
public class SpillableGroupByCache implements GroupByCache {

    private static final Logger logger = LoggerFactory.getLogger(SpillableGroupByCache.class);

    // Min size of 1st level main memory cache in bytes --> lower bound
    private static final int SPGBY_CACHE_MIN_SIZE = 4096; // 4K

    // TODO Generally better to use Collection API with generics instead of
    // array types
    public final LoadingCache<ImmutableBytesWritable, Aggregator[]> cache;
    private SpillManager spillManager = null;
    private int curNumCacheElements;
    private final ServerAggregators aggregators;

    /**
     * Instantiates a Loading LRU Cache that stores key / aggregator[] tuples used for group by
     * queries
     * @param estSize
     * @param estValueSize
     * @param aggs
     * @param ctxt
     */
    public SpillableGroupByCache(final int estSizeNum, ServerAggregators aggs,
            final RegionCoprocessorEnvironment env) {
        curNumCacheElements = 0;
        this.aggregators = aggs;

        Configuration conf = env.getConfiguration();
        final long maxCacheSizeConf =
                conf.getLong(GROUPBY_MAX_CACHE_SIZE_ATTRIB, DEFAULT_GROUPBY_MAX_CACHE_MAX);
        final int numSpillFilesConf =
                conf.getInt(GROUPBY_SPILL_FILES_ATTRIB, DEFAULT_GROUPBY_SPILL_FILES);

        int estValueSize = aggregators.getEstimatedByteSize();
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
                        .removalListener(new RemovalListener<ImmutableBytesWritable, Aggregator[]>() {
                            /*
                             * CacheRemoval listener implementation
                             */
                            @Override
                            public void
                                    onRemoval(RemovalNotification<ImmutableBytesWritable, Aggregator[]> notification) {
                                try {
                                    if (spillManager == null) {
                                        // Lazy instantiation of spillable data
                                        // structures
                                        //
                                        // Only create spill data structs if LRU
                                        // cache is too small
                                        spillManager =
                                                new SpillManager(numSpillFilesConf,
                                                        aggregators, env.getConfiguration());
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
                        }).build(new CacheLoader<ImmutableBytesWritable, Aggregator[]>() {
                            /*
                             * CacheLoader implementation
                             */
                            @Override
                            public Aggregator[] load(ImmutableBytesWritable key) throws Exception {

                                Aggregator[] aggs = null;
                                if (spillManager == null) {
                                    // No spill Manager, always assume this key is new!
                                    aggs =
                                            aggregators.newAggregators(env.getConfiguration());
                                } else {
                                    // Spill manager present, check if key has been
                                    // spilled before
                                    aggs = spillManager.loadEntry(new ImmutableBytesPtr(key));
                                    if (aggs == null) {
                                        // No, key never spilled before, create a new tuple
                                        aggs =
                                                aggregators.newAggregators(env.getConfiguration());
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
        return curNumCacheElements * aggregators.getEstimatedByteSize();
    }

    /**
     * Extract an element from the Cache If element is not present in in-memory cache / or in spill
     * files cache implements an implicit put() of a new key/value tuple and loads it into the cache
     */
    @Override
    public Aggregator[] cache(ImmutableBytesWritable key) {
        try {
            // delegate to the cache implementation
            return cache.get(key);
        } catch (ExecutionException e) {
            // TODO What's the proper way to surface errors?
            // FIXME using a non-checked exception for now...
            throw new RuntimeException(e);
        }
    }

    /**
     * Iterator over the cache and the spilled data structures by returning
     * CacheEntries. CacheEntries are either extracted from the LRU cache or
     * from the spillable data structures.The key/value tuples are returned
     * in non-deterministic order.
     */
    private final class EntryIterator implements Iterator<Map.Entry<ImmutableBytesWritable, Aggregator[]>> {
        final Map<ImmutableBytesWritable, Aggregator[]> cacheMap;
        final Iterator<Map.Entry<ImmutableBytesWritable, Aggregator[]>> cacheIter;
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
        public Map.Entry<ImmutableBytesWritable, Aggregator[]> next() {
            if (spilledCacheIter != null && spilledCacheIter.hasNext()) {
                try {
                    byte[] value = spilledCacheIter.next();
                    // Deserialize into a CacheEntry
                    Map.Entry<ImmutableBytesWritable, Aggregator[]> spilledEntry = spillManager.toCacheEntry(value);

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
            Map.Entry<ImmutableBytesWritable, Aggregator[]> entry = cacheIter.next();
            return new CacheEntry<ImmutableBytesWritable>(entry.getKey(), entry.getValue());
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
     * Closes cache and releases spill resources
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        // Close spillable resources
        Closeables.closeQuietly(spillManager);
    }

    @Override
    public RegionScanner getScanner(final RegionScanner s) {
        final Iterator<Entry<ImmutableBytesWritable, Aggregator[]>>cacheIter = new EntryIterator();

        // scanner using the spillable implementation
        return new BaseRegionScanner() {
            @Override
            public HRegionInfo getRegionInfo() {
                return s.getRegionInfo();
            }

            @Override
            public void close() throws IOException {
                try {
                    s.close();
                } finally {
                    // Always close gbCache and swallow possible Exceptions
                    Closeables.closeQuietly(SpillableGroupByCache.this);
                }
            }

            @Override
            public boolean next(List<KeyValue> results) throws IOException {
                if (!cacheIter.hasNext()) {
                    return false;
                }
                Map.Entry<ImmutableBytesWritable, Aggregator[]> ce = cacheIter.next();
                ImmutableBytesWritable key = ce.getKey();
                Aggregator[] aggs = ce.getValue();
                byte[] value = aggregators.toBytes(aggs);
                if (logger.isDebugEnabled()) {
                    logger.debug("Adding new distinct group: "
                            + Bytes.toStringBinary(key.get(), key.getOffset(), key.getLength())
                            + " with aggregators " + aggs.toString() + " value = "
                            + Bytes.toStringBinary(value));
                }
                results.add(KeyValueUtil.newKeyValue(key.get(), key.getOffset(),
                    key.getLength(), SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, AGG_TIMESTAMP, value,
                    0, value.length));
                return cacheIter.hasNext();
            }
        };
    }
}