package com.salesforce.phoenix.cache.aggcache;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
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
import com.salesforce.phoenix.expression.aggregator.Aggregator;
import com.salesforce.phoenix.expression.aggregator.ServerAggregators;

/**
 * Class that implements an LRU cache for key/Aggregator[] tuples.
 * Keys that do not yet exists are loaded into the cache automatically.
 * Eviction happens on a size limit basis. Evicted elements are spilled to disk.
 */
public class GroupByCache extends AbstractMap<ImmutableBytesPtr, Aggregator[]> {
    private static final Logger logger = LoggerFactory.getLogger(SpillManager.class);
    
    private static final int NUM_SPILLFILES = 5;
    // TODO what are proper defaults?
    private static final int CACHE_MIN_SIZE = 10;
    private static final int CACHE_MAX_SIZE = 10000;
    // TODO Generally better to use Collection API with generics instead of array types
    public final LoadingCache<ImmutableBytesPtr, Aggregator[]> cache;
    private SpillManager spillManager = null;
    private final ObserverContext<RegionCoprocessorEnvironment> context;
    private int curNumCacheElements;
    private final int estValueSize;
    private final ServerAggregators aggregators;
       
    
    /**
     * Instantiates a Loading LRU Cache that stores key / aggregator[] tuples
     * used for group by queries
     * @param estSize
     * @param estValueSize
     * @param aggs
     * @param ctxt
     */
    public GroupByCache( long estSize, int estValueSize, ServerAggregators aggs,
                         ObserverContext<RegionCoprocessorEnvironment> ctxt ) {
        context = ctxt; 
        this.estValueSize = estValueSize;
        curNumCacheElements = 0;
        this.aggregators = aggs;
        
        // use upper and lower bounds for the cache size 
        int maxCacheSize = Math.max(CACHE_MIN_SIZE, Math.min(CACHE_MAX_SIZE,((int)estSize / estValueSize )));
        if (logger.isDebugEnabled()) {
            logger.debug("Instantiating LRU groupby cache of element size: " + maxCacheSize);
        }
        // Cache is element number bounded. Using the max of CACHE_MIN_SIZE and the est MapSize
        cache = CacheBuilder.newBuilder().maximumSize(maxCacheSize)
                .removalListener( 
                        new RemovalListener<ImmutableBytesPtr, Aggregator[]>() {
                            /*
                             * CacheRemoval listener implementation
                             */
                            @Override
                            public void onRemoval(RemovalNotification<ImmutableBytesPtr, Aggregator[]> notification) {
                                try {
                                    if(spillManager == null) {
                                        // Lazy instantiation of spillable data structures
                                        //
                                        // Only create spill data structs if LRU cache is too small
                                        spillManager = new SpillManager(NUM_SPILLFILES, aggregators);
                                    }
                                    spillManager.spill(notification.getKey(), notification.getValue());
                                    // keep track of elements in cache       
                                    curNumCacheElements--;
                                }
                                catch(IOException ioe) {
                                    // TODO rework error handling
                                    throw new RuntimeException(ioe);
                                }
                            }                            
                        })                
                .build(
                       new CacheLoader<ImmutableBytesPtr, Aggregator[]>() {
                           /*
                            * CacheLoader implementation
                            */
                           @Override
                           public Aggregator[] load(ImmutableBytesPtr key) throws Exception {
                               
                               Aggregator[] aggs = null;
                               if(spillManager == null) {
                                   // No spill Manager, always assume this key is new!
                                   aggs = aggregators.newAggregators(context.getEnvironment().getConfiguration());
                               }
                               else {
                                   // Spill manager present, check if key has been spilled before
                                   aggs = spillManager.loadEntry(key);
                                    if(aggs == null) {
                                        // No, key never spilled before, create a new tuple
                                        if (logger.isDebugEnabled()) {
                                            logger.debug("Adding new aggregate bucket for row key " + Bytes.toStringBinary( key.get(),
                                                                                                                            key.getOffset(),
                                                                                                                            key.getLength()) );
                                        }                                                                                
                                        aggs = aggregators.newAggregators(context.getEnvironment().getConfiguration());                                 
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
     * put function of Map interface
     * DO NOT USE
     * Cache takes automatically care of loading non-existing elements
     */
    @Override
    public Aggregator[] put(ImmutableBytesPtr key, Aggregator[] value) {
        // Loading cache implicitly implements a put when a cache hit has occurred
        // Disable the map interface
        throw new IllegalAccessError("put is not supported for a loading cache");
    }

    /**
     * Extract an element from the Cache
     * If element is not present in in-memory cache / or in spill files
     * cache implements an implicit put() of a new key/value tuple and loads it into the cache
     */
    @Override
    public Aggregator[] get(Object key) { 
        try {
            // delegate to the cache implementation
            return cache.get((ImmutableBytesPtr)key);            
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
     * This function creates a iterator over the cache and the spilled data structures.
     * The key/value tuples are returned in non-deterministic order.
     */
    public EntryIterator newEntryIterator() {
        return new EntryIterator();
    }
    
    
    // Iterator that returns CacheEntries.
    // CacheEntries are either extracted from the LRU cache or from the spillable data
    // structures.
    private final class EntryIterator implements Iterator<CacheEntry> {
        
        final Map<ImmutableBytesPtr, Aggregator[]> cacheMap;
        final Iterator<Map.Entry<ImmutableBytesPtr, Aggregator[]>> cacheIter;
        final Iterator<byte[]> spilledCacheIter;

        private EntryIterator() {
            cacheMap = cache.asMap();
            cacheIter = cacheMap.entrySet().iterator();
            if(spillManager != null) {
                spilledCacheIter = spillManager.newDataIterator();
            }
            else {
                spilledCacheIter = null;
            }
        }
        
        @Override
        public boolean hasNext() {
            return cacheIter.hasNext();
        }
        
        @Override
        public CacheEntry next() {
            if(spilledCacheIter != null && spilledCacheIter.hasNext())
            {
                try {
                    byte[] value = spilledCacheIter.next();
                    // Deserialize into a CacheEntry
                    CacheEntry spilledEntry = spillManager.toCacheEntry(value);
                    
                    boolean notFound = false;
                    // check against map and return only if not present
                    while(cacheMap.containsKey(spilledEntry.getKey())) {
                        // LRU Cache entries always take precedence, 
                        // since they are more up to date
                        if(spilledCacheIter.hasNext()) {
                            value = spilledCacheIter.next();
                            spilledEntry = spillManager.toCacheEntry(value);
                        }
                        else {
                            notFound = true;
                            break;
                        }                        
                    }
                    if( !notFound ) {
                        // Return a spilled entry, this only happens if the entry was not
                        // found in the LRU cache
                        return spilledEntry;
                    }
                } catch(IOException ioe) {
                    // TODO rework error handling
                    throw new RuntimeException(ioe);
                }
            }
            // Spilled elements exhausted
            // Finally return all elements from LRU cache
            Map.Entry< ImmutableBytesPtr, Aggregator[] >entry = cacheIter.next();
            CacheEntry ce = new SpillManager.CacheEntry(entry.getKey(), entry.getValue());
            return ce;
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
     * DO NOT USE, instead use the EntryIterator
     */
    @Override
    public Set<java.util.Map.Entry<ImmutableBytesPtr, Aggregator[]>> entrySet() {
        throw new IllegalAccessError("entrySet is not supported for this type of cache");
    }
    
    
    /**
     * Closes cache and releases spill resources
     * @throws IOException
     */
    public void close() throws IOException {
        // Close spillable resources
        Closeables.closeQuietly(spillManager);        
    }
}