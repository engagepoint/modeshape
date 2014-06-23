package org.modeshape.jcr;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author leonid.marushevskiy
 */
public class MapCacheService<K, T> implements CacheService<K, T> {

    private static final int DEFAULT_MAX_CACHE_CAPACITY = 1000;
    private static final int DEFAULT_INITIAL_CACHE_CAPACITY = 100;
    private Map<K, T> mainCache;
    

    public MapCacheService() {
        this.mainCache = Collections.synchronizedMap(new LRUMap<K, T>(DEFAULT_INITIAL_CACHE_CAPACITY, DEFAULT_MAX_CACHE_CAPACITY));     
    }

    public MapCacheService(int initioalCapacity, int cacheCapacity) {          
        this.mainCache = Collections.synchronizedMap(new LRUMap<K, T>(initioalCapacity, cacheCapacity));     
    }    

    @Override
    public void put(K key, T instance) {
        if (instance != null) {
            mainCache.put(key, instance);
        }
    }

    @Override
    public T get(K key) {
        return mainCache.get(key);
    }

    @Override
    public void remove(K key) {
        mainCache.remove(key);
    }

    @Override
    public int size() {
        return mainCache.size();
    }
    
    private class LRUMap<K, V> extends LinkedHashMap<K, V> {

        private int maxCapacity;

        public LRUMap(int initialCapacity, int maxCapacity) {
            super(initialCapacity, 0.75f, true);
            this.maxCapacity = maxCapacity;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > this.maxCapacity;
        }
    }

}
