package org.modeshape.jcr;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;

/**
 * @author leonid.marushevskiy
 */
public class GenericCacheService<K, T> implements CacheService<K, T> {

    public static final int DEFAULT_CACHE_CAPACITY = 1000;
    public static final int DEFAULT_EXPIRATION_IN_SECONDS = 600;
    private final Cache<K, T> mainCache;

    public GenericCacheService() {
        this(DEFAULT_CACHE_CAPACITY, DEFAULT_EXPIRATION_IN_SECONDS);
    }

    public GenericCacheService(int cacheCapacity, int expireAfterWriteSeconds) {
        this.mainCache = CacheBuilder.newBuilder().maximumSize(cacheCapacity).expireAfterWrite(expireAfterWriteSeconds, TimeUnit.SECONDS).build();
    }

    @Override
    public void put(K key, T instance) {
        if (instance != null) {
            mainCache.put(key, instance);
        }
    }

    @Override
    public T get(K key) {
        return mainCache.getIfPresent(key);
    }

    @Override
    public void remove(K key) {
        mainCache.invalidate(key);
    }

}
