package org.modeshape.jcr;

/**
 *
 * @author leonid.marushevskiy
 */
public class GenericCacheContainer {
    private static CacheService<String, Object> instance;
    private static final int MAX_CACHE_CAPACITY = 10000;
    private static final int INITIAL_CACHE_CAPACITY = 100;
    
    public static synchronized CacheService<String, Object> getInstance() {
        if (instance == null) {
            instance = new MapCacheService<String, Object>(INITIAL_CACHE_CAPACITY, MAX_CACHE_CAPACITY);
        }
        return instance;
    }
    
}
