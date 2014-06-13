package org.modeshape.jcr;

/**
 * General contract for caching. Simply stores/retrieves data. Thread safety,
 * eviction policy etc depends on implementation.
 *
 * @param <K> Key
 * @param <T> Type of stored object
 *
 * @author leonid.marushevskiy
 */
public interface CacheService<K, T> {

    /**
     * Add element to cache. Should not throw any exception.
     *
     * @param key
     * @param instance
     */
    void put(K key, T instance);

    /**
     * Get element from cache. Should not throw any exception.
     *
     * @param key
     * @return Found element or null.
     */
    T get(K key);

    /**
     * Removes element from cache. Should not throw any exception.
     *
     * @param key
     */
    void remove(K key);
}
