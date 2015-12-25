/*
 * ModeShape (http://www.modeshape.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
