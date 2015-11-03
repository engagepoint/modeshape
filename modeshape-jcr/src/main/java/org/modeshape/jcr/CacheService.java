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
    
    int size();
}
