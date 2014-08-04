/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.modeshape.connector.cmis.cache;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.chemistry.opencmis.client.runtime.cache.Cache;
import org.apache.chemistry.opencmis.client.runtime.cache.CacheImpl;
import org.apache.chemistry.opencmis.commons.data.ObjectParentData;

/**
 * Generic cache implementation. The cache can get as input parameter some third party implementation of cache.
 */
public class DistriburedCmisCacheImpl extends CacheImpl implements Cache {
    
    public static final String PARENTS_SUFFIX = "_Parents";
    private final ConcurrentMap distributedCache;
    
    /**
     * Default constructor.
     */
    public DistriburedCmisCacheImpl(ConcurrentMap distributedCache) {
        super();
        this.distributedCache = distributedCache;        
    }        

    @Override
    public void clear() {
        super.clear();
        distributedCache.clear();
    }

    @Override
    public boolean containsParents(String objectId) {
        return distributedCache.containsKey(objectId+PARENTS_SUFFIX);
    }

    @Override
    public List<ObjectParentData> getParents(String objectId) {
        return (List<ObjectParentData>) distributedCache.get(objectId+PARENTS_SUFFIX);
    }

    @Override
    public void putParents(String objectId, List<ObjectParentData> parents) {
        distributedCache.put(objectId+PARENTS_SUFFIX, parents);
    }

    @Override
    public void removeParents(String objectId) {
        distributedCache.remove(objectId+PARENTS_SUFFIX);
    }
    
}
