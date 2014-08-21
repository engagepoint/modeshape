/*
 * ModeShape (http://www.modeshape.org)
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * See the AUTHORS.txt file in the distribution for a full listing of
 * individual contributors.
 *
 * ModeShape is free software. Unless otherwise indicated, all code in ModeShape
 * is licensed to you under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * ModeShape is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
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
    
}
