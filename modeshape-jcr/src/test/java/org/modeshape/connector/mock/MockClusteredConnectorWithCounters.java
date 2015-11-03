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
package org.modeshape.connector.mock;

import org.infinispan.Cache;
import org.infinispan.schematic.document.Document;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.spi.federation.DocumentChanges;
import org.modeshape.jcr.spi.federation.PageKey;
import org.modeshape.jcr.value.Name;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jcr.NamespaceRegistry;

/**
 * Created by vyacheslav.polulyakh on 10/20/2014.
 */
public class MockClusteredConnectorWithCounters extends MockConnector {

    protected static Map<String, CounterContainer> counters = new ConcurrentHashMap<String, CounterContainer>(10);

    protected static Map<String, Document> documentsByLocation = new ConcurrentHashMap<String, Document>(10);
    protected static Map<String, Document> documentsById = new ConcurrentHashMap<String, Document>(10);

    @Override
    protected Map<String, Document> getDocumentsByLocationMap() {
        return documentsByLocation;
    }

    @Override
    protected Map<String, Document> getDocumentsByIdMap() {
        return documentsById;
    }

    @Override
    public void initialize(NamespaceRegistry registry, NodeTypeManager nodeTypeManager, Cache cache) {
        synchronized (MockClusteredConnectorWithCounters.class) {
            super.initialize(registry, nodeTypeManager, cache);
        }
    }

    @Override
    public Document getDocumentById(String id) {
        synchronized (MockClusteredConnectorWithCounters.class) {
            getCounter(id).incrementAndGetDocumentByIdCounter();
            return super.getDocumentById(id);
        }
    }

    @Override
    public String getDocumentId(String path) {
        synchronized (MockClusteredConnectorWithCounters.class) {
            return super.getDocumentId(path);
        }
    }

    @Override
    public Collection<String> getDocumentPathsById(String id) {
        synchronized (MockClusteredConnectorWithCounters.class) {
            return super.getDocumentPathsById(id);
        }
    }

    @Override
    public boolean removeDocument(String id) {
        synchronized (MockClusteredConnectorWithCounters.class) {
            return super.removeDocument(id);
        }
    }

    @Override
    public boolean hasDocument(String id) {
        synchronized (MockClusteredConnectorWithCounters.class) {
            return super.hasDocument(id);
        }
    }

    @Override
    public void storeDocument(Document document) {
        synchronized (MockClusteredConnectorWithCounters.class) {
            super.storeDocument(document);
        }
    }

    @Override
    public String newDocumentId(String parentId, Name newDocumentName, Name newDocumentPrimaryType) {
        synchronized (MockClusteredConnectorWithCounters.class) {
            return super.newDocumentId(parentId, newDocumentName, newDocumentPrimaryType);
        }
    }

    @Override
    public void updateDocument(DocumentChanges documentChanges) {
        synchronized (MockClusteredConnectorWithCounters.class) {
            super.updateDocument(documentChanges);
        }
    }

    @Override
    public Document getChildren(PageKey pageKey) {
        synchronized (MockClusteredConnectorWithCounters.class) {
            return super.getChildren(pageKey);
        }
    }

    protected CounterContainer getCounter(String id) {
        if (!counters.containsKey(id) || counters.get(id) == null) {
            counters.put(id, new CounterContainer());
        }

        return counters.get(id);
    }

    public static Map<String, CounterContainer> getCounters() {
        return counters;
    }
}
