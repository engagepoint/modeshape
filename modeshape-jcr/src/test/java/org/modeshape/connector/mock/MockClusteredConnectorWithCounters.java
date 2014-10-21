package org.modeshape.connector.mock;

import org.infinispan.Cache;
import org.infinispan.schematic.document.Document;
import org.modeshape.jcr.RepositoryConfiguration;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.federation.spi.DocumentChanges;
import org.modeshape.jcr.federation.spi.PageKey;
import org.modeshape.jcr.value.Name;

import javax.jcr.NamespaceRegistry;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
