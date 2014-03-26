package org.modeshape.connector.cmis;

import org.apache.chemistry.opencmis.client.api.Session;
import org.modeshape.connector.cmis.features.SingleVersionDocumentsCache;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.modeshape.connector.cmis.operations.CmisObjectFinderUtil;
import org.modeshape.jcr.RepositoryConfiguration;

import java.util.List;
import java.util.Map;

// Cmis Connector runtime container
public class RuntimeSnapshot {

    private Session session;
    private String caughtProjectedId;
    private LocalTypeManager localTypeManager;
    private SingleVersionDocumentsCache singleVersionCache;
    // local document producer instance
    private CmisConnector.ConnectorDocumentProducer documentProducer;
    // projections for unfiled node
    private Map<String, List<RepositoryConfiguration.ProjectionConfiguration>> preconfiguredProjections;

    private CmisObjectFinderUtil cmisObjectFinderUtil;

    public RuntimeSnapshot(Session session, LocalTypeManager localTypeManager, SingleVersionDocumentsCache singleVersionCache,
                           CmisConnector.ConnectorDocumentProducer documentProducer,
                           Map<String, List<RepositoryConfiguration.ProjectionConfiguration>> preconfiguredProjections,
                           CmisObjectFinderUtil cmisObjectFinderUtil) {
        this.session = session;
        this.caughtProjectedId = caughtProjectedId;
        this.localTypeManager = localTypeManager;
        this.singleVersionCache = singleVersionCache;
        this.documentProducer = documentProducer;
        this.preconfiguredProjections = preconfiguredProjections;
        this.cmisObjectFinderUtil = cmisObjectFinderUtil;
    }

    public Session getSession() {
        return session;
    }

    public String getCaughtProjectedId() {
        return caughtProjectedId;
    }

    public LocalTypeManager getLocalTypeManager() {
        return localTypeManager;
    }

    public SingleVersionDocumentsCache getSingleVersionCache() {
        return singleVersionCache;
    }

    public CmisConnector.ConnectorDocumentProducer getDocumentProducer() {
        return documentProducer;
    }

    public Map<String, List<RepositoryConfiguration.ProjectionConfiguration>> getPreconfiguredProjections() {
        return preconfiguredProjections;
    }

    public CmisObjectFinderUtil getCmisObjectFinderUtil() {
        return cmisObjectFinderUtil;
    }

    //

    public void setCaughtProjectedId(String caughtProjectedId) {
        this.caughtProjectedId = caughtProjectedId;
    }

    public void setPreconfiguredProjections(Map<String, List<RepositoryConfiguration.ProjectionConfiguration>> preconfiguredProjections) {
        this.preconfiguredProjections = preconfiguredProjections;
    }

}
