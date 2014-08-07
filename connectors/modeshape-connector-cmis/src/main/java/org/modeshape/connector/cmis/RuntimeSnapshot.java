package org.modeshape.connector.cmis;

import org.apache.chemistry.opencmis.client.api.Session;

import org.apache.commons.lang3.StringUtils;


import org.modeshape.connector.cmis.features.SingleVersionDocumentsCache;
import org.modeshape.connector.cmis.features.SingleVersionOptions;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.modeshape.connector.cmis.operations.CmisObjectFinderUtil;
import org.modeshape.connector.cmis.operations.FilenetObjectFinderUtil;
import org.modeshape.jcr.RepositoryConfiguration;
import org.slf4j.LoggerFactory;


import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import org.infinispan.Cache;

/**
 * Cmis Connector runtime container.
 */
public class RuntimeSnapshot {

    /**
     * lsf4j logger.
     */
    protected static org.slf4j.Logger LOG = LoggerFactory.getLogger(RuntimeSnapshot.class);

    private Session session;
    private String caughtProjectedId;
    private LocalTypeManager localTypeManager;
    private SingleVersionDocumentsCache singleVersionCache;
    // local document producer instance
    private CmisConnector.ConnectorDocumentProducer documentProducer;
    // projections for unfiled node
    private Map<String, List<RepositoryConfiguration.ProjectionConfiguration>> preconfiguredProjections;

    private CmisObjectFinderUtil cmisObjectFinderUtil;

    private LanguageDialect languageDialect;
    
    private Cache cache;

    public RuntimeSnapshot(Session session, LocalTypeManager localTypeManager, SingleVersionOptions singleVersionOptions, SingleVersionDocumentsCache singleVersionCache,
                           CmisConnector.ConnectorDocumentProducer documentProducer,
                           Map<String, List<RepositoryConfiguration.ProjectionConfiguration>> preconfiguredProjections,
                           String cmisObjectFinderUtil, String languageDialect, Cache cache) {
        this.session = session;
        this.caughtProjectedId = caughtProjectedId;
        this.localTypeManager = localTypeManager;
        this.singleVersionCache = singleVersionCache;
        this.documentProducer = documentProducer;
        this.preconfiguredProjections = preconfiguredProjections;
        this.cache = cache;
        this.cmisObjectFinderUtil = initFinder(cmisObjectFinderUtil, singleVersionOptions);
        this.languageDialect = initLanguageDialect(languageDialect);      
    }

    private CmisObjectFinderUtil initFinder(String cmisObjectFinderUtil, SingleVersionOptions singleVersionOptions) {
        if (StringUtils.isEmpty(cmisObjectFinderUtil)) {
            LOG.warn(String.format("cmisObjectFinderUtil parameter is not defined, default realization '%s' will be used", FilenetObjectFinderUtil.class.toString()));
            return new FilenetObjectFinderUtil(session, localTypeManager, singleVersionOptions, cache);
        }
        try {
            Constructor c = Class.forName(cmisObjectFinderUtil).getConstructor(Session.class, LocalTypeManager.class, SingleVersionOptions.class, Cache.class);
            CmisObjectFinderUtil finderUtil = (CmisObjectFinderUtil) c.newInstance(session, localTypeManager, singleVersionOptions, cache);
            return finderUtil;
        } catch (Exception e) {
            LOG.error(String.format("Instatiation of cmisObjectFinderUtil has failed, %s", e.getMessage()), e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private LanguageDialect initLanguageDialect(String value) {
        String defaultValue = "opencmis";
        if (StringUtils.isEmpty(value)) {
            LOG.warn(String.format("languageDialect parameter is empty, default '%s' will be used ", defaultValue));
            return new LanguageDialect(defaultValue);
        }

        return new LanguageDialect(value);

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

    public LanguageDialect getLanguageDialect() {
        return languageDialect;
    }
    
    public Cache getCache() {
        return cache;
    }

    //

    public void setCaughtProjectedId(String caughtProjectedId) {
        this.caughtProjectedId = caughtProjectedId;
    }

    public void setPreconfiguredProjections(Map<String, List<RepositoryConfiguration.ProjectionConfiguration>> preconfiguredProjections) {
        this.preconfiguredProjections = preconfiguredProjections;
    }

    public static org.slf4j.Logger getLOG() {
        return LOG;
    }
}
