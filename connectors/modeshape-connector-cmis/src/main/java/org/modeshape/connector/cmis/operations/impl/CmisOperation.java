package org.modeshape.connector.cmis.operations.impl;


import java.util.List;
import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.Session;
import org.infinispan.Cache;
import org.modeshape.connector.cmis.RuntimeSnapshot;
import org.modeshape.connector.cmis.config.CmisConnectorConfiguration;
import org.modeshape.connector.cmis.features.SingleVersionOptions;
import org.modeshape.connector.cmis.operations.CmisObjectFinderUtil;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.slf4j.LoggerFactory;

public abstract class CmisOperation {

    /**
     * SLF logger.
     */
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(this.getClass());
    
    public static final String OBJECT_DATA_SUFFIX = "_ObjectData";

    protected RuntimeSnapshot snapshot;
    protected CmisConnectorConfiguration config;

    // ease access // probably temporary
    protected Session session;
    protected LocalTypeManager localTypeManager;
    protected SingleVersionOptions singleVersionOptions;
    protected CmisObjectFinderUtil finderUtil;

    protected CmisOperation(RuntimeSnapshot snapshot,
                            CmisConnectorConfiguration config) {
        this.snapshot = snapshot;
        this.config = config;

        // assign quick access properties
        this.session = snapshot.getSession();
        this.localTypeManager = snapshot.getLocalTypeManager();
        this.singleVersionOptions = config.getSingleVersionOptions();
        this.finderUtil = snapshot.getCmisObjectFinderUtil();
    }

    // DEBUG. this method will be removed todo
    public void debug(final String... values) {
        if (config.isDebug()) {
            StringBuilder sb = new StringBuilder();
            for (String value : values) {
                sb.append(value).append(" ");
            }
            log().debug(sb.toString());
        }
    }
    
    protected String getPossibleNullString(String input) {
        return input == null ? "null" : input;
    }

    /**
     * logger.
     * @return slf logger
     */
    protected org.slf4j.Logger log() {
        return LOG;
    }
    
    protected void invalidateCache(CmisObject object, String cacheKey) {
        if (snapshot.getCache() != null) {
            cleanCacheConteiner(cacheKey, snapshot.getCache());
            cleanCacheConteiner(object.getId(), snapshot.getCache());
        }
        object.refresh();
    }
    
    private void cleanCacheConteiner(String cacheKey, Cache distributedCache) {
        String cacheConteinerKey = cacheKey + OBJECT_DATA_SUFFIX;        
        List<String> caches = (List<String>) distributedCache.get(cacheConteinerKey);
        if (caches != null) {
            for (String key : caches) {
                distributedCache.remove(key);
            }
            distributedCache.remove(cacheConteinerKey);
        }
        distributedCache.remove(cacheKey);
    }
}
