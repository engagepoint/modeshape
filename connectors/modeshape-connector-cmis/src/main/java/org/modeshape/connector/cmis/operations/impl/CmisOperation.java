package org.modeshape.connector.cmis.operations.impl;


import org.apache.chemistry.opencmis.client.api.Session;
import org.modeshape.connector.cmis.RuntimeSnapshot;
import org.modeshape.connector.cmis.config.CmisConnectorConfiguration;
import org.modeshape.connector.cmis.features.SingleVersionOptions;
import org.modeshape.connector.cmis.operations.CmisObjectFinderUtil;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.modeshape.jcr.api.Logger;
import org.slf4j.LoggerFactory;

public abstract class CmisOperation {

    /**
     * SLF logger.
     */
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(this.getClass());

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
}
