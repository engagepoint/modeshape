package org.modeshape.connector.cmis.operations.impl;


import org.apache.chemistry.opencmis.client.api.Session;
import org.modeshape.connector.cmis.RuntimeSnapshot;
import org.modeshape.connector.cmis.config.CmisConnectorConfiguration;
import org.modeshape.connector.cmis.features.SingleVersionOptions;
import org.modeshape.connector.cmis.operations.CmisObjectFinderUtil;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;

public abstract class CmisOperation {

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
    public void debug(String... values) {
        if (config.isDebug()) {
            for (String value : values) {
//                System.out.print(value + " ");
            }
//            System.out.println();
        }
    }
}
