package org.modeshape.connector.cmis.operations.impl;


import org.apache.chemistry.opencmis.client.api.Session;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;

public abstract class CmisOperation {

    protected Session session;
    protected LocalTypeManager localTypeManager;
    private boolean debug = false;

    protected CmisOperation(Session session, LocalTypeManager localTypeManager) {
        this.session = session;
        this.localTypeManager = localTypeManager;
    }

    // DEBUG. this method will be removed todo
    public void debug(String... values) {
        if (debug) {
            for (String value : values) {
                System.out.print(value + " ");
            }
            System.out.println();
        }
    }
}
