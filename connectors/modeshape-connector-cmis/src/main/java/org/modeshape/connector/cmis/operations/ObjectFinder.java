package org.modeshape.connector.cmis.operations;

import org.apache.chemistry.opencmis.client.api.CmisObject;

public interface ObjectFinder {

    CmisObject findObject(String suggestedId);


}
