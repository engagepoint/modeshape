package org.modeshape.connector.cmis.operations;

import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.data.PropertyData;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.modeshape.connector.cmis.features.SingleVersionOptions;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.modeshape.connector.cmis.operations.impl.CmisOperationCommons;

import java.util.List;
import org.modeshape.common.i18n.TextI18n;
import org.modeshape.common.logging.Logger;
import org.modeshape.connector.cmis.api.SecondaryIdProcessor;

/*
 * after try to get object by id
 * does additional search with query by specified property
 */
public interface CmisObjectFinderUtil {

    /*
    * tests target type to define whether it has to be saved as singleVersion
    * criteria: type must be listed in SingleVersionOptions.singleVersionExternalTypesIndex + must be a descendant of cmis:document
    *
    * there is another option that might be applied while read objects is to process all the descendants of commonIdType
    */
    boolean doAsSingleVersion(String cmisTypeId);

    /*
    * complete logic for id extraction for cmisObject
    */
    String getObjectMappingId(CmisObject cmisObject);

    String getObjectMappingId(String cmisTypeId, PropertyData<Object> commonIdProp);

    String getObjectMappingId(QueryResult queryResult);

    CmisObject find(String suggestedId);

    ContentStream getContentStream(String id);

}
