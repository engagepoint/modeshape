package org.modeshape.connector.cmis.operations;

import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.data.PropertyData;

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
