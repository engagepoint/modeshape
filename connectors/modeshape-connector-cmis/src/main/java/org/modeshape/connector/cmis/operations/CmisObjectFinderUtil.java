package org.modeshape.connector.cmis.operations;

import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.data.PropertyData;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.infinispan.schematic.internal.delta.SetValueOperation;
import org.modeshape.connector.cmis.features.SingleVersionOptions;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.modeshape.connector.cmis.mapping.MappedCustomType;
import org.modeshape.connector.cmis.operations.impl.CmisOperationCommons;
import org.modeshape.jcr.value.Name;

import java.util.List;

/*
 * after try to get object by id
 * does additional search with query by specified property
 */
public class CmisObjectFinderUtil {

    private Session session;
    private SingleVersionOptions singleVersionOptions;
    private LocalTypeManager localTypeManager;

    public CmisObjectFinderUtil(Session session, LocalTypeManager localTypeManager, SingleVersionOptions singleVersionOptions) {
        this.session = session;
        this.singleVersionOptions = singleVersionOptions;
        this.localTypeManager = localTypeManager;
    }

    /*
    * tests target type to define whether it has to be saved as singleVersion
    * criteria: type must be listed in SingleVersionOptions.singleVersionExternalTypesIndex + must be a descendant of cmis:document
    *
    * there is another option that might be applied while read objects is to process all the descendants of commonIdType
    */
    public boolean doAsSingleVersion(CmisObject cmisObject) {
        // need to resolve jcr name to prefixed/humanReadable
        String cmisTypeId = cmisObject.getPropertyValue(PropertyIds.OBJECT_TYPE_ID).toString();
        boolean doAsSingleVersion = singleVersionOptions.getSingleVersionExternalTypeNames().contains(cmisTypeId);
        ObjectType typeDefinition = localTypeManager.getTypeDefinition(session, cmisTypeId);

        return doAsSingleVersion && typeDefinition.getBaseTypeId() == BaseTypeId.CMIS_DOCUMENT;
    }


    /*
    * complete logic for id extraction for cmisObject
    */
    public String getObjectMappingId(CmisObject cmisObject) {

        if (singleVersionOptions.getCommonIdPropertyName() != null) {
            // use common ID instead
            Property<Object> commonIdProp = cmisObject.getProperty(singleVersionOptions.getCommonIdPropertyName());

            if (doAsSingleVersion(cmisObject) && commonIdProp != null) {
                if (commonIdProp instanceof List && ((List) commonIdProp).size() > 0) {
                    return commonIdProp.getValues().get(0).toString();
                } else if (commonIdProp.getValueAsString() != null) {
                    return commonIdProp.getValueAsString();
                }
            }
        }
        // standard logic
        return CmisOperationCommons.getMappingId(cmisObject);
    }

    public CmisObject find(String suggestedId) {
        try {
            return session.getObject(suggestedId);
        } catch (CmisObjectNotFoundException nfe) {
            return findByCommonId(suggestedId);
        }
    }

    private CmisObject findByCommonId(String id) {
        if (!singleVersionOptions.isConfigured())
            return null;

        String searchValue = singleVersionOptions.commonIdValuePreProcess(id);
        String query = String.format(
                singleVersionOptions.getCommonIdQuery(),
                singleVersionOptions.getCommonIdTypeName(),
                singleVersionOptions.getCommonIdPropertyName(),
                searchValue);
        System.out.println("Trying to find object using query <" + query + ">");
        ItemIterable<QueryResult> queryResult = session.query(query, false);

        if (queryResult == null || queryResult.getTotalNumItems() <= 0 || queryResult.getTotalNumItems() > 1) {
            System.out.println("query result is empty");
            return null;
        }

        System.out.println("got someth from query");
        QueryResult next = queryResult.iterator().next();
        PropertyData<Object> cmisObjectId = next.getPropertyById(PropertyIds.OBJECT_ID);

        try {
            System.out.println("gettting object by id: " + cmisObjectId.getFirstValue().toString());
            return session.getObject(cmisObjectId.getFirstValue().toString());
        } catch (CmisObjectNotFoundException nfe) {
            System.out.println("Failed to find object by " + singleVersionOptions.getCommonIdPropertyName() + " = " + searchValue);
            return null;
        }
    }
}