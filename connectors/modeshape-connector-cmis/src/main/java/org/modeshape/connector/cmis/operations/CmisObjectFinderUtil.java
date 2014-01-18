package org.modeshape.connector.cmis.operations;

import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.data.PropertyData;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.infinispan.schematic.internal.delta.SetValueOperation;
import org.modeshape.connector.cmis.features.SingleVersionOptions;
import org.modeshape.connector.cmis.operations.impl.CmisOperationCommons;

import java.util.List;

/*
 * after try to get object by id
 * does additional search with query by specified property
 */
public class CmisObjectFinderUtil {

    private String commonIdPropertyName;
    private String commonIdTypeName;
    private String commonIdQuery;
    private Session session;

    public CmisObjectFinderUtil(Session session, SingleVersionOptions singleVersionOptions) {
        this.commonIdPropertyName = singleVersionOptions.getCommonIdPropertyName();
        this.commonIdTypeName = singleVersionOptions.getCommonIdTypeName();
        this.commonIdQuery = singleVersionOptions.getCommonIdQuery();
        this.session = session;
    }


    /*
    * complete logic for id extraction for cmisObject
    */
    public String getObjectMappingId(CmisObject cmisObject) {

        if (commonIdPropertyName != null) {
            // use common ID instead
            Property<Object> commonIdProp = cmisObject.getProperty(commonIdPropertyName);

            if (commonIdProp != null) {
                if (commonIdProp instanceof List && ((List) commonIdProp).size() > 0) {
                    // todo check get list logic
                    return commonIdProp.getValues().get(0).toString(); // todo validate prefix ?
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
        if (commonIdPropertyName == null || commonIdTypeName == null || commonIdQuery == null)
            return null;

        String query = String.format(commonIdQuery, commonIdTypeName, commonIdPropertyName,/* id.replace("-","")*/ id);
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
            System.out.println("Failed to find object by " + commonIdPropertyName + " = " + id.replace("-", ""));
            return null;
        }
    }
}