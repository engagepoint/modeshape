package org.modeshape.connector.cmis;

import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.ItemIterable;
import org.apache.chemistry.opencmis.client.api.QueryResult;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.data.PropertyData;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;

public class CmisObjectFinderUtil {

    String commonIdPropertyName;
    String commonIdTypeName;
    String commonIdQuery;
    Session session;

    public CmisObjectFinderUtil(Session session, String commonIdPropertyName,
                                String commonIdTypeName,
                                String commonIdQuery) {
        this.commonIdPropertyName = commonIdPropertyName;
        this.commonIdTypeName = commonIdTypeName;
        this.commonIdQuery = commonIdQuery;
        this.session = session;
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