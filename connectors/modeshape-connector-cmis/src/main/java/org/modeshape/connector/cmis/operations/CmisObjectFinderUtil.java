package org.modeshape.connector.cmis.operations;

import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.data.PropertyData;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.modeshape.connector.cmis.features.SingleVersionOptions;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.modeshape.connector.cmis.operations.impl.CmisOperationCommons;

import java.util.List;
import org.modeshape.common.i18n.TextI18n;
import org.modeshape.common.logging.Logger;
import org.modeshape.jcr.cache.document.DocumentTranslator;

/*
 * after try to get object by id
 * does additional search with query by specified property
 */
public class CmisObjectFinderUtil {
    
    private static final Logger LOGGER = Logger.getLogger(DocumentTranslator.class);

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
    public boolean doAsSingleVersion(String cmisTypeId) {
        // need to resolve jcr name to prefixed/humanReadable
//        String cmisTypeId = cmisObject.getPropertyValue(PropertyIds.OBJECT_TYPE_ID).toString();
        boolean doAsSingleVersion = singleVersionOptions.getSingleVersionExternalTypeNames().contains(cmisTypeId);
        ObjectType typeDefinition = localTypeManager.getTypeDefinition(session, cmisTypeId);

        return doAsSingleVersion && typeDefinition.getBaseTypeId() == BaseTypeId.CMIS_DOCUMENT;
    }


    /*
    * complete logic for id extraction for cmisObject
    */
    public String getObjectMappingId(CmisObject cmisObject) {
        if (singleVersionOptions.getCommonIdPropertyName() != null) {

            String cmisTypeId = cmisObject.getPropertyValue(PropertyIds.OBJECT_TYPE_ID).toString();
            Property<Object> commonIdProp = cmisObject.getProperty(singleVersionOptions.getCommonIdPropertyName());

            String objectMappingId = getObjectMappingId(cmisTypeId, commonIdProp);

            if (objectMappingId != null)
                return objectMappingId;
        }

        // standard logic
        return CmisOperationCommons.getMappingId(cmisObject);
    }

    public String getObjectMappingId(String cmisTypeId, PropertyData<Object> commonIdProp) {
        if (doAsSingleVersion(cmisTypeId) && commonIdProp != null) {
            if (commonIdProp instanceof List && ((List) commonIdProp).size() > 0) {
                return commonIdProp.getValues().get(0).toString();
            } else if (commonIdProp.getFirstValue() != null) {
                return commonIdProp.getFirstValue().toString();
            }
        }

        return null;
    }

    public String getObjectMappingId(QueryResult queryResult) {
        if (singleVersionOptions.getCommonIdPropertyName() != null) {

            String cmisTypeId = queryResult.getPropertyValueById(PropertyIds.OBJECT_TYPE_ID).toString();
            PropertyData<Object> commonIdProp = queryResult.getPropertyById(singleVersionOptions.getCommonIdPropertyName());

            String objectMappingId = getObjectMappingId(cmisTypeId, commonIdProp);
            if (objectMappingId != null)
                return objectMappingId;
        }

        // standard logic
        return CmisOperationCommons.getMappingId(session, queryResult, localTypeManager);
    }

    public CmisObject find(String suggestedId) {
        long startTime = System.currentTimeMillis();
        boolean isVirtualId = singleVersionOptions.getCommonIdProcessorInstance().isProcessedId(suggestedId);
        CmisObject result;
        LOGGER.info(new TextI18n("CmisObjectFinderUtil::find::Start by objectId = {0}."), suggestedId == null ? "null" : suggestedId);
        
        if (suggestedId == null) {
            LOGGER.info(new TextI18n("CmisObjectFinderUtil::find::Method finished due to objectId == null. Time: {0} ms."), System.currentTimeMillis() - startTime);                
            return null;
        }
        try {
            if (isVirtualId){
                result = findByCommonId(suggestedId);
            } else {
                return session.getObject(suggestedId);
            }                      
        } catch (CmisObjectNotFoundException nfe) {
            LOGGER.warn(nfe, new TextI18n("CmisObjectFinderUtil::find::CmisObjectNotFoundException exception. Error content {0}."), nfe.getErrorContent());
            result = findByCommonId(suggestedId);
        }
        LOGGER.info(new TextI18n("CmisObjectFinderUtil::find::Method finished by objectId = {0}. Time: {1} ms."), suggestedId, System.currentTimeMillis() - startTime);                
        return result;

    }

    private CmisObject findByCommonId(String id) {
        long startTime = System.currentTimeMillis();
        if (!singleVersionOptions.isConfigured())
            return null;

        String searchValue = singleVersionOptions.commonIdValuePreProcess(id);
        String query = String.format(
                singleVersionOptions.getCommonIdQuery(),
                singleVersionOptions.getCommonIdTypeName(),
                singleVersionOptions.getCommonIdPropertyName(),
                searchValue);
        ItemIterable<QueryResult> queryResult = session.query(query, false);
        
        if (queryResult == null) {
            LOGGER.warn(new TextI18n("CmisObjectFinderUtil::findByCommonId::Query result is empty. Return null"));            
            return null;
        }

        long totalNumItems = queryResult.getTotalNumItems();

        if (totalNumItems <= 0) {
            LOGGER.warn(new TextI18n("CmisObjectFinderUtil::findByCommonId::Query total items number is [{0}] but must be 1 or more. Return null!!!"), totalNumItems);  
            return null;
        }
        QueryResult next = queryResult.iterator().next();
        PropertyData<Object> cmisObjectId = next.getPropertyById(PropertyIds.OBJECT_ID);
        LOGGER.info(new TextI18n("CmisObjectFinderUtil::findByCommonId::Query [{0}] return {1} items. Time: {2} ms"), query, totalNumItems, (System.currentTimeMillis()-startTime));  
        try {
            String remoteId = cmisObjectId.getFirstValue().toString();
            LOGGER.info(new TextI18n("CmisObjectFinderUtil::findByCommonId::Gettting object from remote repository by id {0}"), remoteId); 
            return session.getObject(remoteId);
        } catch (CmisObjectNotFoundException nfe) {
            LOGGER.warn(nfe, new TextI18n("CmisObjectFinderUtil::find::Failed to find object by {0} = {1}. Error content: {2}"), singleVersionOptions.getCommonIdPropertyName(), searchValue, nfe.getErrorContent());            
            return null;
        }
    }
}