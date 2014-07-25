package org.modeshape.connector.cmis.operations;

import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.data.PropertyData;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.modeshape.common.i18n.TextI18n;
import org.modeshape.common.logging.Logger;
import org.modeshape.connector.cmis.api.SecondaryIdProcessor;
import org.modeshape.connector.cmis.features.SingleVersionOptions;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.modeshape.connector.cmis.operations.impl.CmisOperationCommons;
import org.modeshape.jcr.cache.document.DocumentTranslator;

import java.util.List;
import java.util.NoSuchElementException;
import org.apache.chemistry.opencmis.client.runtime.OperationContextImpl;
import org.modeshape.jcr.GenericCacheContainer;

/*
 * after try to get object by id
 * does additional search with query by specified property
 */
public class FilenetObjectFinderUtil implements CmisObjectFinderUtil{

    private static final Logger LOGGER = Logger.getLogger(DocumentTranslator.class);

    private Session session;
    private SingleVersionOptions singleVersionOptions;
    private LocalTypeManager localTypeManager;

    public FilenetObjectFinderUtil(Session session, LocalTypeManager localTypeManager, SingleVersionOptions singleVersionOptions) {
        this.session = session;
        this.singleVersionOptions = singleVersionOptions;
        this.localTypeManager = localTypeManager;
    }

    @Override
    public boolean doAsSingleVersion(String cmisTypeId) {
        // need to resolve jcr name to prefixed/humanReadable
//        String cmisTypeId = cmisObject.getPropertyValue(PropertyIds.OBJECT_TYPE_ID).toString();
        boolean doAsSingleVersion = singleVersionOptions.getSingleVersionExternalTypeNames().contains(cmisTypeId);
        ObjectType typeDefinition = localTypeManager.getTypeDefinition(session, cmisTypeId);

        return doAsSingleVersion && typeDefinition.getBaseTypeId() == BaseTypeId.CMIS_DOCUMENT;
    }


    @Override
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

    @Override
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

    @Override
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
    @Override
    public CmisObject find(String suggestedId) {
        long startTime = System.currentTimeMillis();
        SecondaryIdProcessor idProcessor = singleVersionOptions.getCommonIdProcessorInstance();
        boolean isVirtualId = false;
        if (idProcessor != null) {
            isVirtualId = singleVersionOptions.getCommonIdProcessorInstance().isProcessedId(suggestedId);        
        }
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
        CmisObject result;
        if (!singleVersionOptions.isConfigured()) {
            return null;
        }
        
        String searchValue = singleVersionOptions.commonIdValuePreProcess(id);
        String remoteId = getRemoteId(id, searchValue);
        if (remoteId == null) {
            LOGGER.warn(new TextI18n("CmisObjectFinderUtil::findByCommonId::Can't find remote Id using query. Return null"));
            return null;
        }
        
        try {
            LOGGER.info(new TextI18n("CmisObjectFinderUtil::findByCommonId::Gettting object from remote repository by id {0}"), remoteId);
            OperationContext getObjectContext = new OperationContextImpl();
            getObjectContext.setIncludeAllowableActions(false);
            getObjectContext.setIncludePathSegments(false);
            getObjectContext.setMaxItemsPerPage(1);
            getObjectContext.setCacheEnabled(true);
            result = session.getObject(remoteId, getObjectContext);
        } catch (CmisObjectNotFoundException nfe) {
            LOGGER.warn(nfe, new TextI18n("CmisObjectFinderUtil::find::Failed to find object by {0} = {1}. Error content: {2}"), singleVersionOptions.getCommonIdPropertyName(), searchValue, nfe.getErrorContent());
            return null;
        }

        return result;
    }
    
    private String getRemoteId(String id, String searchValue) {        
        String remoteId = (String) GenericCacheContainer.getInstance().get(id);
        if (remoteId == null) {
            long startTime = System.currentTimeMillis();
            String query = String.format(
                    singleVersionOptions.getCommonIdQuery(),
                    singleVersionOptions.getCommonIdTypeName(),
                    singleVersionOptions.getCommonIdPropertyName(),
                    searchValue);

            // Remove all unnecessary data from query results to improve performance 
            OperationContext queryContext = new OperationContextImpl();
            queryContext.setCacheEnabled(true);
            queryContext.setIncludeAllowableActions(false);
            queryContext.setIncludePathSegments(false);
            queryContext.setMaxItemsPerPage(1);
            queryContext.setFilterString("cmis:objectId");

            ItemIterable<QueryResult> queryResult = session.query(query, false, queryContext);

            if (queryResult == null) {
                LOGGER.warn(new TextI18n("CmisObjectFinderUtil::findByCommonId::Query result is null. Return null"));
                return null;
            }
            try {
                QueryResult next = queryResult.iterator().next();
                if (next != null) {
                    PropertyData<Object> cmisObjectId = next.getPropertyById(PropertyIds.OBJECT_ID);
                    LOGGER.info(new TextI18n("CmisObjectFinderUtil::findByCommonId::Query [{0}] return atlist 1 item. Time: {1} ms"), query, (System.currentTimeMillis() - startTime));
                    remoteId = cmisObjectId.getFirstValue().toString();
                } else {
                    LOGGER.warn(new TextI18n("CmisObjectFinderUtil::findByCommonId::Query total items number is [0] but must be 1 or more. Return null!!!"));
                    return null;
                }
            } catch (NoSuchElementException e) {
                LOGGER.warn(new TextI18n("CmisObjectFinderUtil::findByCommonId::Query total items number is [0] but must be 1 or more. Return null!!!"));
                return null;
            } 
            GenericCacheContainer.getInstance().put(id, remoteId);
        }
        return remoteId;
    }
}