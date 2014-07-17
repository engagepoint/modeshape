package org.modeshape.connector.cmis.operations.impl;


import java.text.MessageFormat;
import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.ObjectType;
import org.apache.chemistry.opencmis.client.api.QueryResult;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.definitions.DocumentTypeDefinition;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.chemistry.opencmis.commons.enums.Cardinality;
import org.apache.chemistry.opencmis.commons.exceptions.CmisBaseException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisInvalidArgumentException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisRuntimeException;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

public class CmisOperationCommons {

    /**
     * SLF logger.
     */
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(CmisOperationCommons.class);

    /*
     *  connectors do not support versioning, so make document be referenced with versionSeriesId (if exists)
     *  in this case we always get latest version when query document
     *
     *  if external document type is not versionable then regular objectId is used
     */
    public static String getMappingId(CmisObject cmisObject) {
        return (CmisOperationCommons.isDocument(cmisObject) && CmisOperationCommons.isVersioned(cmisObject))
                ? CmisOperationCommons.asDocument(cmisObject).getVersionSeriesId()
                : cmisObject.getId();
    }

    public static String getMappingId(Session session, QueryResult result, LocalTypeManager localTypeManager) {
        String objectTypeId = result.getPropertyValueById(PropertyIds.OBJECT_TYPE_ID).toString();
        ObjectType typeDefinition = localTypeManager.getTypeDefinition(session, objectTypeId);

        boolean document = typeDefinition.getBaseTypeId() == BaseTypeId.CMIS_DOCUMENT;

        boolean versioned = (document)
                ? ((DocumentTypeDefinition) typeDefinition).isVersionable()
                : false;

        return document && versioned
                ? result.getPropertyValueById(PropertyIds.VERSION_SERIES_ID).toString()
                : result.getPropertyValueById(PropertyIds.OBJECT_ID).toString();
    }

    public static String updateVersionedDoc(Session session, CmisObject cmisObject, Map<String, ?> properties, ContentStream contentStream, boolean major) {
        org.apache.chemistry.opencmis.client.api.Document pwc = checkout(session, cmisObject);
        return checkin(pwc, properties, contentStream, major);
    }

    public static boolean isDocument(CmisObject cmisObject) {
        return cmisObject instanceof org.apache.chemistry.opencmis.client.api.Document;
    }

    public static org.apache.chemistry.opencmis.client.api.Document asDocument(CmisObject cmisObject) {
        if (!isDocument(cmisObject))
            if (cmisObject == null) {
                log().info("Object is null");
                throw new CmisInvalidArgumentException("Object is null");
            } else {
                throw new CmisInvalidArgumentException("Object is not a document: "
                    + cmisObject.getId()
                    + " with type "
                    + cmisObject.getType().getId());
            }
        return (org.apache.chemistry.opencmis.client.api.Document) cmisObject;
    }

    /*
    * checks versionable setting of the object's type
    */
    public static boolean isVersioned(CmisObject cmisObject) {
        ObjectType objectType = cmisObject.getType();
        log().info(MessageFormat.format("cmisObject name [{0}] type class [{1}]", cmisObject.getName(), objectType.getClass().getCanonicalName()));
        if (objectType instanceof DocumentTypeDefinition) {
            DocumentTypeDefinition docType = (DocumentTypeDefinition) objectType;
            log().info(MessageFormat.format("cmisObject docType.isVersionable() = [{0}]", docType.isVersionable()));
            return docType.isVersionable();
        }
        log().info(MessageFormat.format("cmisObject docType.isVersionable() = [{0}]", false));
        return false;
    }

    public static org.apache.chemistry.opencmis.client.api.Document checkout(Session session, CmisObject cmisObject) {
        org.apache.chemistry.opencmis.client.api.Document doc = (org.apache.chemistry.opencmis.client.api.Document) cmisObject;
        org.apache.chemistry.opencmis.client.api.ObjectId pwcId = doc.checkOut();
        return asDocument(session.getObject(pwcId));
    }


    public static String checkin(org.apache.chemistry.opencmis.client.api.Document pwc, Map<String, ?> properties, ContentStream contentStream, boolean major) {
        try {
            return pwc.checkIn(major, properties, contentStream, "connector's check in").getId();
        } catch (CmisBaseException e) {
            pwc.cancelCheckOut();
            throw new CmisRuntimeException(e.getMessage(), e);
        }
    }


    // todo improve this logic
    public static Object getRequiredPropertyValue(PropertyDefinition<?> remotePropDefinition) {
        if (remotePropDefinition.getCardinality() == Cardinality.MULTI)
            return Collections.singletonList("");
        return remotePropDefinition.getId();
    }

    protected static Logger log() {
        return LOG;
    }
}
