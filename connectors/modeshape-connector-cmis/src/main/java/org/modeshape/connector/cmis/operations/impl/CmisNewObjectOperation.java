package org.modeshape.connector.cmis.operations.impl;

import org.apache.chemistry.opencmis.client.api.Folder;
import org.apache.chemistry.opencmis.client.api.ObjectType;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.definitions.DocumentTypeDefinition;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.chemistry.opencmis.commons.enums.Updatability;
import org.apache.chemistry.opencmis.commons.enums.VersioningState;
import org.modeshape.connector.cmis.operations.CmisObjectFinderUtil;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.modeshape.connector.cmis.ObjectId;
import org.modeshape.connector.cmis.mapping.MappedCustomType;
import org.modeshape.jcr.value.Name;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class CmisNewObjectOperation extends CmisOperation {


    public CmisNewObjectOperation(Session session, LocalTypeManager localTypeManager,CmisObjectFinderUtil finderUtil) {
        super(session, localTypeManager,finderUtil);
    }

    public String newDocumentId(String parentId,
                                Name name,
                                Name primaryType) {
        Map<String, Object> params = new HashMap<String, Object>();
        try {
            // let'start from checking primary type
            if (primaryType.getLocalName().equals("resource")) {
                // nt:resource node belongs to cmis:document's content thus
                // we must return just parent id without creating any CMIS object
                return ObjectId.toString(ObjectId.Type.CONTENT, parentId);
            }

            // all other node types belong to cmis object
            MappedCustomType mappedType = localTypeManager.getMappedTypes().findByJcrName(primaryType.toString());
            String cmisObjectTypeName = mappedType.getExtName();

            // Ivan, we can pick up object type and property definition map from CMIS repo
            // if not found consider to do an alternative search
            ObjectType objectType = localTypeManager.getTypeDefinition(session, cmisObjectTypeName);
            if (!objectType.isBaseType() /* todo do it other way */) {
                Map<String, PropertyDefinition<?>> propDefs = objectType.getPropertyDefinitions();

                // assign mandatory properties
                Collection<PropertyDefinition<?>> list = propDefs.values();
                for (PropertyDefinition<?> pdef : list) {
                    if (pdef.isRequired() && pdef.getUpdatability() == Updatability.READWRITE) {
                        params.put(pdef.getId(), CmisOperationCommons.getRequiredPropertyValue(pdef));
                    }
                }
            }

            Folder parent = null;

            if (!ObjectId.isUnfiledStorage(parentId)) {
                parent = (Folder) session.getObject(parentId);
            }

            // assign(override) 100% mandatory properties
            params.put(PropertyIds.OBJECT_TYPE_ID, objectType.getId());
            params.put(PropertyIds.NAME, name.getLocalName());

            String result = null;
            // create object and id for it.
            switch (objectType.getBaseTypeId()) {
                case CMIS_FOLDER:

                    String path = parent.getPath() + "/" + name.getLocalName();
                    params.put(PropertyIds.PATH, path);
                    String newFolderId = parent.createFolder(params).getId();
                    result = newFolderId;

                    break;
                case CMIS_DOCUMENT:
                    VersioningState versioningState = VersioningState.NONE;

                    if (objectType instanceof DocumentTypeDefinition) {
                        DocumentTypeDefinition docType = (DocumentTypeDefinition) objectType;
                        versioningState = docType.isVersionable() ? VersioningState.MAJOR : versioningState;
                    }

                    if (parent == null) {
                        // unfiled
                        result = session.createDocument(params, null, null, versioningState, null, null, null).getId();
                        String mappingId = CmisOperationCommons.getMappingId(session.getObject(result));
                        result = ObjectId.toString(ObjectId.Type.OBJECT, mappingId);
                    } else {
                        org.apache.chemistry.opencmis.client.api.Document document = parent.createDocument(params, null, versioningState);
                        result = ObjectId.toString(ObjectId.Type.OBJECT, CmisOperationCommons.getMappingId(document));
                    }

                    break;
                default:
                    debug("return null. base type id is ", objectType.getBaseTypeId().value());
            }

            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
