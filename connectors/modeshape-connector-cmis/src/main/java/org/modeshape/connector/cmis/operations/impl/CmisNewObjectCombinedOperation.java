package org.modeshape.connector.cmis.operations.impl;

import java.text.MessageFormat;
import org.apache.chemistry.opencmis.client.api.Folder;
import org.apache.chemistry.opencmis.client.api.ObjectType;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.definitions.DocumentTypeDefinition;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.chemistry.opencmis.commons.enums.Cardinality;
import org.apache.chemistry.opencmis.commons.enums.Updatability;
import org.apache.chemistry.opencmis.commons.enums.VersioningState;
import org.infinispan.schematic.document.Document;
import org.modeshape.connector.cmis.RuntimeSnapshot;
import org.modeshape.connector.cmis.config.CmisConnectorConfiguration;
import org.modeshape.connector.cmis.ObjectId;
import org.modeshape.connector.cmis.mapping.MappedCustomType;
import org.modeshape.connector.cmis.operations.BinaryContentProducerInterface;
import org.modeshape.jcr.value.Name;

import java.util.*;
import org.apache.chemistry.opencmis.commons.exceptions.CmisStorageException;

public class CmisNewObjectCombinedOperation extends CmisOperation {

    public CmisNewObjectCombinedOperation(RuntimeSnapshot snapshot,
                                          CmisConnectorConfiguration config) {
        super(snapshot, config);
    }


    /*
    * does creation of a document in the external CMIS repository
    * submitting all the collected data: type + name, meta-data, content
    *
    * type, name, meta-data are being taken from cached TempDocument
    */
    public void storeDocument(String parentId,
                                Name name,
                                Name primaryType,
                                Document document,
                                Document documentContent,
                                BinaryContentProducerInterface binaryProducer) {
        long startTime = System.currentTimeMillis();
        debug("Start CmisNewObjectCombinedOperation:storeDocument for parentId = ", getPossibleNullString(parentId), " and name = ", name == null ? "null" : name.getLocalName());
        
        String result = null;
        Map<String, Object> cmisProperties = new HashMap<String, Object>();
        try {
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
                        cmisProperties.put(pdef.getId(), CmisOperationCommons.getRequiredPropertyValue(pdef));
                    }
                }
            }

            Folder parent = null;

            if (!ObjectId.isUnfiledStorage(parentId)) {
                parent = (Folder) session.getObject(parentId);
            }

            // assign(override) 100% mandatory properties
            cmisProperties.put(PropertyIds.OBJECT_TYPE_ID, objectType.getId());
            cmisProperties.put(PropertyIds.NAME, name.getLocalName());


            // extract node properties from the document view
            Document jcrProperties = document.getDocument("properties");


            // ask cmis repository to get property definitions
            // we will use these definitions for correct conversation
            Map<String, PropertyDefinition<?>> propDefs = objectType.getPropertyDefinitions();
            propDefs.putAll(objectType.getPropertyDefinitions());
            MappedCustomType mapping = localTypeManager.getMappedTypes().findByExtName(objectType.getId());
            // jcr properties are grouped by namespace uri
            // we will travers over all namespaces (ie group of properties)

            for (Document.Field field : jcrProperties.fields()) {
                // field has namespace uri as field's name and properties
                // as value
                // getting namespace uri and properties
                String namespaceUri = field.getName();
                Document props = field.getValueAsDocument();

                // namespace uri uniquily defines prefix for the property name
                String prefix = localTypeManager.getPrefixes().value(namespaceUri);

                // now scroll over properties
                for (Document.Field property : props.fields()) {
                    // getting jcr fully qualified name of property
                    // then determine the the name of this property
                    // in the cmis domain
                    String jcrPropertyName = prefix != null ? prefix + ":" + property.getName() : property.getName();
                    String cmisPropertyName = localTypeManager.getPropertyUtils().findCmisName(jcrPropertyName);
                    // correlate with custom mapping
                    cmisPropertyName = mapping.toExtProperty(cmisPropertyName);
                    // now we need to convert value, we will use
                    // property definition from the original cmis repo for this step
                    PropertyDefinition<?> pdef = propDefs.get(cmisPropertyName);

                    // unknown property?
                    if (pdef == null) {
                        // ignore
                        continue;
                    }

                    // make conversation for the value

                    Object cmisValue = null;
                    try {
                        cmisValue = localTypeManager.getPropertyUtils().cmisValue(pdef, property.getName(), props);
                    } catch (Exception e) {
                        debug(e.getMessage());
                    }
                    // store properties for update
                    // incorrect value won't be parsed so cmisValue will have null which may overwrite default value for required property
                    // consider not to put empty values while store ??
                    if (config.isIgnoreEmptyPropertiesOnCreate() && pdef.isRequired() && (cmisValue == null || "".equals(cmisValue.toString()))) {
                        continue;
                    }
                    // add property
                    cmisProperties.put(cmisPropertyName, cmisValue);
                }

            }
            String secondaryIdPropertyName = config.getSingleVersionOptions().getCommonIdPropertyName();
            ObjectId objectId = ObjectId.valueOf(document.getString("key"));
            PropertyDefinition<?> sidDefinition = objectType.getPropertyDefinitions().get(secondaryIdPropertyName);
            String identifier = objectId.getIdentifier();
            if (sidDefinition.getCardinality() == Cardinality.MULTI) {
                List<String> secondaryIds = new ArrayList<String>();
                secondaryIds.add(identifier);
                Object secondaryId = cmisProperties.get(secondaryIdPropertyName);
                if (secondaryId != null && secondaryId instanceof List && !isAlreadyAdded((List<String>) secondaryId, identifier)) {
                    secondaryIds.addAll((List<String>) cmisProperties.get(secondaryIdPropertyName));
                }
                cmisProperties.put(secondaryIdPropertyName, secondaryIds);
            } else {
                cmisProperties.put(secondaryIdPropertyName, identifier);
            }

            VersioningState versioningState = VersioningState.NONE;

            if (objectType instanceof DocumentTypeDefinition) {
                DocumentTypeDefinition docType = (DocumentTypeDefinition) objectType;
                versioningState = docType.isVersionable() ? VersioningState.MAJOR : versioningState;
            }

            String filename = cmisProperties.get(PropertyIds.NAME).toString();
            ContentStream stream = getContentStream(documentContent, filename, binaryProducer);
            if (parent == null) {
                // unfiled
                result = soapSession.createDocument(cmisProperties, null, stream, versioningState).getId();
            } else {
                org.apache.chemistry.opencmis.client.api.Document resultDocument = parent.createDocument(cmisProperties, stream, versioningState);
                result = ObjectId.toString(ObjectId.Type.OBJECT, resultDocument.getId());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new CmisStorageException(MessageFormat.format("Unable to store document [{0}] to external CMIS storage due to unexpected error.", (document == null ? "null" : document.getString("key"))), e);
        }
        debug("Finish CmisNewObjectCombinedOperation:storeDocument for parentId = ", parentId, " and name = ", name.getLocalName(), " with result = ", getPossibleNullString(result), ". Time:", Long.toString(System.currentTimeMillis()-startTime), "ms");
    }
    
    private boolean isAlreadyAdded(List<String> values, String commonId) {
        boolean result = false;
        for (String id : values) {
            if (commonId.contains(id) || id.equals(commonId)) {
                result = true;
                break;
            }
        }
        return result;
    }     

    private ContentStream getContentStream(Document document, String filename, BinaryContentProducerInterface binaryProducer) {
        // original object is here so converting binary value and
        return binaryProducer.jcrBinaryContent(document, filename);

    }
}
