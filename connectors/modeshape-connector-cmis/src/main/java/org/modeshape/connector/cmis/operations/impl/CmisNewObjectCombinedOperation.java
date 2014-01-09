package org.modeshape.connector.cmis.operations.impl;

import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.Folder;
import org.apache.chemistry.opencmis.client.api.ObjectType;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.definitions.DocumentTypeDefinition;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.chemistry.opencmis.commons.enums.Updatability;
import org.apache.chemistry.opencmis.commons.enums.VersioningState;
import org.infinispan.schematic.document.Document;
import org.modeshape.connector.cmis.ObjectId;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.modeshape.connector.cmis.mapping.MappedCustomType;
import org.modeshape.connector.cmis.operations.BinaryContentProducerInterface;
import org.modeshape.jcr.value.Name;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.modeshape.connector.cmis.operations.impl.CmisOperationCommons.asDocument;
import static org.modeshape.connector.cmis.operations.impl.CmisOperationCommons.isDocument;
import static org.modeshape.connector.cmis.operations.impl.CmisOperationCommons.isVersioned;

public class CmisNewObjectCombinedOperation extends CmisOperation {

    private boolean ignoreEmptyPropertiesOnCreate;
    private String secondaryIdPropertyName;

    public CmisNewObjectCombinedOperation(Session session, LocalTypeManager localTypeManager,
                                          String secondaryIdPropertyName,
                                          boolean ignoreEmptyPropertiesOnCreate) {
        super(session, localTypeManager);
        this.ignoreEmptyPropertiesOnCreate = ignoreEmptyPropertiesOnCreate;
        this.secondaryIdPropertyName = secondaryIdPropertyName;
    }

    public String storeDocument(String parentId,
                                Name name,
                                Name primaryType,
                                Document document,
                                Document documentContent,
                                BinaryContentProducerInterface binaryProducer) {
        String result;
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
                    if (ignoreEmptyPropertiesOnCreate && pdef.isRequired() && (cmisValue == null || "".equals(cmisValue.toString()))) {
                        continue;
                    }
                    // add property
                    cmisProperties.put(cmisPropertyName, cmisValue);
                }

            }

//            ObjectId objectId = ObjectId.valueOf(document.getString("key"));
//            String identifier = objectId.getIdentifier().replace("-", "");
//            cmisProperties.put(secondaryIdPropertyName, Collections.singletonList(identifier));
//            cmisProperties.put(secondaryIdPropertyName, identifier);

            VersioningState versioningState = VersioningState.NONE;

            if (objectType instanceof DocumentTypeDefinition) {
                DocumentTypeDefinition docType = (DocumentTypeDefinition) objectType;
                versioningState = docType.isVersionable() ? VersioningState.MAJOR : versioningState;
            }

            ContentStream stream = getContentStream(documentContent, binaryProducer);
            if (parent == null) {
                // unfiled
                result = session.createDocument(cmisProperties, null, stream, versioningState).getId();
            } else {
                org.apache.chemistry.opencmis.client.api.Document resultDocument = parent.createDocument(cmisProperties, stream, versioningState);
                result = ObjectId.toString(ObjectId.Type.OBJECT, resultDocument.getId());
            }

            // replace id with version series id
            result = (versioningState != VersioningState.NONE)
                    ? CmisOperationCommons.asDocument(session.getObject(result)).getVersionSeriesId()
                    : result;

            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private ContentStream getContentStream(Document document, BinaryContentProducerInterface binaryProducer) {
        // original object is here so converting binary value and
        return binaryProducer.jcrBinaryContent(document);

    }
}
