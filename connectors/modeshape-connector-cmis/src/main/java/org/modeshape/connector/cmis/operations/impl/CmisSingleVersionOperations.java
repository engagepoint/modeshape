package org.modeshape.connector.cmis.operations.impl;

import org.apache.chemistry.opencmis.client.api.ObjectType;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.infinispan.schematic.document.Document;
import org.infinispan.schematic.document.EditableDocument;
import org.modeshape.connector.cmis.operations.CmisObjectFinderUtil;
import org.modeshape.connector.cmis.ObjectId;
import org.modeshape.connector.cmis.features.SingleVersionDocumentsCache;
import org.modeshape.connector.cmis.features.SingleVersionOptions;
import org.modeshape.connector.cmis.features.TempDocument;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.modeshape.connector.cmis.mapping.MappedCustomType;
import org.modeshape.connector.cmis.operations.BinaryContentProducerInterface;
import org.modeshape.connector.cmis.operations.DocumentProducer;
import org.modeshape.jcr.JcrLexicon;
import org.modeshape.jcr.federation.spi.DocumentWriter;
import org.modeshape.jcr.value.Name;

import javax.jcr.nodetype.NodeType;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class CmisSingleVersionOperations extends CmisOperation {

    private SingleVersionOptions singleVersionOptions;
    private SingleVersionDocumentsCache singleVersionCache;
    private DocumentProducer documentProducer;

    public CmisSingleVersionOperations(Session session, LocalTypeManager localTypeManager, CmisObjectFinderUtil fUtil,
                                       SingleVersionOptions singleVersionOptions,
                                       SingleVersionDocumentsCache singleVersionCache,
                                       DocumentProducer documentProducer) {
        super(session, localTypeManager, fUtil);
        this.singleVersionOptions = singleVersionOptions;
        this.singleVersionCache = singleVersionCache;
        this.documentProducer = documentProducer;
    }

    public boolean doStoreAsSingleVersion(ObjectId objectId) {
        return singleVersionCache.containsKey(objectId.getIdentifier());
    }

    public void storeDocument(ObjectId objectId, Document document, CmisNewObjectCombinedOperation cmisStoreOperation,
                              BinaryContentProducerInterface binaryContentProducer) {
        TempDocument tempDocument = singleVersionCache.get(objectId.getIdentifier());
        if (objectId.getType() == ObjectId.Type.CONTENT) {
            cmisStoreOperation.storeDocument(
                    tempDocument.getParentId(), tempDocument.getName(), tempDocument.getPrimaryType(),
                    tempDocument.getDocument(),
                    document, binaryContentProducer);
            singleVersionCache.remove(objectId.getIdentifier());
            // seems must clean reference as well todo check
            singleVersionCache.removeReference(tempDocument.getParentId(), objectId.getIdentifier());
        } else {
            tempDocument.setDocument(document);
        }
    }

    public boolean doAsSingleVersion(Name primaryType) {
        MappedCustomType mappedType = localTypeManager.getMappedTypes().findByJcrName(primaryType.toString());
        String cmisObjectTypeName = mappedType.getExtName();
        // need to resolve jcr name to prefixed/humanReadable
        boolean doAsSingleVersion = singleVersionOptions.getSingleVersionTypeNames().contains(primaryType);
        ObjectType typeDefinition = localTypeManager.getTypeDefinition(session, cmisObjectTypeName);

        return doAsSingleVersion && typeDefinition.getBaseTypeId() == BaseTypeId.CMIS_DOCUMENT;
    }

    public String newDocumentId(String parentId,
                                Name name,
                                Name primaryType) {

        String resultGuid = singleVersionOptions.getSingleVersionGUIDPrefix() + UUID.randomUUID().toString();
        TempDocument value = new TempDocument(parentId, name, primaryType);
        singleVersionCache.put(resultGuid, value);
        // parent
        List<String> childValues = singleVersionCache.getReferences(parentId);
        if (childValues == null) childValues = new ArrayList<String>();
        childValues.add(resultGuid);
        singleVersionCache.putReferences(parentId, childValues);

        return resultGuid;
    }

    public void addCachedChildren(String id, DocumentWriter writer) {
        if (singleVersionCache.containsReferences(id)) {
            List<String> strings = singleVersionCache.getReferences(id);
            for (String childId : strings) {
                TempDocument tempDocument = singleVersionCache.get(childId);
                if (tempDocument != null)
                    writer.addChild(childId, tempDocument.getName().getLocalName());
            }
        }
    }

    public Document getCachedTempDocument(ObjectId suggestedObjectId) {
        TempDocument theParams = singleVersionCache.get(suggestedObjectId.getIdentifier());
        if (theParams.getDocument() != null) {
            Document document = theParams.getDocument();
            System.out.println("Return cached document:: " + document);
            return document;
        }
        if (suggestedObjectId.getType() == ObjectId.Type.OBJECT) {
            DocumentWriter writer = documentProducer.getNewDocument(suggestedObjectId.toString());
            // set correct type
            writer.setPrimaryType(theParams.getPrimaryType());
            // parents
            writer.setParents(ObjectId.toString(ObjectId.Type.OBJECT, theParams.getParentId()));
            // fill properties?
            // reference
            writer.addMixinType(NodeType.MIX_REFERENCEABLE);
            writer.addProperty(JcrLexicon.UUID, suggestedObjectId.toString());
            // content node - mandatory child for a document
//                writer.addChild(ObjectId.toString(ObjectId.Type.CONTENT, id), JcrConstants.JCR_CONTENT);
            EditableDocument document = writer.document();
            System.out.println("Return cached document by init params:: " + document);
            return document;
        } else if (suggestedObjectId.getType() == ObjectId.Type.CONTENT) {
            return null;
                /*String contentId = ObjectId.toString(ObjectId.Type.CONTENT, objectId.getIdentifier());
                DocumentWriter writer = newDocument(contentId);

                writer.setPrimaryType(NodeType.NT_RESOURCE);
                writer.setParent(objectId.getIdentifier());

                // reference
                writer.addMixinType(NodeType.MIX_REFERENCEABLE);
                writer.addProperty(JcrLexicon.UUID, contentId);*/

//                Property<Object> lastModified = doc.getProperty(PropertyIds.LAST_MODIFICATION_DATE);
//                Property<Object> lastModifiedBy = doc.getProperty(PropertyIds.LAST_MODIFIED_BY);

//                writer.addProperty(JcrLexicon.LAST_MODIFIED, localTypeManager.getPropertyUtils().jcrValues(lastModified));
//                writer.addProperty(JcrLexicon.LAST_MODIFIED_BY, localTypeManager.getPropertyUtils().jcrValues(lastModifiedBy));

//                writer.addMixinType(NodeType.MIX_CREATED);
//                Property<Object> created = doc.getProperty(PropertyIds.CREATION_DATE);
//                Property<Object> createdBy = doc.getProperty(PropertyIds.CREATED_BY);
//                writer.addProperty(JcrLexicon.CREATED, localTypeManager.getPropertyUtils().jcrValues(created));
//                writer.addProperty(JcrLexicon.CREATED_BY, localTypeManager.getPropertyUtils().jcrValues(createdBy));

                /*EditableDocument document = writer.document();
                System.out.println("Return cached document content by init params:: " + document);
                return document;*/
        }
        throw new CmisObjectNotFoundException("Cached object is not found under given key");
    }
}
