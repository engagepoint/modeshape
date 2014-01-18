package org.modeshape.connector.cmis.operations.impl;


import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.chemistry.opencmis.commons.definitions.TypeDefinition;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.chemistry.opencmis.commons.enums.Updatability;
import org.infinispan.schematic.document.Document;
import org.modeshape.connector.cmis.CmisObjectFinderUtil;
import org.modeshape.connector.cmis.Constants;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.modeshape.connector.cmis.mapping.MappedCustomType;
import org.modeshape.connector.cmis.ObjectId;
import org.modeshape.connector.cmis.operations.DocumentProducer;
import org.modeshape.jcr.JcrLexicon;
import org.modeshape.jcr.api.JcrConstants;
import org.modeshape.jcr.federation.spi.DocumentWriter;
import org.modeshape.jcr.federation.spi.PageKey;
import org.modeshape.jcr.federation.spi.PageWriter;
import org.modeshape.jcr.value.BinaryValue;

import javax.jcr.nodetype.NodeType;
import java.io.InputStream;
import java.util.*;

public class CmisGetObjectOperation extends CmisOperation {

    private boolean addRequiredPropertiesOnRead;
    private boolean hideRootFolderReference;
    private DocumentProducer documentProducer;
    private String projectedNodeId;
    private String remoteUnfiledNodeId;
    private String commonIdPropertyName;

    public CmisGetObjectOperation(Session session, LocalTypeManager localTypeManager,
                                  boolean addRequiredPropertiesOnRead, boolean hideRootFolderReference,
                                  String projectedNodeId,
                                  String remoteUnfiledNodeId,
                                  String commonIdPropertyName,
                                  DocumentProducer documentProducer,
                                  CmisObjectFinderUtil finderUtil) {
        super(session, localTypeManager, finderUtil);
        this.addRequiredPropertiesOnRead = addRequiredPropertiesOnRead;
        this.hideRootFolderReference = hideRootFolderReference;
        this.documentProducer = documentProducer;
        this.projectedNodeId = projectedNodeId;
        this.remoteUnfiledNodeId = remoteUnfiledNodeId;
        this.commonIdPropertyName = commonIdPropertyName;
    }


    /**
     * Translates CMIS folder object to JCR node
     *
     * @param cmisObject CMIS folder object
     * @return JCR node document.
     */
    public DocumentWriter cmisFolder(CmisObject cmisObject) {
        CmisGetChildrenOperation childrenOperation =
                new CmisGetChildrenOperation(session, localTypeManager, remoteUnfiledNodeId,
                        commonIdPropertyName, finderUtil);

        Folder folder = (Folder) cmisObject;
        DocumentWriter writer = documentProducer.getNewDocument(ObjectId.toString(ObjectId.Type.OBJECT, folder.getId()));
        // set correct type
        writer.setPrimaryType(localTypeManager.cmisTypeToJcr(cmisObject.getType().getId()).getJcrName());
        // parent
        writer.setParent(folder.getParentId());
        // properties
        cmisProperties(folder, writer);
        // children
        childrenOperation.cmisChildren(folder, writer);

        // append repository information to the root node
        if (folder.isRootFolder() && !hideRootFolderReference) {
            writer.addChild(ObjectId.toString(ObjectId.Type.REPOSITORY_INFO, ""), Constants.REPOSITORY_INFO_NODE_NAME);
        }
        if (cmisObject.getId().equals(projectedNodeId)) {
            writer.addChild(ObjectId.toString(ObjectId.Type.UNFILED_STORAGE, ""), ObjectId.Type.UNFILED_STORAGE.getValue());
        }
        // mandatory mixins
        writer.addMixinType(NodeType.MIX_REFERENCEABLE);
        writer.addProperty(JcrLexicon.UUID, cmisObject.getId());
        //
        writer.addMixinType(NodeType.MIX_LAST_MODIFIED);
        Property<Object> lastModified = folder.getProperty(PropertyIds.LAST_MODIFICATION_DATE);
        Property<Object> lastModifiedBy = folder.getProperty(PropertyIds.LAST_MODIFIED_BY);
        writer.addProperty(JcrLexicon.LAST_MODIFIED, localTypeManager.getPropertyUtils().jcrValues(lastModified));
        writer.addProperty(JcrLexicon.LAST_MODIFIED_BY, localTypeManager.getPropertyUtils().jcrValues(lastModifiedBy));
        return writer;
    }


    /**
     * Translates cmis document object to JCR node.
     *
     * @param cmisObject cmis document node
     * @return JCR node document.
     */
    public Document cmisDocument(CmisObject cmisObject, String incomingId) {
        org.apache.chemistry.opencmis.client.api.Document doc = CmisOperationCommons.asDocument(cmisObject);

        // document and internalId
        // internal id = cmisId (when folder or not versioned) || version series id
        String internalId = CmisOperationCommons.isVersioned(cmisObject) ? doc.getVersionSeriesId() : doc.getId();
        DocumentWriter writer = documentProducer.getNewDocument(ObjectId.toString(ObjectId.Type.OBJECT, incomingId));

        // set correct type
        writer.setPrimaryType(localTypeManager.cmisTypeToJcr(cmisObject.getType().getId()).getJcrName());

        // parents
        List<Folder> parents = doc.getParents();
        ArrayList<String> parentIds = new ArrayList<String>();
        for (Folder f : parents) {
            parentIds.add(ObjectId.toString(ObjectId.Type.OBJECT, f.getId()));
        }
        // no parents = unfiled
        if (parentIds.isEmpty()) parentIds.add(ObjectId.toString(ObjectId.Type.UNFILED_STORAGE, ""));
        // write parents
        writer.setParents(parentIds);

        // fill properties
        cmisProperties(doc, writer);

        // reference
        writer.addMixinType(NodeType.MIX_REFERENCEABLE);
        writer.addProperty(JcrLexicon.UUID, incomingId);

        // content node - mandatory child for a document
        writer.addChild(ObjectId.toString(ObjectId.Type.CONTENT, incomingId), JcrConstants.JCR_CONTENT);

        // modification date
        writer.addMixinType(NodeType.MIX_LAST_MODIFIED);

        return writer.document();
    }

    /**
     * Converts binary content into JCR node.
     *
     * @param id the id of the CMIS document.
     * @return JCR node representation.
     */
    public Document cmisContent(String id, String incomingId) {
        String contentId = ObjectId.toString(ObjectId.Type.CONTENT, incomingId);
        DocumentWriter writer = documentProducer.getNewDocument(contentId);

        org.apache.chemistry.opencmis.client.api.Document doc = CmisOperationCommons.asDocument(finderUtil.find(id));
        writer.setPrimaryType(NodeType.NT_RESOURCE);
        writer.setParent(incomingId);

        if (doc.getContentStream() != null) {
            InputStream is = doc.getContentStream().getStream();
            BinaryValue content = localTypeManager.getFactories().getBinaryFactory().create(is);
            writer.addProperty(JcrConstants.JCR_DATA, content);
            writer.addProperty(JcrConstants.JCR_MIME_TYPE, doc.getContentStream().getMimeType());
        }

        // reference
        writer.addMixinType(NodeType.MIX_REFERENCEABLE);
        writer.addProperty(JcrLexicon.UUID, contentId);

        Property<Object> lastModified = doc.getProperty(PropertyIds.LAST_MODIFICATION_DATE);
        Property<Object> lastModifiedBy = doc.getProperty(PropertyIds.LAST_MODIFIED_BY);

        writer.addProperty(JcrLexicon.LAST_MODIFIED, localTypeManager.getPropertyUtils().jcrValues(lastModified));
        writer.addProperty(JcrLexicon.LAST_MODIFIED_BY, localTypeManager.getPropertyUtils().jcrValues(lastModifiedBy));

        writer.addMixinType(NodeType.MIX_CREATED);
        Property<Object> created = doc.getProperty(PropertyIds.CREATION_DATE);
        Property<Object> createdBy = doc.getProperty(PropertyIds.CREATED_BY);
        writer.addProperty(JcrLexicon.CREATED, localTypeManager.getPropertyUtils().jcrValues(created));
        writer.addProperty(JcrLexicon.CREATED_BY, localTypeManager.getPropertyUtils().jcrValues(createdBy));

        return writer.document();
    }

    /**
     * Translates CMIS repository information into Node.
     *
     * @return node document.
     */
    public Document jcrUnfiled(String originalId, String caughtProjectedId) {
        DocumentWriter writer = documentProducer.getNewDocument(ObjectId.toString(ObjectId.Type.OBJECT, ObjectId.Type.UNFILED_STORAGE.getValue()));
        Folder root = session.getRootFolder();

        writer.setPrimaryType(NodeType.NT_FOLDER);
        writer.setParent(caughtProjectedId);

        writer.addMixinType(NodeType.MIX_REFERENCEABLE);
        writer.addProperty(JcrLexicon.UUID, ObjectId.Type.UNFILED_STORAGE.getValue());

        writer.addMixinType(NodeType.MIX_LAST_MODIFIED);
        Property<Object> lastModified = root.getProperty(PropertyIds.LAST_MODIFICATION_DATE);
        Property<Object> lastModifiedBy = root.getProperty(PropertyIds.LAST_MODIFIED_BY);
        writer.addProperty(JcrLexicon.LAST_MODIFIED, localTypeManager.getPropertyUtils().jcrValues(lastModified));
        writer.addProperty(JcrLexicon.LAST_MODIFIED_BY, localTypeManager.getPropertyUtils().jcrValues(lastModifiedBy));

        if (originalId.contains("#")) {
            CmisGetChildrenOperation childrenOperation =
                    new CmisGetChildrenOperation(session, localTypeManager, remoteUnfiledNodeId,
                            commonIdPropertyName, finderUtil);
            childrenOperation.getChildren(new PageKey(originalId), writer);
        } else {
            writer.addPage(ObjectId.toString(ObjectId.Type.UNFILED_STORAGE, ""), 0, Constants.DEFAULT_PAGE_SIZE, PageWriter.UNKNOWN_TOTAL_SIZE);
        }

        return writer.document();
    }

    /**
     * Converts CMIS object's properties to JCR node localTypeManager.getPropertyUtils().
     *
     * @param object CMIS object
     * @param writer JCR node representation.
     */
    private void cmisProperties(CmisObject object,
                                DocumentWriter writer) {
        // convert properties
        List<Property<?>> cmisProperties = object.getProperties();
        TypeDefinition type = object.getType();
        MappedCustomType typeMapping = localTypeManager.getMappedTypes().findByExtName(type.getId());

        Set<String> propertyDefinitions = new LinkedHashSet<String>(type.getPropertyDefinitions().keySet());

        for (Property<?> cmisProperty : cmisProperties) {
            // pop item prom list
            propertyDefinitions.remove(cmisProperty.getId());
            //
            PropertyDefinition<?> propertyDefinition = type.getPropertyDefinitions().get(cmisProperty.getId());
            String jcrPropertyName = typeMapping.toJcrProperty(cmisProperty.getId());

            // filtered = ignored
            if (jcrPropertyName == null) continue;
            // if unhandled continue = ignore
            if (!getShouldHandleCustomProperty(type, typeMapping, jcrPropertyName, propertyDefinition)) continue;

            // now handle -> it is our custom property or basic jcr one
            Object[] values = localTypeManager.getPropertyUtils().jcrValues(cmisProperty);
            writer.addProperty(jcrPropertyName, values);
        }

        // error protection
        if (addRequiredPropertiesOnRead) {
            for (String requiredExtProperty : propertyDefinitions) {
                PropertyDefinition<?> propertyDefinition = type.getPropertyDefinitions().get(requiredExtProperty);
                if (propertyDefinition.isRequired() && propertyDefinition.getUpdatability() == Updatability.READWRITE && !requiredExtProperty.startsWith(Constants.CMIS_PREFIX)) {
                    String pname = localTypeManager.getPropertyUtils().findJcrName(requiredExtProperty);
                    String propertyTargetName = typeMapping.toJcrProperty(pname);
                    writer.addProperty(propertyTargetName, CmisOperationCommons.getRequiredPropertyValue(propertyDefinition));
                }
            }
        }
    }

    /*
    * custom property filter criteria
    */
    private boolean getShouldHandleCustomProperty(TypeDefinition type, MappedCustomType typeMapping, String jcrPropertyName, PropertyDefinition<?> propertyDefinition) {
        // should be no additional properties for basic types
        boolean noCustomAllowed = type.getId().equals(BaseTypeId.CMIS_DOCUMENT.value());
        // we ignore RO properties as they are mostly system specific additionals
        // when handled -> increases mapping size
        boolean readOnly = propertyDefinition.getUpdatability() == Updatability.READONLY;
        boolean isCustom = !jcrPropertyName.startsWith("jcr:");

        boolean isUnhandledCustomProp = (isCustom) && (readOnly || noCustomAllowed);
        if (isUnhandledCustomProp) return false;

        // jcr analog not found but property is not our custom one
        boolean isUnknown = jcrPropertyName.startsWith(Constants.CMIS_PREFIX);
        if (isUnknown) return false;

        return true;
    }
}
