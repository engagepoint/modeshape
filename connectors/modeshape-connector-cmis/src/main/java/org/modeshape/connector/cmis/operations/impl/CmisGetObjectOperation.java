package org.modeshape.connector.cmis.operations.impl;


import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.chemistry.opencmis.commons.definitions.TypeDefinition;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.chemistry.opencmis.commons.enums.Updatability;
import org.infinispan.schematic.document.Document;
import org.modeshape.connector.cmis.RuntimeSnapshot;
import org.modeshape.connector.cmis.config.CmisConnectorConfiguration;
import org.modeshape.connector.cmis.Constants;
import org.modeshape.connector.cmis.features.CmisBinaryValue;
import org.modeshape.connector.cmis.mapping.MappedCustomType;
import org.modeshape.connector.cmis.ObjectId;
import org.modeshape.jcr.JcrLexicon;
import org.modeshape.jcr.api.JcrConstants;
import org.modeshape.jcr.federation.spi.DocumentWriter;
import org.modeshape.jcr.federation.spi.PageKey;
import org.modeshape.jcr.federation.spi.PageWriter;

import javax.jcr.nodetype.NodeType;
import java.util.*;
import org.apache.chemistry.opencmis.client.runtime.OperationContextImpl;
import org.modeshape.jcr.ModeShapeLexicon;

public class CmisGetObjectOperation extends CmisOperation {
    
    public static final String MIX_CONTENT_STREAM = "{http://www.jcp.org/jcr/mix/1.0}contentStream";
    
    String projectedNodeId;

    public CmisGetObjectOperation(RuntimeSnapshot snapshot,
                                  CmisConnectorConfiguration config,
                                  String projectedNodeId) {
        super(snapshot, config);
        this.projectedNodeId = projectedNodeId;
    }


    /**
     * Translates CMIS folder object to JCR node
     *
     * @param cmisObject CMIS folder object
     * @return JCR node document.
     */
    public DocumentWriter cmisFolder(CmisObject cmisObject) {
        long startTime = System.currentTimeMillis();
        debug("Start CmisGetObjectOperation:cmisFolder for cmisObject = ", cmisObject == null ? "null" : cmisObject.getName());
        CmisGetChildrenOperation childrenOperation = new CmisGetChildrenOperation(snapshot, config);

        Folder folder = (Folder) cmisObject;
        DocumentWriter writer = snapshot.getDocumentProducer().getNewDocument(ObjectId.toString(ObjectId.Type.OBJECT, folder.getId()));
        // set correct type
        writer.setPrimaryType(localTypeManager.cmisTypeToJcr(cmisObject.getType().getId()).getJcrName());
        // parent
        writer.setParent(folder.getParentId());
        // properties
        cmisProperties(folder, writer);
        // children
        childrenOperation.cmisChildren(folder, writer);

        // append repository information to the root node
        if (folder.isRootFolder() && !config.isHideRootFolderReference()) {
            writer.addChild(ObjectId.toString(ObjectId.Type.REPOSITORY_INFO, ""), Constants.REPOSITORY_INFO_NODE_NAME);
        }
        if (cmisObject.getId().equals(projectedNodeId) || cmisObject.getId().equals(projectedNodeId)) {
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
                        
        writer.addMixinType(ModeShapeLexicon.FEDERATED);
        
        debug("Finish CmisGetObjectOperation:cmisFolder for cmisObject = ", cmisObject.getName(), ". Time:", Long.toString(System.currentTimeMillis()-startTime), "ms");
        return writer;
    }


    /**
     * Translates cmis document object to JCR node.
     *
     * @param cmisObject cmis document node
     * @param incomingId jcr key by which document is referenced. it is preferable to set it as document id
     * @return JCR node document.
     */
    public Document cmisDocument(CmisObject cmisObject, String incomingId) {
        long startTime = System.currentTimeMillis();
        debug("Start CmisGetObjectOperation:cmisDocument for cmisObject = ", cmisObject == null ? "null" : cmisObject.getName(), " and incomingId = ", getPossibleNullString(incomingId));
        org.apache.chemistry.opencmis.client.api.Document doc = CmisOperationCommons.asDocument(cmisObject);

        // document and internalId
        DocumentWriter writer = snapshot.getDocumentProducer().getNewDocument(ObjectId.toString(ObjectId.Type.OBJECT, incomingId));

        // set correct type
        writer.setPrimaryType(localTypeManager.cmisTypeToJcr(cmisObject.getType().getId()).getJcrName());

        // parents
        OperationContext context = new OperationContextImpl();
        context.setCacheEnabled(true);        
        List<Folder> parents = doc.getParents(context);
        ArrayList<String> parentIds = new ArrayList<String>();
        for (Folder f : parents) {
            parentIds.add(ObjectId.toString(ObjectId.Type.OBJECT, f.getId()));
        }
        // no parents = unfiled
        if (parentIds.isEmpty()) parentIds.add(ObjectId.toString(ObjectId.Type.UNFILED_STORAGE, ""));

        // set parents
        writer.setParents(parentIds);

        // content node - mandatory child for a document
        writer.addChild(ObjectId.toString(ObjectId.Type.CONTENT, incomingId), JcrConstants.JCR_CONTENT);

        // basic properties
        cmisProperties(doc, writer);

        writer.addMixinType(NodeType.MIX_REFERENCEABLE);
        writer.addProperty(JcrLexicon.UUID, incomingId);

        writer.addMixinType(NodeType.MIX_LAST_MODIFIED);
        Property<Object> lastModified = doc.getProperty(PropertyIds.LAST_MODIFICATION_DATE);
        Property<Object> lastModifiedBy = doc.getProperty(PropertyIds.LAST_MODIFIED_BY);
        writer.addProperty(JcrLexicon.LAST_MODIFIED, localTypeManager.getPropertyUtils().jcrValues(lastModified));
        writer.addProperty(JcrLexicon.LAST_MODIFIED_BY, localTypeManager.getPropertyUtils().jcrValues(lastModifiedBy));
        
        writer.addMixinType(NodeType.MIX_MIMETYPE);
        Property<Object> mimeType = doc.getProperty(PropertyIds.CONTENT_STREAM_MIME_TYPE);
        writer.addProperty(JcrLexicon.MIMETYPE, localTypeManager.getPropertyUtils().jcrValues(mimeType));

        writer.addMixinType(MIX_CONTENT_STREAM);
        Property<Object> contentStreamLength = doc.getProperty(PropertyIds.CONTENT_STREAM_LENGTH);
        Property<Object> contentStreamFileName = doc.getProperty(PropertyIds.CONTENT_STREAM_FILE_NAME);
        writer.addProperty(JcrLexicon.CONTENT_STREAM_LENGTH, localTypeManager.getPropertyUtils().jcrValues(contentStreamLength));
        writer.addProperty(JcrLexicon.CONTENT_STREAM_FILE_NAME, localTypeManager.getPropertyUtils().jcrValues(contentStreamFileName));
        
        writer.addMixinType(ModeShapeLexicon.FEDERATED);  
        
        writer.addMixinType(ModeShapeLexicon.NODE_INFO_MIXIN);
        Property<Object> nodeName = doc.getProperty(PropertyIds.NAME);
        writer.addProperty(ModeShapeLexicon.NODE_NAME, localTypeManager.getPropertyUtils().jcrValues(nodeName));
        
        debug("Finish CmisGetObjectOperation:cmisDocument for cmisObject = ", cmisObject.getName(), " and incomingId = ", incomingId, ". Time:", Long.toString(System.currentTimeMillis()-startTime), "ms");
        return writer.document();
    }

    /**
     * Converts binary content into JCR node.
     *
     * @param id the id of the CMIS document.
     * @return JCR node representation.
     */
    public Document cmisContent(String id) {
        long startTime = System.currentTimeMillis();
        debug("Start CmisGetObjectOperation:cmisContent for cmisObject with Id = ", getPossibleNullString(id));
        String contentId = ObjectId.toString(ObjectId.Type.CONTENT, id);
        DocumentWriter writer = snapshot.getDocumentProducer().getNewDocument(contentId);

        org.apache.chemistry.opencmis.client.api.Document doc = CmisOperationCommons.asDocument(finderUtil.find(id));
        writer.setPrimaryType(NodeType.NT_RESOURCE);
        writer.setParent(id);

        //ContentStream stream = doc.getContentStream();
        //if (stream != null) {
            //InputStream is = stream.getStream();
            CmisBinaryValue cmisBinaryValue = new CmisBinaryValue(id, null, config.getSourceName(), config.getMimeTypeDetector());

            //BinaryValue content = localTypeManager.getFactories().getBinaryFactory().create(is);
            writer.addProperty(JcrConstants.JCR_DATA, cmisBinaryValue);


        writer.addMixinType(NodeType.MIX_MIMETYPE);
        Property<Object> mimeType = doc.getProperty(PropertyIds.CONTENT_STREAM_MIME_TYPE);
        writer.addProperty(JcrLexicon.MIMETYPE, localTypeManager.getPropertyUtils().jcrValues(mimeType));

        writer.addMixinType(MIX_CONTENT_STREAM);
        Property<Object> contentStreamLength = doc.getProperty(PropertyIds.CONTENT_STREAM_LENGTH);
        Property<Object> contentStreamFileName = doc.getProperty(PropertyIds.CONTENT_STREAM_FILE_NAME);
        writer.addProperty(JcrLexicon.CONTENT_STREAM_LENGTH, localTypeManager.getPropertyUtils().jcrValues(contentStreamLength));
        writer.addProperty(JcrLexicon.CONTENT_STREAM_FILE_NAME, localTypeManager.getPropertyUtils().jcrValues(contentStreamFileName));

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
        
        writer.addMixinType(ModeShapeLexicon.FEDERATED);
        
        writer.addMixinType(ModeShapeLexicon.NODE_INFO_MIXIN);
        Property<Object> nodeName = doc.getProperty(PropertyIds.NAME);
        writer.addProperty(ModeShapeLexicon.NODE_NAME, localTypeManager.getPropertyUtils().jcrValues(nodeName));

        debug("Finish CmisGetObjectOperation:cmisContent for cmisObject with Id = ", id, ". Time:", Long.toString(System.currentTimeMillis() - startTime), "ms");
        return writer.document();
    }

    /**
     * Translates CMIS repository information into Node.
     *
     * @return node document.
     */
    public Document jcrUnfiled(String originalId, String caughtProjectedId) {
        long startTime = System.currentTimeMillis();
        debug("Start CmisGetObjectOperation:jcrUnfiled for originalId = ", getPossibleNullString(originalId), " and caughtProjectedId = ", getPossibleNullString(caughtProjectedId));
       
        DocumentWriter writer = snapshot.getDocumentProducer().getNewDocument(ObjectId.toString(ObjectId.Type.OBJECT, ObjectId.Type.UNFILED_STORAGE.getValue()));
        Folder root = session.getRootFolder();

        writer.setPrimaryType(NodeType.NT_FOLDER);
        if (caughtProjectedId == null) {
            // replace with logger lately
            log().info("Caught ROOT node as NULL when filling Unfiled node!!..!!");
        }
        writer.setParent(caughtProjectedId);
//        writer.setParent("[root]");

        writer.addMixinType(NodeType.MIX_REFERENCEABLE);
        writer.addProperty(JcrLexicon.UUID, ObjectId.Type.UNFILED_STORAGE.getValue());

        writer.addMixinType(NodeType.MIX_LAST_MODIFIED);
        Property<Object> lastModified = root.getProperty(PropertyIds.LAST_MODIFICATION_DATE);
        Property<Object> lastModifiedBy = root.getProperty(PropertyIds.LAST_MODIFIED_BY);
        writer.addProperty(JcrLexicon.LAST_MODIFIED, localTypeManager.getPropertyUtils().jcrValues(lastModified));
        writer.addProperty(JcrLexicon.LAST_MODIFIED_BY, localTypeManager.getPropertyUtils().jcrValues(lastModifiedBy));       
                
        writer.addMixinType(ModeShapeLexicon.FEDERATED);

        if (originalId.contains("#")) {
            CmisGetChildrenOperation childrenOperation =
                    new CmisGetChildrenOperation(snapshot, config);
            PageKey pageKey = new PageKey(originalId);
            if (pageKey.getBlockSize() == 0) {
                childrenOperation.getChildren(new PageKey(pageKey.getParentId(), pageKey.getOffsetString(), config.getPageSizeUnfiled()), writer, 0);
            } else {
                childrenOperation.getChildren(pageKey, writer);
            }

        } else {
            writer.addPage(ObjectId.toString(ObjectId.Type.UNFILED_STORAGE, ""), 0, 0, PageWriter.UNKNOWN_TOTAL_SIZE);
        }

        debug("Finish CmisGetObjectOperation:jcrUnfiled for originalId = ", originalId, " and caughtProjectedId = ", caughtProjectedId, ". Time:", Long.toString(System.currentTimeMillis()-startTime), "ms");        
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

        Map<String, Object[]> propMap = processProperties(cmisProperties, type, typeMapping, propertyDefinitions);
        for (Map.Entry<String, Object[]> entry : propMap.entrySet()) {
            writer.addProperty(entry.getKey(), entry.getValue());
        }


        // error protection
        if (config.isAddRequiredPropertiesOnRead()) {
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

    public Map<String, Object[]> processProperties(List<Property<?>> cmisProperties, TypeDefinition type, MappedCustomType typeMapping, Set<String> propertyDefinitions) {
        long startTime = System.currentTimeMillis();
        debug("Start CmisGetObjectOperation:processProperties");
       
        Map<String, Object[]> result = new LinkedHashMap<String, Object[]>(cmisProperties.size());
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
            result.put(jcrPropertyName, values);
        }
        debug("Finish CmisGetObjectOperation:processProperties. Time:", Long.toString(System.currentTimeMillis() - startTime), "ms");
        return result;
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
