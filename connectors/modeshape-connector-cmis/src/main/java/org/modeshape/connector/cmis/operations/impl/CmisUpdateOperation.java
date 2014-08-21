package org.modeshape.connector.cmis.operations.impl;

import java.text.MessageFormat;
import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.FileableCmisObject;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.infinispan.schematic.document.Document;
import org.modeshape.connector.cmis.RuntimeSnapshot;
import org.modeshape.connector.cmis.config.CmisConnectorConfiguration;
import org.modeshape.connector.cmis.mapping.MappedCustomType;
import org.modeshape.connector.cmis.ObjectId;
import org.modeshape.connector.cmis.operations.BinaryContentProducerInterface;
import org.modeshape.jcr.federation.spi.DocumentChanges;
import org.modeshape.jcr.value.Name;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.chemistry.opencmis.commons.enums.VersioningState;

import static org.modeshape.connector.cmis.operations.impl.CmisOperationCommons.asDocument;
import static org.modeshape.connector.cmis.operations.impl.CmisOperationCommons.isVersioned;
import org.apache.chemistry.opencmis.commons.exceptions.CmisUpdateConflictException;
import org.slf4j.LoggerFactory;

public class CmisUpdateOperation extends CmisOperation {
    
    private final org.slf4j.Logger log = LoggerFactory.getLogger(this.getClass());
    public static final String OBJECT_DATA_SUFFIX = "_ObjectData";

    public CmisUpdateOperation(RuntimeSnapshot snapshot,
                               CmisConnectorConfiguration config) {
        super(snapshot, config);
    }

    private String mapProperty(MappedCustomType mapping, Name name) {
        // prefix
        debug("name.getNamespaceUri()", name.getNamespaceUri());
        String prefix = localTypeManager.getPrefixes().value(name.getNamespaceUri());
        debug("prefix", prefix);

        // prefixed name of the property in jcr domain is
        String jcrPropertyName = prefix != null ? prefix + ":" + name.getLocalName() : name.getLocalName();
        debug("jcrName", jcrPropertyName);

        // the name of this property in cmis domain is
        String cmisPropertyName = localTypeManager.getPropertyUtils().findCmisName(jcrPropertyName);
        debug("cmisName", cmisPropertyName);

        // correct. AAA!!!
        cmisPropertyName = mapping.toExtProperty(cmisPropertyName);
        debug("cmis replaced name", cmisPropertyName);

        return cmisPropertyName;
    }

    public void updateDocument(DocumentChanges delta, BinaryContentProducerInterface binaryProducer) {
        long startTime = System.currentTimeMillis();
        // object id is a composite key which holds information about
        // unique object identifier and about its type
        ObjectId objectId = ObjectId.valueOf(delta.getDocumentId());        
        debug("Start CmisUpdateOperation:updateDocument for objectId = ", objectId == null ? "null" : objectId.getIdentifier());   
        VersioningState versioningState = VersioningState.valueOf(config.getVersioningOnUpdateMetadata());
        boolean major = versioningState == VersioningState.MAJOR;
                    
        // this action depends from object type
        switch (objectId.getType()) {
            case REPOSITORY_INFO:
                // repository node is read only
                break;
            case CONTENT:
                // in the jcr domain content is represented by child node of
                // the nt:file node while in cmis domain it is a property of
                // the cmis:document object. so to perform this operation we need
                // to restore identifier of the original cmis:document. it is easy
                String cmisId = objectId.getIdentifier();
                
                // now let's get the reference to this object
                CmisObject cmisObject = finderUtil.find(cmisId);

                if (cmisObject == null) {
                    debug("Finish CmisUpdateOperation:updateDocument for objectId = ", objectId == null ? "null" : objectId.getIdentifier(), " CmisObject is null. Throw exception. Time: ", String.valueOf(System.currentTimeMillis()-startTime), " ms");
                    throw new CmisObjectNotFoundException("Cannot find CMIS object with id: " + cmisId);
                }

                // for this case we have the only property - jcr:data
                DocumentChanges.PropertyChanges changes = delta.getPropertyChanges();

                if (!changes.getRemoved().isEmpty()) {
                    if (isVersioned(cmisObject)) {
                        CmisOperationCommons.updateVersionedDoc(session, cmisObject, null, null, true);   // todo check if it removes
                    } else {
                        asDocument(cmisObject).deleteContentStream();
                    }
                } else {
                    String filename = cmisObject.getName();
                    ContentStream stream = binaryProducer.jcrBinaryContent(delta.getDocument(), filename);

                    if (stream != null) {
                        if (isVersioned(cmisObject)) {
                            if (isVersioned(cmisObject)) {
                                CmisOperationCommons.updateVersionedDoc(session, cmisObject, null, stream, true);
                            }
                        } else {
                            asDocument(cmisObject).setContentStream(stream, true);
                        }
                    }
                }
                invalidateCache(cmisObject, cmisId);
                break;
            case OBJECT:
                // modifying cmis:folders and cmis:documents
                cmisId = objectId.getIdentifier();
                cmisObject = finderUtil.find(cmisId);

                // checking that object exists
                if (cmisObject == null) {
                    debug("Finish CmisUpdateOperation:updateDocument for objectId = ", objectId == null ? "null" : objectId.getIdentifier(), " CmisObject is null. Throw exception. Time:", String.valueOf(System.currentTimeMillis()-startTime), "ms");
                    throw new CmisObjectNotFoundException("Cannot find CMIS object with id: " + objectId.getIdentifier());
                }

                changes = delta.getPropertyChanges();

                // process children changes
                Map<String, Name> map = new HashMap<String, Name>();
                map.putAll(delta.getChildrenChanges().getRenamed());
                map.putAll(delta.getChildrenChanges().getAppended());

                if (map.size() > 0) {
                    debug("Children changes: renamed and appended", Integer.toString(map.size()));

                    CmisObject child;
                    String before, after;
                    for (Map.Entry<String, Name> entry : map.entrySet()) {
                        child = finderUtil.find(entry.getKey());
                        if (child != null) {
                            before = child.getName();
                            after = entry.getValue().getLocalName();

                            if (after.equals(before)) {
                                continue;
                            }

                            debug("Child renamed", entry.getKey(), ":", before + "\t=>\t" + after);

                            // determine if in child's parent already exists a child with same name
                            if (isExistCmisObject(((FileableCmisObject) child).getParents().get(0).getPath() + "/" + after)) {
                                // already exists, so generates a temporary name
                                after += "-temp";
                            }

                            rename(child, after, versioningState, major);
                        }
                    }
                }
                Document props = delta.getDocument().getDocument("properties");

                // Prepare store for the cmis properties
                Map<String, Object> updateProperties = new HashMap<String, Object>();

                // ask cmis repository to get property definitions
                // we will use these definitions for correct conversation
                Map<String, PropertyDefinition<?>> propDefs = cmisObject.getBaseType().getPropertyDefinitions();
                propDefs.putAll(cmisObject.getType().getPropertyDefinitions());
                debug("cmisObject PropertyDefinitions", propDefs.toString());

                // group added and modified properties
                ArrayList<Name> modifications = new ArrayList<Name>();
                modifications.addAll(changes.getAdded());
                modifications.addAll(changes.getChanged());

                MappedCustomType mapping = localTypeManager.getMappedTypes().findByExtName(cmisObject.getType().getId());

                // convert names and values
                for (Name name : modifications) {
                    String cmisPropertyName = mapProperty(mapping, name);

                    // in cmis domain this property is defined as
                    PropertyDefinition<?> pdef = propDefs.get(cmisPropertyName);

                    // unknown property?
                    if (pdef == null) {
                        // ignore
                        continue;
                    }

                    // convert value and store
                    Document jcrValues = props.getDocument(name.getNamespaceUri());

                    Object value = localTypeManager.getPropertyUtils().cmisValue(pdef, name.getLocalName(), jcrValues);
                    // ! error protection
                    if (config.isIgnoreEmptyPropertiesOnCreate() && pdef.isRequired() && value == null) {
                        debug("WARNING: property [", cmisPropertyName, "] is required but EMPTY !!!!");
                        continue;
                    }

                    debug("adding prop ", cmisPropertyName, " value ", value.toString());
                    updateProperties.put(cmisPropertyName, value);
                }

                // step #2: nullify removed properties
                for (Name name : changes.getRemoved()) {
                    String cmisPropertyName = mapProperty(mapping, name);

                    // in cmis domain this property is defined as
                    PropertyDefinition<?> pdef = propDefs.get(cmisPropertyName);

                    // unknown property? - ignore
                    if (pdef == null) {                        
                        debug(cmisPropertyName, "unknown or readonly property - ignore ..");
                        continue;
                    }

                    updateProperties.put(cmisPropertyName, null);
                }

                // run update action
                debug("update properties?? ", updateProperties.isEmpty() ? "No" : "Yep");
                debug("cmisObject class: ", cmisObject.getClass().getCanonicalName(), " type ", cmisObject.getType().getDescription(), " name: ", cmisObject.getName());
                if (!updateProperties.isEmpty()) {
                    debug("Properties to update ", updateProperties.toString());
                    if ((cmisObject instanceof org.apache.chemistry.opencmis.client.api.Document) && isVersioned(cmisObject)) {
                        debug("cmisObject is versioned Document");
                        if (versioningState == VersioningState.NONE) {
                            cmisObject.updateProperties(updateProperties);
                        } else {
                            CmisOperationCommons.updateVersionedDoc(session, cmisObject, updateProperties, null, major);                            
                        }                        
                    } else if (cmisObject instanceof org.apache.chemistry.opencmis.client.api.Folder) {
                        debug("cmisObject is Folder");
                        try {
                            cmisObject.updateProperties(updateProperties);
                        } catch (CmisUpdateConflictException e) {
                            log().info(MessageFormat.format("Skip update properties [{0}] for folder [{1}] due to error {2}", updateProperties.toString(), cmisObject.getName(), e.getMessage()));
                        }
                    } else {
                        debug("cmisObject is not versioned");
                        try {
                            cmisObject.updateProperties(updateProperties);
                        } catch (CmisUpdateConflictException e) {
                            log().info(MessageFormat.format("{0} Try to update object as versioned", e.getMessage()));
                            CmisOperationCommons.updateVersionedDoc(session, cmisObject, updateProperties, null, major);                            
                        }
                    }
                }

                // MOVE:
                if (delta.getParentChanges().hasNewPrimaryParent()) {
                    move(cmisObject, delta);

                    // rename temporary name to a original
                    String name = cmisObject.getName();
                    if (name.endsWith("-temp")) {
                        rename(cmisObject, name.replace("-temp", ""), versioningState, major);
                    }
                }
                invalidateCache(cmisObject, cmisId);
                
                break;
            case UNFILED_STORAGE:
                // process unfiled changes
                Map<String, Name> renamed = new HashMap<String, Name>();
                renamed.putAll(delta.getChildrenChanges().getRenamed());

                if (renamed.size() > 0)
                {
                    debug("Unfiled changes: renamed ", Integer.toString(renamed.size()));

                    CmisObject child;
                    String before, after;
                    for (Map.Entry<String, Name> entry : renamed.entrySet())
                    {
                        child = finderUtil.find(entry.getKey());
                        before = child.getName();
                        after = entry.getValue().getLocalName();

                        if (after.equals(before)) continue;

                        debug("Unfiled renamed", entry.getKey(), ":", before + "\t=>\t" + after);

                        rename(child, after, versioningState, major);    
                                
                        invalidateCache(child, entry.getKey());
                    }
                }                  
        }
        debug("Finish CmisUpdateOperation:updateDocument for objectId = ", objectId == null ? "null" : objectId.getIdentifier(), ". Time:", String.valueOf(System.currentTimeMillis()-startTime), "ms");
    }

    /**
     * Utility method for checking if CMIS object exists at defined path
     *
     * @param path path for object
     * @return <code>true</code> if exists, <code>false</code> otherwise
     */
    private boolean isExistCmisObject(String path) {
        try {
            session.getObjectByPath(path);
            return true;
        } catch (CmisObjectNotFoundException e) {
            return false;
        }
    }

    /**
     * Utility method for renaming CMIS object
     *
     * @param object CMIS object to rename
     * @param name new name
     */
    private void rename(CmisObject object, String name, VersioningState versioningState, boolean major) {
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put(PropertyIds.NAME, name);

        List<CmisObject> parents = new ArrayList<CmisObject>();
        CmisObject parent = null;

        parents.addAll(((FileableCmisObject) object).getParents());
        if (!parents.isEmpty()) {
            parent = parents.get(0);
        }
        if ((parent instanceof org.apache.chemistry.opencmis.client.api.Document) && isVersioned(object)) {
            if (versioningState == VersioningState.NONE) {
                object.updateProperties(properties);
            } else {
                CmisOperationCommons.updateVersionedDoc(session, object, properties, null, major);
            }
        } else object.updateProperties(properties);   
    }

    /**
     * Utility method for moving CMIS object
     *
     * @param object CMIS object to move
     * @param delta changes for determining of destination CMIS object
     */
    private void move(CmisObject object, DocumentChanges delta) {
        FileableCmisObject target = (FileableCmisObject) object;
        CmisObject src = target.getParents().get(0);
        String dst = delta.getParentChanges().getNewPrimaryParent();

        if (src.getId().equals(dst)) {
            return;
        }

        target.move(src, finderUtil.find(dst));   
    }
    
    private void invalidateCache(CmisObject object, String cacheKey) {
        if (snapshot.getCache() != null) {
            snapshot.getCache().remove(cacheKey);
            snapshot.getCache().remove(object.getId());
            snapshot.getCache().remove(cacheKey+OBJECT_DATA_SUFFIX);
            snapshot.getCache().remove(object.getId()+OBJECT_DATA_SUFFIX);  
        }
        object.refresh();
    }
}
