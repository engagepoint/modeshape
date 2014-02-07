package org.modeshape.connector.cmis.operations.impl;

import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.FileableCmisObject;
import org.apache.chemistry.opencmis.client.api.Folder;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.apache.chemistry.opencmis.commons.spi.Holder;
import org.apache.chemistry.opencmis.commons.spi.ObjectService;
import org.infinispan.schematic.document.Document;
import org.modeshape.connector.cmis.operations.CmisObjectFinderUtil;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.modeshape.connector.cmis.mapping.MappedCustomType;
import org.modeshape.connector.cmis.ObjectId;
import org.modeshape.connector.cmis.operations.BinaryContentProducerInterface;
import org.modeshape.jcr.federation.spi.DocumentChanges;
import org.modeshape.jcr.value.Name;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.modeshape.connector.cmis.operations.impl.CmisOperationCommons.asDocument;
import static org.modeshape.connector.cmis.operations.impl.CmisOperationCommons.isVersioned;


public class CmisUpdateOperation extends CmisOperation {

    private boolean ignoreEmptyPropertiesOnCreate;

    public CmisUpdateOperation(Session session, LocalTypeManager localTypeManager, boolean ignoreEmptyPropertiesOnCreate,CmisObjectFinderUtil finderUtil) {
        super(session, localTypeManager,finderUtil);
        this.ignoreEmptyPropertiesOnCreate = ignoreEmptyPropertiesOnCreate;
    }

	private String mapProperty(MappedCustomType mapping, Name name)
	{
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
        // object id is a composite key which holds information about
        // unique object identifier and about its type
        ObjectId objectId = ObjectId.valueOf(delta.getDocumentId());

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
                    throw new CmisObjectNotFoundException("Cannot find CMIS object with id: " + cmisId);
                }

                // for this case we have the only property - jcr:data
                DocumentChanges.PropertyChanges changes = delta.getPropertyChanges();

                if (!changes.getRemoved().isEmpty()) {
                    if (isVersioned(cmisObject)) {
                        CmisOperationCommons.updateVersionedDoc(session, cmisObject, null, null);   // todo check if it removes
                    } else {
                        asDocument(cmisObject).deleteContentStream();
                    }
                } else {
                    ContentStream stream = binaryProducer.jcrBinaryContent(delta.getDocument());

                    if (stream != null) {
                        if (isVersioned(cmisObject)) {
                            if (isVersioned(cmisObject))
                                CmisOperationCommons.updateVersionedDoc(session, cmisObject, null, stream);
                        } else {
                            asDocument(cmisObject).setContentStream(stream, true);
                        }
                    }
                }
                break;
            case OBJECT:
                // modifying cmis:folders and cmis:documents
                cmisObject = finderUtil.find(objectId.getIdentifier());

				// checking that object exists
				if (cmisObject == null) throw new CmisObjectNotFoundException("Cannot find CMIS object with id: " + objectId.getIdentifier());

                changes = delta.getPropertyChanges();

				// process children changes
				Map<String, Name> map = new HashMap<String, Name>();
				map.putAll(delta.getChildrenChanges().getRenamed());
				map.putAll(delta.getChildrenChanges().getAppended());

				if (map.size() > 0)
				{
					debug("Children changes: renamed and appended", Integer.toString(map.size()));

					CmisObject child;
					String before, after;
					for (Map.Entry<String, Name> entry : map.entrySet())
					{
						child = finderUtil.find(entry.getKey());
						before = child.getName();
						after = entry.getValue().getLocalName();

						if (after.equals(before)) continue;

						debug("Child renamed", entry.getKey(), ":", before + "\t=>\t" + after);

						Map<String, Object> properties = new HashMap<String, Object>();
						properties.put("cmis:name", after);

						if ((cmisObject instanceof org.apache.chemistry.opencmis.client.api.Document) && isVersioned(cmisObject))
						{
							CmisOperationCommons.updateVersionedDoc(session, child, properties, null);
						}
						else child.updateProperties(properties);
					}
				}
                Document props = delta.getDocument().getDocument("properties");

                // Prepare store for the cmis properties
                Map<String, Object> updateProperties = new HashMap<String, Object>();

                // ask cmis repository to get property definitions
                // we will use these definitions for correct conversation
                Map<String, PropertyDefinition<?>> propDefs = cmisObject.getBaseType().getPropertyDefinitions();
                propDefs.putAll(cmisObject.getType().getPropertyDefinitions());

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
                    if (ignoreEmptyPropertiesOnCreate && pdef.isRequired() && value == null) {
                        debug("WARNING: property [", cmisPropertyName, "] is required but EMPTY !!!!");
                        continue;
                    }

                    debug("adding prop", cmisPropertyName, "value", value.toString());
                    updateProperties.put(cmisPropertyName, value);
                }

                // step #2: nullify removed properties
                for (Name name : changes.getRemoved()) {
					String cmisPropertyName = mapProperty(mapping, name);

					// in cmis domain this property is defined as
					PropertyDefinition<?> pdef = propDefs.get(cmisPropertyName);

					// unknown property? - ignore
					if (pdef == null) {
						debug(cmisPropertyName, "unknown property - ignore ..");
						continue;
					}

					updateProperties.put(cmisPropertyName, null);
                }

                // run update action
                debug("update properties?? ", updateProperties.isEmpty() ? "No" : "Yep");
                if (!updateProperties.isEmpty()) {
                    if ((cmisObject instanceof org.apache.chemistry.opencmis.client.api.Document) && isVersioned(cmisObject)) {
                        CmisOperationCommons.updateVersionedDoc(session, cmisObject, updateProperties, null);
                    } else {
                        cmisObject.updateProperties(updateProperties);
                    }
                }


				// MOVE:
				if (delta.getParentChanges().hasNewPrimaryParent()) move(cmisObject, delta);

                break;
        }
        debug("end of update story -----------------------------");

    }

	private void move(CmisObject object, DocumentChanges delta)
	{
		FileableCmisObject target = (FileableCmisObject) object;
		CmisObject src = target.getParents().get(0);
		String dst = delta.getParentChanges().getNewPrimaryParent();

		if (src.getId().equals(dst)) return;

		target.move(src, finderUtil.find(dst));
	}
}
