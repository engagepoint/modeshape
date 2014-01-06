/*
 * ModeShape (http://www.modeshape.org)
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * See the AUTHORS.txt file in the distribution for a full listing of
 * individual contributors.
 *
 * ModeShape is free software. Unless otherwise indicated, all code in ModeShape
 * is licensed to you under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * ModeShape is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.modeshape.connector.cmis;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinitionTemplate;

import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.Folder;
import org.apache.chemistry.opencmis.client.api.ItemIterable;
import org.apache.chemistry.opencmis.client.api.ObjectType;
import org.apache.chemistry.opencmis.client.api.Property;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.client.api.Tree;
import org.apache.chemistry.opencmis.client.bindings.spi.StandardAuthenticationProvider;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.data.RepositoryInfo;
import org.apache.chemistry.opencmis.commons.definitions.Choice;
import org.apache.chemistry.opencmis.commons.definitions.DocumentTypeDefinition;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.chemistry.opencmis.commons.definitions.TypeDefinition;
import org.apache.chemistry.opencmis.commons.enums.*;
import org.apache.chemistry.opencmis.commons.exceptions.CmisBaseException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisInvalidArgumentException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.ContentStreamImpl;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.infinispan.schematic.document.Binary;
import org.infinispan.schematic.document.Document;
import org.infinispan.schematic.document.Document.Field;
import org.modeshape.connector.cmis.config.TypeCustomMappingList;
import org.modeshape.jcr.federation.spi.*;
import org.modeshape.jcr.federation.spi.UnfiledSupportConnector;
import org.modeshape.connector.cmis.util.CryptoUtils;
import org.modeshape.connector.cmis.util.TypeMappingConfigUtil;
import org.modeshape.jcr.JcrLexicon;
import org.modeshape.jcr.api.JcrConstants;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.federation.spi.Connector;
import org.modeshape.jcr.federation.spi.DocumentChanges;
import org.modeshape.jcr.federation.spi.DocumentChanges.ChildrenChanges;
import org.modeshape.jcr.federation.spi.DocumentChanges.PropertyChanges;
import org.modeshape.jcr.value.BinaryValue;
import org.modeshape.jcr.value.Name;
import org.modeshape.jcr.value.ValueFactories;
import org.w3c.dom.Element;

import java.security.GeneralSecurityException;
import java.util.*;

/**
 * This connector exposes the content of a CMIS repository.
 * <p>
 * The Content Management Interoperability Services (CMIS) standard deﬁnes a domain model and Web Services that can be used by
 * applications to work with one or more Content Management repositories/systems.
 * </p>
 * <p>
 * The CMIS connector is designed to be layered on top of existing Content Management systems. It is intended to use Apache
 * Chemistry API to access services provided by Content Management system and incorporate those services into Modeshape content
 * repository.
 * </p>
 * <p>
 * There are several attributes that should be configured on each external source:
 * <ul>
 * <li><strong><code></code></strong></li>
 * <li><strong><code>aclService</code></strong> URL of the Access list service binding entry point. The ACL Services are used to
 * discover and manage Access Control Lists.</li>
 * <li><strong><code>discoveryService</code></strong> URL of the Discovery service binding entry point. Discovery service executes
 * a CMIS query statement against the contents of the repository.</li>
 * <li><strong><code>multifilingService</code></strong> URL of the Multi-filing service binding entry point. The Multi-ﬁling
 * Services are used to ﬁle/un-ﬁle objects into/from folders.</li>
 * <li><strong><code>navigationService</code></strong> URL of the Navigation service binding entry point. The Navigation service
 * gets the list of child objects contained in the speciﬁed folder.</li>
 * <li><strong><code>objectService</code></strong> URL of the Object service binding entry point. Creates a document object of the
 * speciﬁed type (given by the cmis:objectTypeId property) in the (optionally) speciﬁed location</li>
 * <li><strong><code>policyService</code></strong> URL of the Policy service binding entry point. Applies a speciﬁed policy to an
 * object.</li>
 * <li><strong><code>relationshipService</code></strong> URL of the Relationship service binding entry point. Gets all or a subset
 * of relationships associated with an independent object.</li>
 * <li><strong><code>repositoryService</code></strong> URL of the Repository service binding entry point. Returns a list of CMIS
 * repositories available from this CMIS service endpoint.</li>
 * <li><strong><code>versioningService</code></strong> URL of the Policy service binding entry point. Create a private working
 * copy (PWC) of the document.</li>
 * </ul>
 * </p>
 * <p>
 * The connector results in the following form
 * </p>
 * <table cellspacing="0" cellpadding="1" border="1">
 * <tr>
 * <th>Path</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td><code>/repository_info</code></td>
 * <td>Repository description</td>
 * </tr>
 * <tr>
 * <td><code>/filesAndFolder</code></td>
 * <td>The structure of the folders and files in the projected repository</td>
 * </tr>
 * <table>
 *
 * @author Oleg Kulikov
 * @author Ivan Vasyliev
 * @author Nick Knysh
 */
public class CmisConnector extends Connector implements UnfiledSupportConnector {

    // path and id for the repository node
    private static final String REPOSITORY_INFO_ID = "repositoryInfo";
    private static final String REPOSITORY_INFO_NODE_NAME = "repositoryInfo";
    private static final String CMIS_DOCUMENT_UNVERSIONED = "cmis:unversioned-document";
    private static final String FEATURE_NAME_SNS = "SNS";
    public static final String CMIS_PREFIX = "cmis:";
    private static final String JCR_DATA = "jcr:data";
    private Session session;
    private ValueFactories factories;
    // binding parameters
    private String aclService;
    private String discoveryService;
    private String multifilingService;
    private String navigationService;
    private String objectService;
    private String policyService;
    private String relationshipService;
    private String repositoryService;
    private String versioningService;
    // root folder reference flag
    private boolean hideRootFolderReference;
    // credentials
    private String user;
    private String password;
    // repository id
    private String repositoryId;
    private Properties properties;
    private Nodes nodes;
    // configuration
    private TypeCustomMappingList customMapping = new TypeCustomMappingList();
    private boolean debug = false;
    private boolean ignoreEmptyPropertiesOnCreate = false; // to not reset required properties on document create
    private boolean addRequiredPropertiesOnRead = false; // add required properties to a document if not present
    private String clientPortProvider; // client port container
    private String[] typesMapping;
    private String[] applicableUnfiledTypes;
    // type mapping
    private MappedTypesContainer mappedTypes;
    private Collection<Name> applicableTypesInstance;

    //
    private Prefix prefixes = new Prefix();
    private Map<String, ObjectType> cachedTypeDefinitions = new HashMap<String, ObjectType>();

    public String[] getTypesMapping() {
        return typesMapping;
    }

    @Override
    public Collection<Name> getApplicableUnfiledTypes() {
        return applicableTypesInstance;
    }

    public CmisConnector() {
        super();
    }

    // DEBUG. this method will be removed todo
    public void debug(String... values) {
        if (debug) {
            for (String value : values) {
                System.out.print(value + " ");
            }
            System.out.println();
        }
    }

    @Override
    public void initialize(NamespaceRegistry registry,
                           NodeTypeManager nodeTypeManager) throws RepositoryException, IOException {
        super.initialize(registry, nodeTypeManager);
        // read mapping
        this.mappedTypes = TypeMappingConfigUtil.getMappedTypes(customMapping);
        if (customMapping != null && customMapping.getNamespaces() != null) {
            debug("Found ", Integer.toString(customMapping.getNamespaces().size()), " mapped namspaces ..");
        }
        debug("Added ", Integer.toString(mappedTypes.size()), " mapped types ..");
        if (debug) logMappedTypes();


        this.factories = getContext().getValueFactories();

        properties = new Properties(getContext().getValueFactories());
        nodes = new Nodes();

        // default factory implementation
        Map<String, String> parameter = new HashMap<String, String>();

        // user credentials
        if (StringUtils.isNotEmpty(user) && StringUtils.isNotEmpty(password)) {
            parameter.put(SessionParameter.USER, user);
            parameter.put(SessionParameter.PASSWORD, getPassword());
        }

        // connection settings
        parameter.put(SessionParameter.BINDING_TYPE, BindingType.WEBSERVICES.value());
        parameter.put(SessionParameter.WEBSERVICES_ACL_SERVICE, aclService);
        parameter.put(SessionParameter.WEBSERVICES_DISCOVERY_SERVICE, discoveryService);
        parameter.put(SessionParameter.WEBSERVICES_MULTIFILING_SERVICE, multifilingService);
        parameter.put(SessionParameter.WEBSERVICES_NAVIGATION_SERVICE, navigationService);
        parameter.put(SessionParameter.WEBSERVICES_OBJECT_SERVICE, objectService);
        parameter.put(SessionParameter.WEBSERVICES_POLICY_SERVICE, policyService);
        parameter.put(SessionParameter.WEBSERVICES_RELATIONSHIP_SERVICE, relationshipService);
        parameter.put(SessionParameter.WEBSERVICES_REPOSITORY_SERVICE, repositoryService);
        parameter.put(SessionParameter.WEBSERVICES_VERSIONING_SERVICE, versioningService);
        parameter.put(SessionParameter.REPOSITORY_ID, repositoryId);
        if (StringUtils.isNotEmpty(clientPortProvider))
            parameter.put(SessionParameter.WEBSERVICES_JAXWS_IMPL, clientPortProvider);

        SessionFactoryImpl factory = SessionFactoryImpl.newInstance();
        session = factory.createSession(parameter, null, new StandardAuthenticationProvider() {
            private static final long serialVersionUID = 1L;

            @Override
            public Element getSOAPHeaders(Object portObject) {
                // Place headers here
                return super.getSOAPHeaders(portObject);
            }
        }, null);

        registerPredefinedNamspaces(registry);
        List<NodeTypeTemplate> definitionsList = new ArrayList<NodeTypeTemplate>();
        importTypes(session.getTypeDescendants(null, Integer.MAX_VALUE, true), nodeTypeManager, registry, definitionsList);
        updateTypes(nodeTypeManager, definitionsList);
        // after importing and updating - we need to register/update all types
        NodeTypeDefinition[] nodeDefs = new NodeTypeDefinition[definitionsList.size()];
        definitionsList.toArray(nodeDefs);
        nodeTypeManager.registerNodeTypes(nodeDefs, true);
        // todo: reimport, use types from manager
        registerRepositoryInfoType(nodeTypeManager);

        // unfiled support
        if (applicableUnfiledTypes != null && applicableUnfiledTypes.length > 0) {
            applicableTypesInstance = new HashSet<Name>(applicableUnfiledTypes.length);
            for (String unfiledType : applicableUnfiledTypes) {
                Name name = factories().getNameFactory().create(unfiledType);
                applicableTypesInstance.add(name);
            }
            applicableTypesInstance = Collections.unmodifiableCollection(applicableTypesInstance);
        } else {
            applicableTypesInstance = Collections.EMPTY_SET;
        }
    }

    private void logMappedTypes() {
        for (MappedCustomType mapping : getMappedTypes().mappings) {
            debug("Mapping: jcr/cmisExt <" + mapping.getJcrName() + "> = <" + mapping.getExtName() + ">");
            debug("Mapped Properties..");
            for (Map.Entry<String, String> entry : mapping.indexJcrProperties.entrySet()) {
                debug("mapped property jct/cmisExt <" + entry.getKey() + "> = <" + entry.getValue() + ">");
            }
            StringBuilder ignoredPropsString = new StringBuilder();
            if (mapping.getIgnoreExternalProperties() != null)
                for (String s : mapping.getIgnoreExternalProperties()) {
                    ignoredPropsString.append(s).append(";");
                }
            debug("Ignored external Properties: " + ignoredPropsString.toString());
            debug("end mapping info -----");
        }
    }

    public MappedTypesContainer getMappedTypes() {
        return mappedTypes;
    }


    public Session getSession() {
        return session;
    }

    /*
     * register default CMIS namespace
     * and namespaces from mapping section of connector's configuration
     *
     * do it before node types registration because:
     * - ns uri is not necessary per type in this case
     * - problems may appear when registering namespace together with nodetype (but probably may be fixed)
    */
    public void registerPredefinedNamspaces(NamespaceRegistry registry) throws RepositoryException {
        // modeshape cmis
        registry.registerNamespace(CmisLexicon.Namespace.PREFIX, CmisLexicon.Namespace.URI);

        if (customMapping == null || customMapping.getNamespaces() == null) return;

        // custom
        for (Map.Entry<String, String> entry : customMapping.getNamespaces().entrySet()) {
            String nsPrefix = entry.getKey();
            String nsUri = entry.getValue();
            if (!isNsAlreadyRegistered(null, registry, nsPrefix, nsUri)) {
                registry.registerNamespace(nsPrefix, nsUri);
            }
            prefixes.addNamespace(nsPrefix, nsUri);
        }
    }

    /*
    * get decrypted password
    */
    private String getPassword() throws IOException, RepositoryException {
        String decrypt;
        try {
            decrypt = CryptoUtils.decrypt(password, CryptoUtils.KEY_AS_HEX);
        } catch (GeneralSecurityException e) {
            throw new RepositoryException("Can't decrypt password");
        }
        return decrypt;
    }

    @Override
    public Document getDocumentById(String id) {
        // object id is a composite key which holds information about
        // unique object identifier and about its type
        ObjectId objectId = ObjectId.valueOf(id);

        // this action depends from object type
        switch (objectId.getType()) {
            case REPOSITORY_INFO:
                // convert information about repository from
                // cmis domain into jcr domain
                return cmisRepository();
            case CONTENT:
                // in the jcr domain content is represented by child node of
                // the nt:file node while in cmis domain it is a property of
                // the cmis:document object. This action searches original
                // cmis:document and converts its content property into jcr node
                return cmisContent(objectId.getIdentifier());
            case OBJECT:
                // converts cmis folders and documents into jcr folders and files
                return cmisObject(objectId.getIdentifier());
            default:
                return null;
        }
    }

    @Override
    public String getDocumentId(String path) {
        // establish relation between path and object identifier
        return session.getObjectByPath(path).getId();
    }

    @Override
    public Collection<String> getDocumentPathsById(String id) {
        System.out.println("------------- Get document by Id");
        CmisObject obj = session.getObject(id);
        // check that object exist
        if (obj instanceof Folder) {
            return Collections.singletonList(((Folder) obj).getPath());
        }
        if (obj instanceof org.apache.chemistry.opencmis.client.api.Document) {
            org.apache.chemistry.opencmis.client.api.Document doc = (org.apache.chemistry.opencmis.client.api.Document) obj;
            List<Folder> parents = doc.getParents();
            List<String> paths = new ArrayList<String>(parents.size());
            for (Folder parent : doc.getParents()) {
                paths.add(parent.getPath() + "/" + doc.getName());
            }
            return paths;
        }
        return Collections.emptyList();
    }

    @Override
    public boolean removeDocument(String id) {
        // object id is a composite key which holds information about
        // unique object identifier and about its type
        ObjectId objectId = ObjectId.valueOf(id);

        // this action depends from object type
        switch (objectId.getType()) {
            case REPOSITORY_INFO:
                // information about repository is ready only
                return false;
            case CONTENT:
                // in the jcr domain content is represented by child node of
                // the nt:file node while in cmis domain it is a property of
                // the cmis:document object. so to perform this operation we need
                // to restore identifier of the original cmis:document. it is easy
                String cmisId = objectId.getIdentifier();

                org.apache.chemistry.opencmis.client.api.Document doc = null;
                try {
                    doc = asDocument(session.getObject(cmisId));
                } catch (org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException nfe) {
                    return true;
                }

                // object exists?
                if (doc == null) {
                    // object does not exist. probably was deleted by from cmis domain
                    // we don't know how to handle such case yet, thus TODO
                    return false;
                }

                // delete content stream
                if (isVersioned(doc)) {
                    // ignore deletion of content stream. lets rely on delete document then
                    // may be called before document deletion. in this case may not be necessary
//                    deleteStreamVersioned(doc);
                    debug("ignore content stream deletion for versioned document.");
                } else {
                    try {
                        doc = asDocument(session.getObject(cmisId));
                        if (doc != null)
                            doc.deleteContentStream();
                    } catch (org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException nfe) {
                        return true;
                    }

                }

                return true;
            case OBJECT:
                // these type points to either cmis:document or cmis:folder so
                // we can just delete it using original identifier defined in cmis domain.
                CmisObject object = null;
                try {
                    object = session.getObject(objectId.getIdentifier());
                } catch (org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException nfe) {
                    return true;
                }

                // check that object exist
                if (object == null) {
                    return true;
                }
                if (object instanceof Folder) {
                    debug("deleting folder ", object.getId());
//                    ((Folder) object).deleteTree(true, UnfileObject.UNFILE, false);
                    try {
                        object = session.getObject(objectId.getIdentifier());
                        if (object != null)
                            ((Folder) object).deleteTree(true, UnfileObject.DELETE, false);      // todo check for unfiled support + unfile
                    } catch (org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException nfe) {
                        return true;
                    }

//                    folder.delete(true);
                } else {
                    debug("deleting document ", object.getId());
                    try {
                        object = session.getObject(objectId.getIdentifier());
                        if (object != null)
                            object.delete(true);
                    } catch (org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException nfe) {
                        return true;
                    }

                }

                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean hasDocument(String id) {
        // object id is a composite key which holds information about
        // unique object identifier and about its type
        ObjectId objectId = ObjectId.valueOf(id);

        // this action depends from object type
        switch (objectId.getType()) {
            case REPOSITORY_INFO:
                // exist always
                return true;
            case CONTENT:
                // in the jcr domain content is represented by child node of
                // the nt:file node while in cmis domain it is a property of
                // the cmis:document object. so to perform this operation we need
                // to restore identifier of the original cmis:document. it is easy
                String cmisId = objectId.getIdentifier();

                // now checking that this document exists
                return session.getObject(cmisId) != null;
            default:
                // here we checking cmis:folder and cmis:document
                return session.getObject(id) != null;
        }
    }

    @Override
    public void storeDocument(Document document) {
        // object id is a composite key which holds information about
        // unique object identifier and about its type
        ObjectId objectId = ObjectId.valueOf(document.getString("key"));

        // this action depends from object type
        debug("=== STORE document called ====", document.toString());
        switch (objectId.getType()) {
            case REPOSITORY_INFO:
                // repository information is ready only
                return;
            case CONTENT:
                // in the jcr domain content is represented by child node of
                // the nt:file node while in cmis domain it is a property of
                // the cmis:document object. so to perform this operation we need
                // to restore identifier of the original cmis:document. it is easy
                String cmisId = objectId.getIdentifier();

                // now let's get the reference to this object
                CmisObject cmisObject = session.getObject(cmisId);

                // object exists?
                if (cmisObject == null) {
                    // object does not exist. propably was deleted by from cmis domain
                    // we don't know how to handle such case yet, thus TODO
                    return;
                }

                // original object is here so converting binary value and
                // updating original cmis:document
                ContentStream stream = jcrBinaryContent(document);
                if (stream != null) {
                    if (isVersioned(cmisObject)) updateVersionedDoc(cmisObject, null, stream);
                    else asDocument(cmisObject).setContentStream(stream, true, true);
                }
                break;
            case OBJECT:
                // extract node properties from the document view
                Document jcrProperties = document.getDocument("properties");

                // check that we have jcr properties to store in the cmis repo
                if (jcrProperties == null) {
                    // nothing to store
                    return;
                }

                // if node has properties we need to pickup cmis object from
                // cmis repository, convert properties from jcr domain to cmis
                // and update properties
                cmisObject = null;
                try {
                    cmisObject = session.getObject(objectId.getIdentifier());
                } catch (Exception e) {
                    debug("Not found object with id - " + objectId.getIdentifier());
                }

                // unknown object?
                if (cmisObject == null) {
                    // exit silently
                    return;
                }

                // Prepare store for the cmis properties
                Map<String, Object> updateProperties = new HashMap<String, Object>();

                // ask cmis repository to get property definitions
                // we will use these definitions for correct conversation
                Map<String, PropertyDefinition<?>> propDefs = cmisObject.getBaseType().getPropertyDefinitions();
                propDefs.putAll(cmisObject.getType().getPropertyDefinitions());
                MappedCustomType mapping = mappedTypes.findByExtName(cmisObject.getType().getId());
                // jcr properties are grouped by namespace uri
                // we will travers over all namespaces (ie group of properties)
                debug();
                debug("Store properties for", objectId.getIdentifier(), cmisObject.getType().getId(), "with mapping to :", mapping != null ? mapping.getJcrName() : "<none>");

                for (Field field : jcrProperties.fields()) {
                    // field has namespace uri as field's name and properties
                    // as value
                    // getting namespace uri and properties
                    String namespaceUri = field.getName();
                    debug("property nsUri", namespaceUri);
                    Document props = field.getValueAsDocument();

                    // namespace uri uniquily defines prefix for the property name
                    String prefix = prefixes.value(namespaceUri);

                    // now scroll over properties
                    for (Field property : props.fields()) {
                        // getting jcr fully qualified name of property
                        // then determine the the name of this property
                        // in the cmis domain
                        String jcrPropertyName = prefix != null ? prefix + ":" + property.getName() : property.getName();
                        debug("property ", jcrPropertyName);
                        String cmisPropertyName = properties.findCmisName(jcrPropertyName);
                        // correlate with custom mapping
                        cmisPropertyName = mapping != null ? mapping.toExtProperty(cmisPropertyName) : cmisPropertyName;
                        debug("cmisPropertyName", cmisPropertyName);
                        // now we need to convert value, we will use
                        // property definition from the original cmis repo for this step
                        PropertyDefinition<?> pdef = propDefs.get(cmisPropertyName);

                        // unknown property?
                        if (pdef == null) {
                            // ignore
                            debug("property definition not found. ignoring", cmisPropertyName, "...");
                            continue;
                        }

                        // make conversation for the value

                        debug("getting cmis value. jcr value type is ", property.getValue().getClass().getSimpleName(),
                                "; vs cmis type: ", pdef.getPropertyType().value());
                        Object cmisValue = null;
                        try {
                            cmisValue = properties.cmisValue(pdef, property.getName(), props);
                        } catch (Exception e) {
                            debug(e.getMessage());
                        }
                        debug("put property: [[", cmisPropertyName, "]] = <", cmisValue != null ? cmisValue.toString() : "", ">");
                        // store properties for update
                        // incorrect value won't be parsed so cmisValue will have null which may overwrite default value for required property
                        // consider not to put empty values while store ??
                        if (ignoreEmptyPropertiesOnCreate && pdef.isRequired() && (cmisValue == null || "".equals(cmisValue.toString()))) {
                            debug("WARNING: property [", cmisPropertyName, "] is required but EMPTY !!!!");
                            continue;
                        }
                        // add property
                        updateProperties.put(cmisPropertyName, cmisValue);
                    }

                }
                // finally execute update action
                debug("update / store properties?? ", updateProperties.isEmpty() ? "No" : "Yep");
                if (!updateProperties.isEmpty()) {
                    if (isDocument(cmisObject) && isVersioned(cmisObject)) {
                        updateVersionedDoc(cmisObject, updateProperties, null);
                    } else {
                        cmisObject.updateProperties(updateProperties);
                    }
                }
                break;
        }
        debug("-- end of store story ---------");
    }

    @Override
    public void updateDocument(DocumentChanges delta) {
        debug("-======== Update Document called delta: ==============-");
        debug("getDocumentId:", delta.getDocumentId());
        debug(delta.getDocument().toString());
        debug("getChildrenChanges/getAppended:", Integer.toString(delta.getChildrenChanges().getAppended().size()));
        debug("getChildrenChanges/getRenamed:", Integer.toString(delta.getChildrenChanges().getRenamed().size()));
        debug("getChildrenChanges/getInsertedBeforeAnotherChild:", Integer.toString(delta.getChildrenChanges().getInsertedBeforeAnotherChild().size()));
        debug("getChildrenChanges/getRemoved:", Integer.toString(delta.getChildrenChanges().getRemoved().size()));

        debug("getParentChanges/getAdded:", Integer.toString(delta.getParentChanges().getAdded().size()));
        debug("getParentChanges/getRemoved:", Integer.toString(delta.getParentChanges().getRemoved().size()));
        debug("getParentChanges/getNewPrimaryParent:", delta.getParentChanges().getNewPrimaryParent());

        debug("getReferrerChanges/getAddedStrong:", Integer.toString(delta.getReferrerChanges().getAddedStrong().size()));
        debug("getReferrerChanges/getAddedWeak:", Integer.toString(delta.getReferrerChanges().getAddedWeak().size()));
        debug("getReferrerChanges/getRemovedStrong:", Integer.toString(delta.getReferrerChanges().getRemovedStrong().size()));
        debug("getReferrerChanges/getRemovedWeak:", Integer.toString(delta.getReferrerChanges().getRemovedWeak().size()));

        debug("getPropertyChanges/getRemoved:", Integer.toString(delta.getPropertyChanges().getRemoved().size()));
        debug("getPropertyChanges/getChanged:", Integer.toString(delta.getPropertyChanges().getChanged().size()));
        debug("getPropertyChanges/getAdded:", Integer.toString(delta.getPropertyChanges().getAdded().size()));

        debug("getMixinChanges/getAdded:", Integer.toString(delta.getMixinChanges().getAdded().size()));
        debug("getMixinChanges/getRemoved:", Integer.toString(delta.getMixinChanges().getRemoved().size()));

        debug("-======== || ==============-");
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
                CmisObject cmisObject = session.getObject(cmisId);
                if (cmisObject == null) {
                    throw new CmisObjectNotFoundException("Cannot find CMIS object with id: " + cmisId);
                }

                // for this case we have the only property - jcr:data
                PropertyChanges changes = delta.getPropertyChanges();

                if (!changes.getRemoved().isEmpty()) {
                    if (isVersioned(cmisObject))
                        updateVersionedDoc(cmisObject, null, null);   // todo check if it removes
                    else asDocument(cmisObject).deleteContentStream();

                } else {
                    ContentStream stream = jcrBinaryContent(delta.getDocument());
                    if (stream != null) {
                        if (isVersioned(cmisObject)) {
                            if (isVersioned(cmisObject)) updateVersionedDoc(cmisObject, null, stream);
                        } else {
                            asDocument(cmisObject).setContentStream(stream, true);
                        }
                    }
                }

                break;
            case OBJECT:
                // modifing cmis:folders and cmis:documents
                cmisObject = session.getObject(objectId.getIdentifier());
                changes = delta.getPropertyChanges();


                // process children changes TODO TODO
                if (delta.getChildrenChanges().getRenamed().size() > 0) {
                    debug("Children changes: renamed", Integer.toString(delta.getChildrenChanges().getRenamed().size()));
                    for (Map.Entry<String, Name> entry : delta.getChildrenChanges().getRenamed().entrySet()) {
                        debug("Child renamed", entry.getKey(), " = ", entry.getValue().toString());

                        CmisObject childCmisObject = session.getObject(entry.getKey());
                        Map<String, Object> updProperties = new HashMap<String, Object>();
                        updProperties.put("cmis:name", entry.getValue().getLocalName());
                        if ((cmisObject instanceof org.apache.chemistry.opencmis.client.api.Document) && isVersioned(cmisObject)) {
                            updateVersionedDoc(childCmisObject, updProperties, null);
                        } else {
                            childCmisObject.updateProperties(updProperties);
                        }
                    }
                }

//                if (delta.getChildrenChanges().getRemoved().size() > 0) {
//                    for (String childId : delta.getChildrenChanges().getRemoved()) {
//                        CmisObject object = session.getObject(childId);
//                        debug("removing child", childId, "with parent", object.getProperty("cmis:parentId").getValue().toString());
//                        CmisObject cmisFolerObject = session.getObject(objectId.getIdentifier());

//                        session.getBinding().getMultiFilingService().removeObjectFromFolder(repositoryId, object.getId(), cmisFolerObject.getId(), null);
//                        ((FileableCmisObject) object).removeFromFolder(cmisFolerObject);
                        /*if (object instanceof Folder) {
                            ((Folder) object).deleteTree(true, UnfileObject.DELETESINGLEFILED, false);
                        } else {
                            ((FileableCmisObject) object).delete(true);
                        }*/

//                        debug("move ", object.getId());
//                        if (!cmisFolerObject.getId().equals(session.getRootFolder().getId()))
//                            ((FileableCmisObject) object).move(cmisFolerObject, session.getRootFolder());



                        /*Map<String, Object> updProperties = new HashMap<String, Object>();
                        updProperties.put(PropertyIds.PARENT_ID, null);
                        updProperties.put(PropertyIds.PATH, null);
                        debug("update properties?? ", updProperties.isEmpty() ? "No" : "Yep");
                        if (!updProperties.isEmpty()) {

                            if ((object instanceof org.apache.chemistry.opencmis.client.api.Document) && isVersioned(object)) {
                                updateVersionedDoc(object, updProperties, null);
                            } else {
                                object.updateProperties(updProperties);
                            }
                        }*/

//                    }
//                }

                debug();
                debug("======= update object =============");

                Document props = delta.getDocument().getDocument("properties");

                // checking that object exists
                if (cmisObject == null) {
                    throw new CmisObjectNotFoundException("Cannot find CMIS object with id: " + objectId.getIdentifier());
                }

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

                MappedCustomType mapping = mappedTypes.findByExtName(cmisObject.getType().getId());

                // convert names and values
                for (Name name : modifications) {
                    debug("name.getNamespaceUri()", name.getNamespaceUri());
                    String prefix = prefixes.value(name.getNamespaceUri());
                    debug("prefix", prefix);
                    // prefixed name of the property in jcr domain is
                    String jcrPropertyName = prefix != null ? prefix + ":" + name.getLocalName() : name.getLocalName();
                    debug("jcrPName", jcrPropertyName);
                    // the name of this property in cmis domain is
                    String cmisPropertyName = properties.findCmisName(jcrPropertyName);
                    debug("cmisName", cmisPropertyName);
                    // correct. AAA!!!
                    cmisPropertyName = mapping != null ? mapping.toExtProperty(cmisPropertyName) : cmisPropertyName;
                    debug("cmis replaced name", cmisPropertyName);
                    // in cmis domain this property is defined as
                    PropertyDefinition<?> pdef = propDefs.get(cmisPropertyName);

                    // unknown property?
                    if (pdef == null) {
                        // ignore
                        continue;
                    }

                    // convert value and store
                    Document jcrValues = props.getDocument(name.getNamespaceUri());

                    Object value = properties.cmisValue(pdef, name.getLocalName(), jcrValues);
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
                    String prefix = prefixes.value(name.getNamespaceUri());

                    // prefixed name of the property in jcr domain is
                    String jcrPropertyName = prefix != null ? prefix + ":" + name.getLocalName() : name.getLocalName();
                    // the name of this property in cmis domain is
                    String cmisPropertyName = properties.findCmisName(jcrPropertyName);

                    // correlate with mapping
                    cmisPropertyName = mapping != null ? mapping.toExtProperty(cmisPropertyName) : cmisPropertyName;

                    // in cmis domain this property is defined as
                    PropertyDefinition<?> pdef = propDefs.get(cmisPropertyName);

                    // unknown property?
                    if (pdef == null) {
                        // ignore
                        debug(cmisPropertyName, "unknown property - ignore ..");
                        continue;
                    }

                    updateProperties.put(cmisPropertyName, null);
                }

                // run update action
                debug("update properties?? ", updateProperties.isEmpty() ? "No" : "Yep");
                if (!updateProperties.isEmpty()) {

                    if ((cmisObject instanceof org.apache.chemistry.opencmis.client.api.Document) && isVersioned(cmisObject)) {
                        updateVersionedDoc(cmisObject, updateProperties, null);
                    } else {
                        cmisObject.updateProperties(updateProperties);
                    }
                }
                // check TODO TODO
                ChildrenChanges childrenChanges = delta.getChildrenChanges();
                Map<String, Name> renamed = childrenChanges.getRenamed();

                for (String key : renamed.keySet()) {
                    CmisObject object = session.getObject(key);
                    if (object == null) continue;

                    Map<String, Object> newName = new HashMap<String, Object>();
                    newName.put("cmis:name", renamed.get(key).getLocalName());

                    object.updateProperties(newName);
                }
                break;
        }
        debug("end of update story -----------------------------");

    }


    @Override
    public boolean isReadonly() {
        return false;
    }

    @Override
    public String newDocumentId(String parentId,
                                Name name,
                                Name primaryType) {
        Map<String, Object> params = new HashMap<String, Object>();
        try {
            debug("-============== NEW DOCUMENT ID ================-", parentId, " / ", name.toString(), "[", primaryType.toString(), " ]");
            // let'start from checking primary type
            if (primaryType.getLocalName().equals("resource")) {
                // nt:resource node belongs to cmis:document's content thus
                // we must return just parent id without creating any CMIS object
                return ObjectId.toString(ObjectId.Type.CONTENT, parentId);
            }

            // all other node types belong to cmis object
            String cmisObjectTypeName = nodes.findCmisName(primaryType);
            debug("type ", cmisObjectTypeName);
            cmisObjectTypeName = jcrTypeToCmis(cmisObjectTypeName);
            debug("resolved type ", cmisObjectTypeName);

            // Ivan, we can pick up object type and property definition map from CMIS repo
            // if not found consider to do an alternative search
            ObjectType objectType = getTypeDefinition(session, cmisObjectTypeName);
            if (!objectType.isBaseType() /* todo do it on other way */) {
                debug("session get type definition for ", cmisObjectTypeName, " : ", objectType.toString());
                Map<String, PropertyDefinition<?>> propDefs = objectType.getPropertyDefinitions();

                // assign mandatory properties
                Collection<PropertyDefinition<?>> list = propDefs.values();
                for (PropertyDefinition<?> pdef : list) {
                    if (pdef.isRequired() && pdef.getUpdatability() == Updatability.READWRITE) {
                        params.put(pdef.getId(), getRequiredPropertyValue(pdef));
                    }
                }
            }

            Folder parent = null;
            try {
                parent = (Folder) session.getObject(parentId);
            } catch (Exception e) {
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
                    debug("return folder id", result, ". new folder id ", newFolderId);

                    break;
                case CMIS_DOCUMENT:
                    VersioningState versioningState = VersioningState.NONE;
                    debug("new Doc, parentId", parentId);

                    if (objectType instanceof DocumentTypeDefinition) {
                        DocumentTypeDefinition docType = (DocumentTypeDefinition) objectType;
                        versioningState = docType.isVersionable() ? VersioningState.MAJOR : versioningState;
                    }

                    debug("to call. createDocument..", "with properties:");
                    for (Map.Entry<String, Object> entry : params.entrySet()) {
                        debug("property", entry.getKey(), "value", entry.getValue().toString());
                    }

                    if (parent == null) {
                        // unfiled
                        debug("try create unfiled..");
                        result = session.createDocument(params, null, null, versioningState, null, null, null).getId();
                    } else {
                        debug("call prent.createDocument..");
                        org.apache.chemistry.opencmis.client.api.Document document = parent.createDocument(params, null, versioningState);
                        result = ObjectId.toString(ObjectId.Type.OBJECT, document.getId());
                    }

                    debug("return CMIS_DOCUMENT", result);
                    String resultId = (versioningState != VersioningState.NONE) ? asDocument(session.getObject(result)).getVersionSeriesId() : result;
                    debug("return CMIS_DOCUMENT", resultId);
                    result = resultId;

                    break;
                default:
                    debug("return null. base type id is ", objectType.getBaseTypeId().value());
            }

            debug("-===NEW DOCUMENT ID ===RESULT= [", primaryType.toString(), "] = ", result, (result == null) ? "!!!!!!" : "");
            return result;
        } catch (Exception e) {
            debug(e.getMessage());
            debug("-===NEW DOCUMENT ID === exceptio !!!!");
        }
        return null;
    }

    // todo improve this logic
    public Object getRequiredPropertyValue(PropertyDefinition<?> remotePropDefinition) {
        if (remotePropDefinition.getCardinality() == Cardinality.MULTI)
            return Collections.singletonList("");
        return remotePropDefinition.getId();
    }

    /**
     * Converts CMIS object to JCR node.
     *
     * @param id the identifier of the CMIS object
     * @return JCR node document.
     */
    private Document cmisObject(String id) {
        CmisObject cmisObject = null;
        try {
            cmisObject = session.getObject(id);
        } catch (CmisObjectNotFoundException cnfe) {
            return null;
        }

        // object does not exist? return null
        if (cmisObject == null) {
            return null;
        }

        // converting CMIS object to JCR node
        switch (cmisObject.getBaseTypeId()) {
            case CMIS_FOLDER:
                return cmisFolder(cmisObject);
            case CMIS_DOCUMENT:
                return cmisDocument(cmisObject);
            case CMIS_POLICY:
            case CMIS_RELATIONSHIP:
            case CMIS_SECONDARY:
            case CMIS_ITEM:
        }

        // unexpected object type
        return null;
    }


    public String cmisTypeToJcr(String cmisTypeId) {
        if (mappedTypes.isEmpty())
            return cmisTypeId;

        MappedCustomType byExtName = mappedTypes.findByExtName(cmisTypeId);
        return (byExtName != null)
                ? byExtName.getJcrName()
                : cmisTypeId;

    }

    public String jcrTypeToCmis(String jcrName) {
        if (mappedTypes.isEmpty())
            return jcrName;

        MappedCustomType byExtName = mappedTypes.findByJcrName(jcrName);
        return (byExtName != null)
                ? byExtName.getExtName()
                : jcrName;

    }

    /**
     * Translates CMIS folder object to JCR node
     *
     * @param cmisObject CMIS folder object
     * @return JCR node document.
     */
    private Document cmisFolder(CmisObject cmisObject) {
        Folder folder = (Folder) cmisObject;
        DocumentWriter writer = newDocument(ObjectId.toString(ObjectId.Type.OBJECT, folder.getId()));

        ObjectType objectType = cmisObject.getType();
        if (objectType.isBaseType()) {
            writer.setPrimaryType(NodeType.NT_FOLDER);
        } else {
            writer.setPrimaryType(cmisTypeToJcr(objectType.getId()));
        }

        writer.setParent(folder.getParentId());

        writer.addMixinType(NodeType.MIX_REFERENCEABLE);
        writer.addProperty(JcrLexicon.UUID, cmisObject.getId());

        cmisProperties(folder, writer);
        cmisChildren(folder, writer);

        // append repository information to the root node
        if (folder.isRootFolder() && !hideRootFolderReference) {
            writer.addChild(ObjectId.toString(ObjectId.Type.REPOSITORY_INFO, ""), REPOSITORY_INFO_NODE_NAME);
        }
        writer.addMixinType(NodeType.MIX_LAST_MODIFIED);
        Property<Object> lastModified = folder.getProperty(PropertyIds.LAST_MODIFICATION_DATE);
        Property<Object> lastModifiedBy = folder.getProperty(PropertyIds.LAST_MODIFIED_BY);
        writer.addProperty(JcrLexicon.LAST_MODIFIED, properties.jcrValues(lastModified));
        writer.addProperty(JcrLexicon.LAST_MODIFIED_BY, properties.jcrValues(lastModifiedBy));
        return writer.document();
    }


    /**
     * Translates cmis document object to JCR node.
     *
     * @param cmisObject cmis document node
     * @return JCR node document.
     */
    public Document cmisDocument(CmisObject cmisObject) {
        org.apache.chemistry.opencmis.client.api.Document doc = asDocument(cmisObject);

        // document and internalID
        // internal id = cmisId (when folder or not versioned) || version series id
        String internalId = isVersioned(cmisObject) ? doc.getVersionSeriesId() : doc.getId();
        DocumentWriter writer = newDocument(ObjectId.toString(ObjectId.Type.OBJECT, internalId));

        // type
        ObjectType objectType = cmisObject.getType();
        writer.setPrimaryType(objectType.isBaseType() ? NodeType.NT_FILE : cmisTypeToJcr(objectType.getId()));

        // parents
        List<Folder> parents = doc.getParents();
        ArrayList<String> parentIds = new ArrayList<String>();
        for (Folder f : parents) {
            parentIds.add(ObjectId.toString(ObjectId.Type.OBJECT, f.getId()));
        }
        writer.setParents(parentIds);

        // properties
        // document specific property conversation
        cmisProperties(doc, writer);

        // reference
        writer.addMixinType(NodeType.MIX_REFERENCEABLE);
        writer.addProperty(JcrLexicon.UUID, internalId);

        // children
        writer.addChild(ObjectId.toString(ObjectId.Type.CONTENT, internalId), JcrConstants.JCR_CONTENT);

        return writer.document();
    }

    /**
     * Converts binary content into JCR node.
     *
     * @param id the id of the CMIS document.
     * @return JCR node representation.
     */
    private Document cmisContent(String id) {
        String contentId = ObjectId.toString(ObjectId.Type.CONTENT, id);
        DocumentWriter writer = newDocument(contentId);

        org.apache.chemistry.opencmis.client.api.Document doc = asDocument(session.getObject(id));
        writer.setPrimaryType(NodeType.NT_RESOURCE);
        writer.setParent(id);

        if (doc.getContentStream() != null) {
            InputStream is = doc.getContentStream().getStream();
            BinaryValue content = factories.getBinaryFactory().create(is);
            writer.addProperty(JcrConstants.JCR_DATA, content);
            writer.addProperty(JcrConstants.JCR_MIME_TYPE, doc.getContentStream().getMimeType());
        }

        // reference
        writer.addMixinType(NodeType.MIX_REFERENCEABLE);
        writer.addProperty(JcrLexicon.UUID, contentId);

        Property<Object> lastModified = doc.getProperty(PropertyIds.LAST_MODIFICATION_DATE);
        Property<Object> lastModifiedBy = doc.getProperty(PropertyIds.LAST_MODIFIED_BY);

        writer.addProperty(JcrLexicon.LAST_MODIFIED, properties.jcrValues(lastModified));
        writer.addProperty(JcrLexicon.LAST_MODIFIED_BY, properties.jcrValues(lastModifiedBy));

        writer.addMixinType(NodeType.MIX_CREATED);
        Property<Object> created = doc.getProperty(PropertyIds.CREATION_DATE);
        Property<Object> createdBy = doc.getProperty(PropertyIds.CREATED_BY);
        writer.addProperty(JcrLexicon.CREATED, properties.jcrValues(created));
        writer.addProperty(JcrLexicon.CREATED_BY, properties.jcrValues(createdBy));

        return writer.document();
    }

    /**
     * Converts CMIS object's properties to JCR node properties.
     *
     * @param object CMIS object
     * @param writer JCR node representation.
     */
    private void cmisProperties(CmisObject object,
                                DocumentWriter writer) {
        // convert properties
        List<Property<?>> list = object.getProperties();
        TypeDefinition type = object.getType();
        MappedCustomType typeMapping = mappedTypes.findByExtName(type.getId());

        Set<String> requiredExtProperties = new LinkedHashSet<String>(type.getPropertyDefinitions().keySet());

//        debug();
//        debug("properties for ", object.getType().getId(), typeMapping != null ? typeMapping.getJcrName() : "<no jcr mapping>");
        for (Property<?> property : list) {
//            debug("Process property -- : [", property.getId(), " ] with value:", property.getValueAsString());
            String pname = properties.findJcrName(property.getId());
            PropertyDefinition<?> propertyDefinition = type.getPropertyDefinitions().get(property.getId());

            boolean ignore = (pname != null && !pname.startsWith("jcr:")) && ((propertyDefinition.getUpdatability() == Updatability.READONLY)
                    || type.getId().equals(BaseTypeId.CMIS_DOCUMENT.value()));


            // check if ignore defined explicitly with configuration -> ignoreExternalProperties
            ignore = ignore || ((typeMapping != null) && typeMapping.getIgnoreExternalProperties().contains(pname));

            if (pname != null && !pname.startsWith(CMIS_PREFIX) && !ignore) {
                String propertyTargetName = typeMapping != null ? typeMapping.toJcrProperty(pname) : pname;
                Object[] values = properties.jcrValues(property);
                if (propertyDefinition.isRequired() && (values == null || values.length == 0)) {
                    debug("WARNING: property [", property.getId(), "] has empty value!!!!");
                }
                writer.addProperty(propertyTargetName, values);
            }
            requiredExtProperties.remove(property.getId());
        }

        // error protection
        if (addRequiredPropertiesOnRead) {
            for (String requiredExtProperty : requiredExtProperties) {
                PropertyDefinition<?> propertyDefinition = type.getPropertyDefinitions().get(requiredExtProperty);
                if (propertyDefinition.isRequired() && propertyDefinition.getUpdatability() == Updatability.READWRITE && !requiredExtProperty.startsWith(CMIS_PREFIX)) {
                    String pname = properties.findJcrName(requiredExtProperty);
                    String propertyTargetName = typeMapping != null ? typeMapping.toJcrProperty(pname) : pname;
                    debug("error protection: adding required property: ", propertyTargetName);
                    writer.addProperty(propertyTargetName, getRequiredPropertyValue(propertyDefinition));
                }
            }
        }
//        debug("propertie done");
//        debug();
    }

    /**
     * Converts CMIS folder children to JCR node children
     *
     * @param folder CMIS folder
     * @param writer JCR node representation
     */
    private void cmisChildren(Folder folder,
                              DocumentWriter writer) {
        ItemIterable<CmisObject> it = folder.getChildren();
        for (CmisObject obj : it) {
            if (isDocument(obj) && isVersioned(obj))
                writer.addChild(asDocument(obj).getVersionSeriesId(), obj.getName());
            else writer.addChild(obj.getId(), obj.getName());
        }
    }

    /**
     * Translates CMIS repository information into Node.
     *
     * @return node document.
     */
    private Document cmisRepository() {
        RepositoryInfo info = session.getRepositoryInfo();
        DocumentWriter writer = newDocument(ObjectId.toString(ObjectId.Type.REPOSITORY_INFO, ""));

        writer.setPrimaryType(CmisLexicon.REPOSITORY);
        writer.setId(REPOSITORY_INFO_ID);

        // product name/vendor/version
        writer.addProperty(CmisLexicon.VENDOR_NAME, info.getVendorName());
        writer.addProperty(CmisLexicon.PRODUCT_NAME, info.getProductName());
        writer.addProperty(CmisLexicon.PRODUCT_VERSION, info.getProductVersion());

        return writer.document();
    }

    /**
     * Creates content stream using JCR node.
     *
     * @param document JCR node representation
     * @return CMIS content stream object
     */
    private ContentStream jcrBinaryContent(Document document) {

        try {
            DocumentReader reader = readDocument(document);
            Document props = document.getDocument("properties").getDocument(JcrLexicon.Namespace.URI);

            String fileName = props.getString("fileName");
            String mimeType = props.getString("mimeType");

            org.modeshape.jcr.value.Property content = reader.getProperty(JCR_DATA);
            BinaryValue binary = factories().getBinaryFactory().create(content.getFirstValue());
//        OutputStream ostream = new BufferedOutputStream(new FileOutputStream(file));
//        IoUtil.write(binary.getStream(), ostream);
            // create content stream
            ContentStreamImpl contentStream = new ContentStreamImpl(fileName, BigInteger.valueOf(binary.getSize()), mimeType, binary.getStream());
            return contentStream;
        } catch (RepositoryException re) {
            debug("get content RepositoryException ", re.getMessage());
//        } catch (IOException ioe) {
//            debug("get content RepositoryException ", ioe.getMessage());
        }

        return null;
    }

    private ContentStream jcrBinaryContentOld(Document document) {
        // pickup node properties
        Document props = document.getDocument("properties").getDocument(JcrLexicon.Namespace.URI);

        // extract binary value and content
        Binary value = props.getBinary("data");

        if (value == null) {
            return null;
        }

        byte[] content = value.getBytes();
        String fileName = props.getString("fileName");
        String mimeType = props.getString("mimeType");

        // wrap with input stream
        ByteArrayInputStream bin = new ByteArrayInputStream(content);
        bin.reset();

        // create content stream
        return new ContentStreamImpl(fileName, BigInteger.valueOf(content.length), mimeType, bin);
    }

    /**
     * Import CMIS types to JCR repository.
     *
     * @param types       CMIS types
     * @param typeManager JCR type manager
     * @param registry
     * @throws RepositoryException if there is a problem importing the types
     */
    private void importTypes(List<Tree<ObjectType>> types,
                             NodeTypeManager typeManager,
                             NamespaceRegistry registry,
                             List<NodeTypeTemplate> typeTemplates) throws RepositoryException {
        for (Tree<ObjectType> tree : types) {
            importType(tree.getItem(), typeManager, registry, typeTemplates);
            importTypes(tree.getChildren(), typeManager, registry, typeTemplates);
        }
    }

    /**
     * Import given CMIS type to the JCR repository.
     *
     * @param cmisType    cmis object type
     * @param typeManager JCR type manager/
     * @param registry    jcr namespace registry
     * @throws RepositoryException if there is a problem importing the types
     */
    @SuppressWarnings("unchecked")
    public void importType(ObjectType cmisType,
                           NodeTypeManager typeManager,
                           NamespaceRegistry registry,
                           List<NodeTypeTemplate> typeTemplates) throws RepositoryException {
        // cache
        cachedTypeDefinitions.put(cmisType.getId(), cmisType);
        // skip base types because we are going to
        // map base types directly
        if (cmisType.isBaseType() || cmisType.getId().equals(CMIS_DOCUMENT_UNVERSIONED)) {
            return;
        }
        String cmisTypeId = cmisType.getId();
        MappedCustomType mapping = mappedTypes.findByExtName(cmisType.getId());
        if (mapping != null) cmisTypeId = mapping.getJcrName();

        // namespace registration
        debug("Type: ", cmisTypeId);
        if (!cmisTypeId.equals(cmisType.getLocalName()) && cmisTypeId.contains(":")) {

            String nsPrefix = cmisTypeId.substring(0, cmisTypeId.indexOf(":"));
            String nsUri = mapping == null ? cmisType.getLocalNamespace() : mapping.getJcrNamespaceUri();
            debug("check type namespace type: ", nsPrefix, ":", nsUri);
            // check is ns is not registered already with exactly same prefix and uri
            // if one of items presents typeManager should throw an exception while registering
            if (!isNsAlreadyRegistered(cmisType, registry, nsPrefix, nsUri)) {
                debug("register namespace type: ", nsPrefix, ":", nsUri);
                registry.registerNamespace(nsPrefix, nsUri);
            }
            prefixes.addNamespace(nsPrefix, nsUri);
        }


        // create node type template
        NodeTypeTemplate type = typeManager.createNodeTypeTemplate();

        // convert CMIS type's attributes to node type template we have just created
        type.setName(mapping != null ? mapping.getJcrName() : cmisType.getId());
        type.setAbstract(!cmisType.isCreatable());
        type.setMixin(false);
        type.setOrderableChildNodes(true);
        type.setQueryable(true);
        type.setDeclaredSuperTypeNames(superTypes(cmisType));

        Map<String, PropertyDefinition<?>> props = cmisType.getPropertyDefinitions();
        Set<String> names = props.keySet();
        // properties
        for (String name : names) {
//            debug("importing property: ", name, " ...");
            if (name.startsWith(CMIS_PREFIX)) continue; // ignore them. they must be handled/mapped with default logic
            if (mapping != null && mapping.getIgnoreExternalProperties().contains(name)) continue; // explicit ignore

            PropertyDefinition<?> cmisPropDef = props.get(name);
            PropertyDefinitionTemplate jcrProp = typeManager.createPropertyDefinitionTemplate();
            jcrProp.setName(mapping != null ? mapping.toJcrProperty(name) : name);

            jcrProp.setMandatory(cmisPropDef.isRequired());
            jcrProp.setRequiredType(properties.getJcrType(cmisPropDef.getPropertyType()));
            jcrProp.setMultiple(cmisPropDef.getCardinality().equals(Cardinality.MULTI));
            jcrProp.setProtected(cmisPropDef.getUpdatability() == Updatability.READONLY);
            jcrProp.setAvailableQueryOperators(new String[]{});
            jcrProp.setAutoCreated(false);

            // if property is protected and already declared in parents - ignore it
            // we cannot override protected property so try to avoid it
            if (jcrProp.isProtected()/* && parentsHasPropertyDeclared(typeManager, type, jcrProp)*/) {
//                debug("ignore", jcrProp.getName());
                continue;
            }

            if (cmisPropDef.getChoices() != null && cmisPropDef.getChoices().size() > 0) {
                LinkedList<String> choices = new LinkedList<String>();
                for (Choice choice : cmisPropDef.getChoices()) {
                    if (choice.getValue() != null && choice.getValue().size() > 0)
                        choices.add((String) choice.getValue().get(0));
                }
                jcrProp.setValueConstraints(choices.toArray(new String[choices.size()]));
            }

            if (!jcrProp.isProtected()) type.getPropertyDefinitionTemplates().add(jcrProp);
        }

        typeTemplates.add(type);
        Name jcrName = getContext().getValueFactories().getNameFactory().create(type.getName());
        nodes.addTypeMapping(jcrName, cmisTypeId);
    }
    /*
    * update existent types in typeManager with new features
    */
    public void updateTypes(NodeTypeManager typeManager, List<NodeTypeTemplate> defList) throws RepositoryException {
        if (mappedTypes == null)
            return;

        if (typeManager == null)
            return;

        for ( String typeKey : mappedTypes.indexByJcrName.keySet()) {
            MappedCustomType mcType = mappedTypes.findByJcrName(typeKey);
            // Enabling SNS
            if (mcType.hasFeature(FEATURE_NAME_SNS)) {
                NodeDefinitionTemplate child = typeManager.createNodeDefinitionTemplate();
                child.setName("*");
                String baseTypeName = mcType.getFeature(FEATURE_NAME_SNS);

                child.setRequiredPrimaryTypeNames(new String[]{baseTypeName});
                child.setSameNameSiblings(true);

                // Obtain type definition - and update it
                boolean foundInDefList = false;
                for( NodeTypeTemplate defType: defList ) {
                    if (defType.getName().equals(typeKey)) {
                        foundInDefList = true;
                        defType.getNodeDefinitionTemplates().add(child);
                    }
                }
                if (foundInDefList) {
                    continue;  // was already updated in definition list
                }

                NodeType type = null;
                try {
                    type = typeManager.getNodeType(typeKey);
                } catch (NoSuchNodeTypeException e) {
                    continue;   // no such type registered
                }
                if (type == null)
                    continue;
                NodeTypeTemplate typeTemplate = typeManager.createNodeTypeTemplate(type);
                typeTemplate.getNodeDefinitionTemplates().add(child);
                // Type was obtained from type manager, so update it there
                NodeTypeDefinition[] nodeDefs = new NodeTypeDefinition[]{typeTemplate};
                typeManager.registerNodeTypes(nodeDefs, true);
            }
        }
    }

    /*
    * looking for protected property through node's parents` definitions
    */
    public boolean parentsHasPropertyDeclared(javax.jcr.nodetype.NodeTypeManager typeManager, NodeTypeDefinition typeDef, PropertyDefinitionTemplate pt) {
        for (String sType : typeDef.getDeclaredSupertypeNames()) {
            try {
                NodeType nodeType = typeManager.getNodeType(sType);
                javax.jcr.nodetype.PropertyDefinition[] propertyDefinitions = nodeType.getPropertyDefinitions();
                for (javax.jcr.nodetype.PropertyDefinition propertyDefinition : propertyDefinitions) {
                    if (propertyDefinition.getName().equals(pt.getName()) && propertyDefinition.isProtected())
                        return true;
                }
            } catch (RepositoryException ignore) {/**/}
        }

        return false;
    }

    private boolean isNsAlreadyRegistered(ObjectType cmisType, NamespaceRegistry registry, String nsPrefix, String nsUri) throws RepositoryException {
        if (ArrayUtils.contains(registry.getPrefixes(), nsPrefix) && ArrayUtils.contains(registry.getURIs(), nsUri))
            return true;

        if (cmisType != null && StringUtils.equals(cmisType.getBaseType().getLocalNamespace(), cmisType.getLocalNamespace()))
            return true;

        return false;
    }

    /**
     * Determines supertypes for the given CMIS type in terms of JCR.
     *
     * @param cmisType given CMIS type
     * @return supertypes in JCR lexicon.
     */
    private String[] superTypes(ObjectType cmisType) {
        String parentType = (cmisType.getParentType() != null) ?
                getJcrTypeId(cmisTypeToJcr(cmisType.getParentType().getId()))
                : null;

        if (parentType == null) {
            if (cmisType.getBaseTypeId() == BaseTypeId.CMIS_FOLDER) {
                parentType = JcrConstants.NT_FOLDER;
            } else if (cmisType.getBaseTypeId() == BaseTypeId.CMIS_DOCUMENT) {
                parentType = JcrConstants.NT_FILE;
            }
        }

        return addMixins(cmisType, new String[]{parentType});
    }


    /*
    * supposed to add mix:versionable if cmis types is a VersionableDocument
    * there used to be versionable notation but versioning support for connectors is not there yet
    */
    protected String[] addMixins(ObjectType cmisType, String[] superTypes) {
        return superTypes;
    }


    /*
    * replace direct CMIS types with JCR equivalents
    */
    private String getJcrTypeId(String cmisTypeId) {
        if (cmisTypeId == null) return null;

        if (cmisTypeId.equals(BaseTypeId.CMIS_DOCUMENT.value())) {
            return JcrConstants.NT_FILE;
        } else if (cmisTypeId.equals(BaseTypeId.CMIS_FOLDER.value())) {
            return JcrConstants.NT_FOLDER;
        }

        return cmisTypeId;
    }

    /**
     * Defines node type for the repository info.
     *
     * @param typeManager JCR node type manager.
     * @throws RepositoryException
     */
    @SuppressWarnings("unchecked")
    private void registerRepositoryInfoType(NodeTypeManager typeManager) throws RepositoryException {
        // create node type template
        NodeTypeTemplate type = typeManager.createNodeTypeTemplate();

        // convert CMIS type's attributes to node type template we have just created
        type.setName("cmis:repository");
        type.setAbstract(false);
        type.setMixin(false);
        type.setOrderableChildNodes(true);
        type.setQueryable(true);
        type.setDeclaredSuperTypeNames(new String[]{JcrConstants.NT_FOLDER});

        PropertyDefinitionTemplate vendorName = typeManager.createPropertyDefinitionTemplate();
        vendorName.setAutoCreated(false);
        vendorName.setName("cmis:vendorName");
        vendorName.setMandatory(false);

        type.getPropertyDefinitionTemplates().add(vendorName);

        PropertyDefinitionTemplate productName = typeManager.createPropertyDefinitionTemplate();
        productName.setAutoCreated(false);
        productName.setName("cmis:productName");
        productName.setMandatory(false);

        type.getPropertyDefinitionTemplates().add(productName);

        PropertyDefinitionTemplate productVersion = typeManager.createPropertyDefinitionTemplate();
        productVersion.setAutoCreated(false);
        productVersion.setName("cmis:productVersion");
        productVersion.setMandatory(false);

        type.getPropertyDefinitionTemplates().add(productVersion);

        // register type
        NodeTypeDefinition[] nodeDefs = new NodeTypeDefinition[]{type};
        typeManager.registerNodeTypes(nodeDefs, true);
    }


    public boolean isVersioned(CmisObject cmisObject) {
        ObjectType objectType = cmisObject.getType();
        if (objectType instanceof DocumentTypeDefinition) {
            DocumentTypeDefinition docType = (DocumentTypeDefinition) objectType;
            return docType.isVersionable();
        }

        return false;
    }

    public org.apache.chemistry.opencmis.client.api.Document checkout(CmisObject cmisObject) {
        debug("checkout..");
        org.apache.chemistry.opencmis.client.api.Document doc = (org.apache.chemistry.opencmis.client.api.Document) cmisObject;
        org.apache.chemistry.opencmis.client.api.ObjectId objectId1 = doc.checkOut();
        org.apache.chemistry.opencmis.client.api.Document pwc = (org.apache.chemistry.opencmis.client.api.Document) session.getObject(objectId1);
        debug("checked out pwc:", pwc.toString());
        return pwc;
    }


    public String checkin(org.apache.chemistry.opencmis.client.api.Document pwc, Map<String, ?> properties, ContentStream contentStream, String storedId) {
        try {
            debug("checkin..");
            String id = pwc.checkIn(true, properties, contentStream, "connector's check in").getId();
            debug("checkin id:", id);
        } catch (CmisBaseException e) {
            e.printStackTrace(); // todo
            pwc.cancelCheckOut();
        }
        return pwc.getId();
    }

    public void deleteStreamVersioned(CmisObject object) {
        org.apache.chemistry.opencmis.client.api.Document pwc = checkout(object);
        pwc.deleteContentStream();
    }

    public void deleteVersioned(CmisObject object) {
        org.apache.chemistry.opencmis.client.api.Document document = asDocument(object);
        debug("is PWC:", Boolean.toString(document.isPrivateWorkingCopy()));
        debug("is VSeries checked out:", Boolean.toString(document.isVersionSeriesCheckedOut()));
        if (document.isPrivateWorkingCopy() || document.isVersionSeriesCheckedOut())
            document.deleteAllVersions();

        org.apache.chemistry.opencmis.client.api.Document pwc = checkout(object);
        pwc.deleteAllVersions();
    }


    public String updateVersionedDoc(CmisObject cmisObject, Map<String, ?> properties, ContentStream contentStream) {
        String storedId = cmisObject.getId();
        org.apache.chemistry.opencmis.client.api.Document pwc = checkout(cmisObject);
        return checkin(pwc, properties, contentStream, storedId);
    }

    public boolean isDocument(CmisObject cmisObject) {
        return cmisObject instanceof org.apache.chemistry.opencmis.client.api.Document;
    }

    public org.apache.chemistry.opencmis.client.api.Document asDocument(CmisObject cmisObject) {
        if (!isDocument(cmisObject))
            throw new CmisInvalidArgumentException("Object is not a document: "
                    + cmisObject.getId()
                    + " with type "
                    + cmisObject.getType().getId());
        return (org.apache.chemistry.opencmis.client.api.Document) cmisObject;
    }


    private ObjectType getTypeDefinition(Session session, String typeId) {
        if (cachedTypeDefinitions.containsKey(typeId)) {
            ObjectType typeDefinition = session.getTypeDefinition(typeId);
            cachedTypeDefinitions.put(typeId, typeDefinition);
        }
        return cachedTypeDefinitions.get(typeId);
    }

    public Document getChildReference(String parentKey,
                                      String childKey) {
        CmisObject object = session.getObject(childKey);
        return newChildReference(object.getId(), object.getName());
    }
}
