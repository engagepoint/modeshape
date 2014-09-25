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

import org.apache.chemistry.opencmis.client.api.CmisObject;


import org.apache.chemistry.opencmis.client.api.*;

import org.apache.chemistry.opencmis.client.bindings.spi.StandardAuthenticationProvider;
import org.apache.chemistry.opencmis.client.runtime.ObjectIdImpl;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.data.RepositoryInfo;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.apache.chemistry.opencmis.commons.enums.IncludeRelationships;
import org.apache.chemistry.opencmis.commons.exceptions.CmisRuntimeException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.ContentStreamImpl;
import org.apache.commons.lang3.StringUtils;
import org.infinispan.schematic.document.Document;
import org.modeshape.common.collection.Problem;
import org.modeshape.common.collection.Problems;
import org.modeshape.common.collection.SimpleProblems;
import org.modeshape.connector.cmis.common.CompareTypesI18n;
import org.modeshape.connector.cmis.config.CmisConnectorConfiguration;
import org.modeshape.connector.cmis.config.TypeCustomMappingList;
import org.modeshape.connector.cmis.features.CmisBinaryValue;
import org.modeshape.connector.cmis.features.SingleVersionDocumentsCache;
import org.modeshape.connector.cmis.features.SingleVersionOptions;
import org.modeshape.connector.cmis.features.TempDocument;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.modeshape.connector.cmis.mapping.MappedTypesContainer;
import org.modeshape.connector.cmis.operations.BinaryContentProducerInterface;
import org.modeshape.connector.cmis.operations.CmisObjectFinderUtil;
import org.modeshape.connector.cmis.operations.DocumentProducer;
import org.modeshape.connector.cmis.operations.impl.*;
import org.modeshape.connector.cmis.util.CryptoUtils;
import org.modeshape.jcr.JcrLexicon;
import org.modeshape.jcr.RepositoryConfiguration;
import org.modeshape.jcr.api.JcrConstants;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.federation.spi.*;
import org.modeshape.jcr.value.BinaryValue;
import org.modeshape.jcr.value.Name;
import org.modeshape.jcr.value.binary.ExternalBinaryValue;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinitionTemplate;
import javax.xml.ws.handler.HandlerResolver;
import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.util.*;

import org.infinispan.Cache;
import org.modeshape.connector.cmis.cache.DistriburedCmisCacheImpl;

import static org.modeshape.connector.cmis.operations.impl.CmisOperationCommons.asDocument;
import static org.modeshape.connector.cmis.operations.impl.CmisOperationCommons.isDocument;
import static org.modeshape.connector.cmis.util.CompareTypeDefinitionsUtil.compareTypeDefinitions;

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
 * </table>
 *
 * @author Oleg Kulikov
 * @author Ivan Vasyliev
 * @author Nick Knysh
 */
public class CmisConnector extends Connector implements Pageable, UnfiledSupportConnector, EnhancedConnector, SelfCheckConnector {

    /**
     * SLF logger.
     */
    public final org.slf4j.Logger LOG = LoggerFactory.getLogger(this.getClass());

    // path and id for the repository node
    private Session session;
    // -----  json settings -------------
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

    private String atomPubUrl;

    private Properties properties;
    // root folder reference flag
    private boolean hideRootFolderReference;
    // credentials
    private String user;
    private String password;
    // repository id
    private String repositoryId;
    // client port container.
    // required for CmisClient to pick correct ws implementation
    private String clientPortProvider;
    // types customization
    public TypeCustomMappingList customMapping = new TypeCustomMappingList();
    // to not reset required properties on document create
    private boolean ignoreEmptyPropertiesOnCreate = false;
    // add required properties to a document if not present
    private boolean addRequiredPropertiesOnRead = false;
    // unfiled types binding
    private String[] applicableUnfiledTypes;
    // id of remote unfiled storage if any
    // for FileNet normally = NULL
    private String remoteUnfiledNodeId;
    private String versioningOnUpdateMetadata = "MAJOR";
    // debug
    private boolean debug = false;

    private String unfiledQueryTemplate = null;

    // sns index for optimization
    // -1 will calculate real value
    private int snsCommonIndex = 0; /*-1*/

    //
    private boolean folderSetUnknownChildren = false;

    private String connectorProblems = null;

    long pageSize = Constants.DEFAULT_PAGE_SIZE;
    long pageSizeUnfiled = Constants.DEFAULT_PAGE_SIZE_UNFILED;

    // single version && commonId  logic
    private SingleVersionOptions singleVersionOptions = new SingleVersionOptions();

    Map<String, List<RepositoryConfiguration.ProjectionConfiguration>> preconfiguredProjections;

    // -----  runtime variables container -------------
    private RuntimeSnapshot runtimeSnapshot;
    // indicates storage which connector looks at
    private String languageDialect;

    // name of class with search realization
    private String cmisObjectFinderUtil;

    public CmisConnector() {
        super();
    }

    // external usage --------
    public TypeCustomMappingList getCustomMapping() {
        return customMapping;
    }

    public SingleVersionOptions getSingleVersionOptions() {
        return singleVersionOptions;
    }

    public Map<String, ObjectType> getCachedTypeDefinitions() {
        Map<String, ObjectType> cachedTypeDefinitions =
                runtimeSnapshot.getLocalTypeManager().getCachedTypeDefinitions();
        return cachedTypeDefinitions;
    }

    // -------------------

    // internal containers for passing as paramenters
    private CmisConnectorConfiguration configuration;

    //

    @Override
    public Collection<Name> getApplicableUnfiledTypes() {
        return runtimeSnapshot.getLocalTypeManager().getApplicableTypesInstance();
    }

    public String getObjectMappedId(QueryResult queryResult) {
        return runtimeSnapshot.getCmisObjectFinderUtil().getObjectMappingId(queryResult);
    }

    // required by the custom query processor
    public Session getSession() {
        return runtimeSnapshot.getSession();
    }

    private LocalTypeManager getLocalTypeManager() {
        return runtimeSnapshot.getLocalTypeManager();
    }

    @Override
    public boolean isReadonly() {
        return false;
    }

    public MappedTypesContainer getMappedTypes() {
        return runtimeSnapshot.getLocalTypeManager().getMappedTypes();
    }

    /**
     * Map of all registered properties, where key is "external name"
     * and value is "jcr name".
     *
     * @return Map of registered properties.
     */
    public Map<String, String> getRegisteredProperties() {
        return runtimeSnapshot.getLocalTypeManager().getRegisteredProperties();
    }

    @Override
    public void initialize(NamespaceRegistry registry,
                           NodeTypeManager nodeTypeManager, Cache cache) throws RepositoryException, IOException, IllegalArgumentException {
        super.initialize(registry, nodeTypeManager, cache);

        // pack settings into containers for easy passing to sub-classes
        this.configuration = new CmisConnectorConfiguration(
                ignoreEmptyPropertiesOnCreate,
                addRequiredPropertiesOnRead,
                snsCommonIndex, remoteUnfiledNodeId, unfiledQueryTemplate, folderSetUnknownChildren,
                pageSize, pageSizeUnfiled, singleVersionOptions,
                hideRootFolderReference,
                debug, versioningOnUpdateMetadata,
                getSourceName(), getMimeTypeDetector());

        // setup CMIS connection
        session = getSoapCmisConnection(cache);

        // create types container
        LocalTypeManager localTypeManager = new LocalTypeManager(
                getContext().getValueFactories(),
                registry, nodeTypeManager,
                customMapping);
        localTypeManager.setDebug(debug);
        // register external types into JCR
        localTypeManager.initialize(session, applicableUnfiledTypes);
        // Single version saving feature
        try {
            singleVersionOptions.initialize(factories(), localTypeManager);
        } catch (Exception e) {
            throw new RepositoryException(e);
        }
        // extended getObject logic


        SingleVersionDocumentsCache singleVersionCache = new SingleVersionDocumentsCache();
        ConnectorDocumentProducer documentProducer = new ConnectorDocumentProducer();


        runtimeSnapshot = new RuntimeSnapshot(session, localTypeManager, singleVersionOptions, singleVersionCache,
                documentProducer, preconfiguredProjections, cmisObjectFinderUtil, languageDialect, cache);
    }


    @Override
    public Document getDocumentById(String id) {
        // object id is a composite key which holds information about
        // unique object identifier and about its type
        log().info("GET-DOCUMENT-BY-ID : " + id);
        ObjectId objectId = ObjectId.valueOf(id);

        CmisGetObjectOperation cmisGetObjectOperation = getCmisGetOperation();
        // lookup in the cache first
        if (isCachedTempDocument(objectId.getIdentifier())) {
            return getCmisSingleVersionOperations().getCachedTempDocument(objectId);
        }

        // this action depends from object type
        switch (objectId.getType()) {
            case REPOSITORY_INFO:
                // convert information about repository from
                // cmis domain into jcr domain
                return cmisRepository();
            case UNFILED_STORAGE:
                // unfiled storage node
                return cmisGetObjectOperation.jcrUnfiled(id, getUnfiledParentId());
            case CONTENT:
                // in the jcr domain content is represented by child node of
                // the nt:file node while in cmis domain it is a property of
                // the cmis:document object. This action searches original
                // cmis:document and converts its content property into jcr node
                // result object should have same id as requested
                return cmisGetObjectOperation.cmisContent(objectId.getIdentifier());
            //return null;
            case OBJECT:
                // converts cmis folders and documents into jcr folders and files
                return cmisObject(objectId.getIdentifier());

            default:
                return null;
        }
    }

    @Override
    public ExternalBinaryValue getBinaryValue(String id) {
        try {
            CmisObjectFinderUtil finderUtil = runtimeSnapshot.getCmisObjectFinderUtil();
            return new CmisBinaryValue(id, finderUtil, getSourceName(), getMimeTypeDetector());
        } catch (CmisObjectNotFoundException e) {
            return null;
        }
    }


    /*
    * look up local temp storage for proposed id
    */
    public boolean isCachedTempDocument(String id) {
        return runtimeSnapshot.getSingleVersionCache().containsKey(id);
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
            case UNFILED_STORAGE:
                // exist when supported
                return true;
            case CONTENT:
                // in the jcr domain content is represented by child node of
                // the nt:file node while in cmis domain it is a property of
                // the cmis:document object. so to perform this operation we need
                // to restore identifier of the original cmis:document. it is easy
                // now checking that this document exists
                return runtimeSnapshot.getCmisObjectFinderUtil().find(objectId.getIdentifier()) != null;
            default:
                // here we checking cmis:folder and cmis:document
                return runtimeSnapshot.getCmisObjectFinderUtil().find(id) != null;
        }
    }


    // --------------- id/path resolutions ----------------

    @Override
    public String getDocumentId(String path) {
        log().info("GET-DOCUMENT-BY-PATH : " + path);
        // establish relation between path and object identifier
        String id = runtimeSnapshot.getSession().getObjectByPath(path).getId();
        // try to catch and save first projection's folderId to stick unfiled to it..
//        if (caughtProjectedId == null) caughtProjectedId = id;
        // what if 1st projection is not up but second is ok ?? will we get there to get Id of the first one ?

        return id;
    }

    @Override
    public Collection<String> getDocumentPathsById(String id) {
        CmisObject obj = runtimeSnapshot.getSession().getObject(id);
        if (obj instanceof Folder) {
            return Collections.singletonList(((Folder) obj).getPath());
        }
        if (isDocument(obj)) {
            org.apache.chemistry.opencmis.client.api.Document doc = asDocument(obj);
            List<Folder> parents = doc.getParents();
            List<String> paths = new ArrayList<String>(parents.size());
            for (Folder parent : doc.getParents()) {
                paths.add(parent.getPath() + "/" + doc.getName());
            }
            return paths;

        }


        return Collections.emptyList();
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
     * @param name   new name
     */
    private void rename(CmisObject object, String name) {
        Map<String, Object> newName = new HashMap<String, Object>();
        newName.put("cmis:name", name);

        object.updateProperties(newName);
    }


    // -------------------  CRUD implementation ------------------

    @Override
    public String newDocumentId(String parentId,
                                Name name,
                                Name primaryType) {

        // do some early check to prevent extra actions..
        // let'start from checking primary type
        if (primaryType.getLocalName().equals("resource")) {
            // nt:resource node belongs to cmis:document's content thus
            // we must return just parent id without creating any CMIS object
            return ObjectId.toString(ObjectId.Type.CONTENT, parentId);
        }

        CmisSingleVersionOperations singleVersionOps = getCmisSingleVersionOperations();

        if (singleVersionOps.doAsSingleVersion(primaryType)) {
            return singleVersionOps.newDocumentId(parentId, name, primaryType);
        } else {
            CmisNewObjectOperation cmisNewObjectOperation = getCmisNewObjectOperation();
            return cmisNewObjectOperation.newDocumentId(parentId, name, primaryType);
        }
    }


    @Override
    public void storeDocument(Document document) {
        ObjectId objectId = ObjectId.valueOf(document.getString("key"));
        CmisSingleVersionOperations singleVersionOps = getCmisSingleVersionOperations();

        if (singleVersionOps.doStoreAsSingleVersion(objectId)) {
            singleVersionOps.storeDocument(objectId, document,
                    getCmisNewObjectCombinedOperation(),
                    new BinaryContentProducer());
        } else {
            CmisStoreOperation cmisStoreOperation = getCmisStoreOperation();
            cmisStoreOperation.storeDocument(document, new BinaryContentProducer());
        }
    }

    @Override
    public void updateDocument(DocumentChanges delta) {
        CmisUpdateOperation cmisUpdateOperation = getCmisUpdateOperation();
        cmisUpdateOperation.updateDocument(delta, new BinaryContentProducer());
    }

    @Override
    public boolean removeDocument(String id) {
        return new CmisDeleteOperation(runtimeSnapshot, configuration).removeDocument(id);
    }

    /**
     * Converts CMIS object to JCR node.
     *
     * @param id the identifier of the CMIS object
     * @return JCR node document.
     */
    private Document cmisObject(String id) {
        CmisObject cmisObject = runtimeSnapshot.getCmisObjectFinderUtil().find(id);

        // object does not exist? return null
        if (cmisObject == null) {
            return null;
        }

        CmisGetObjectOperation cmisGetObjectOperation = getCmisGetOperation();

        // converting CMIS object to JCR node
        switch (cmisObject.getBaseTypeId()) {
            case CMIS_FOLDER:
                DocumentWriter document = cmisGetObjectOperation.cmisFolder(cmisObject);
                getCmisSingleVersionOperations().addCachedChildren(id, document);

                return document.document();

            case CMIS_DOCUMENT:
                return cmisGetObjectOperation.cmisDocument(cmisObject, id);
            case CMIS_POLICY:
            case CMIS_RELATIONSHIP:
            case CMIS_SECONDARY:
            case CMIS_ITEM:
        }

        // unexpected object type
        return null;
    }

    /**
     * Translates CMIS repository information into Node.
     *
     * @return node document.
     */
    private Document cmisRepository() {
        RepositoryInfo info = runtimeSnapshot.getSession().getRepositoryInfo();
        DocumentWriter writer = newDocument(ObjectId.toString(ObjectId.Type.REPOSITORY_INFO, ""));

        writer.setPrimaryType(CmisLexicon.REPOSITORY);
        writer.setId(Constants.REPOSITORY_INFO_ID);

        // product name/vendor/version
        writer.addProperty(CmisLexicon.VENDOR_NAME, info.getVendorName());
        writer.addProperty(CmisLexicon.PRODUCT_NAME, info.getProductName());
        writer.addProperty(CmisLexicon.PRODUCT_VERSION, info.getProductVersion());

        return writer.document();
    }


    // -------------------------------- AUXILIARIES -------------------------

    /* regular store operation */
    private CmisStoreOperation getCmisStoreOperation() {
        validateConnectorForErrors();
        return new CmisStoreOperation(runtimeSnapshot, configuration);
    }

    /* regular update operation */
    private CmisUpdateOperation getCmisUpdateOperation() {
        validateConnectorForErrors();
        return new CmisUpdateOperation(runtimeSnapshot, configuration);
    }

    /* universal getChildren op */
    private CmisGetChildrenOperation getCmisGetChildrenOperation() {
        validateConnectorForErrors();
        return new CmisGetChildrenOperation(runtimeSnapshot, configuration);
    }

    /* newObject/store ops combined in a single call - used by singleVersion feature */
    private CmisNewObjectCombinedOperation getCmisNewObjectCombinedOperation() {
        validateConnectorForErrors();
        return new CmisNewObjectCombinedOperation(runtimeSnapshot, configuration);
    }

    /* a set of modified actions and utils/checks to support singleVersion feature */
    private CmisSingleVersionOperations getCmisSingleVersionOperations() {
        validateConnectorForErrors();
        return new CmisSingleVersionOperations(runtimeSnapshot, configuration);
    }

    /* regular newObjectId op */
    private CmisNewObjectOperation getCmisNewObjectOperation() {
        validateConnectorForErrors();
        return new CmisNewObjectOperation(runtimeSnapshot, configuration);
    }

    /*
    * new instance of cmis getObjectOperation
    */
    public CmisGetObjectOperation getCmisGetOperation() {
        validateConnectorForErrors();
        return new CmisGetObjectOperation(runtimeSnapshot, configuration, getUnfiledParentId());
    }

    /**
     * Validate {@link #connectorProblems} for writen errors.
     * In normal state {@link #connectorProblems} must bee <code>null</code>
     *
     * @throws org.modeshape.jcr.federation.spi.ConnectorException with messages from {@link #connectorProblems}
     */
    private void validateConnectorForErrors() {
        if (connectorProblems != null) {
            throw new ConnectorException(connectorProblems);
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


    // ------------------------------ INITIALIZATIONS ----------------------------------


    private Session getSoapCmisConnection(Cache cache) throws IOException, RepositoryException {
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

//        parameter.put(SessionParameter.CACHE_SIZE_OBJECTS, "700");
//        parameter.put(SessionParameter.CACHE_TTL_OBJECTS, "10000");

        if (StringUtils.isNotEmpty(clientPortProvider))
            parameter.put(SessionParameter.WEBSERVICES_JAXWS_IMPL, clientPortProvider);

        SessionFactoryImpl factory = SessionFactoryImpl.newInstance();
        org.apache.chemistry.opencmis.client.runtime.cache.Cache cmisCache = new DistriburedCmisCacheImpl(cache);
        cmisCache.initialize(null, parameter);
        return factory.createSession(parameter, null, new StandardAuthenticationProvider() {
            private static final long serialVersionUID = 1L;

            @Override
            public Element getSOAPHeaders(Object portObject) {
                // Place headers here
                return super.getSOAPHeaders(portObject);
            }

            @Override
            public HandlerResolver getHandlerResolver() {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }
        }, cmisCache);
    }

    private Session getAtomCmisConnection(Cache cache) throws IOException, RepositoryException {
        if (atomPubUrl != null && !atomPubUrl.isEmpty()) {
            Map<String, String> parameter = new HashMap<String, String>();

            // user credentials
            if (StringUtils.isNotEmpty(user) && StringUtils.isNotEmpty(password)) {
                parameter.put(SessionParameter.USER, user);
                parameter.put(SessionParameter.PASSWORD, getPassword());
            }

            // connection settings
            parameter.put(SessionParameter.BINDING_TYPE, BindingType.ATOMPUB.value());
            parameter.put(SessionParameter.ATOMPUB_URL, atomPubUrl);
            parameter.put(SessionParameter.REPOSITORY_ID, repositoryId);

            SessionFactoryImpl factory = SessionFactoryImpl.newInstance();

            org.apache.chemistry.opencmis.client.runtime.cache.Cache cmisCache = new DistriburedCmisCacheImpl(cache);
            cmisCache.initialize(null, parameter);
            return factory.createSession(parameter, null, new StandardAuthenticationProvider() {
                private static final long serialVersionUID = 1L;

                @Override
                public Element getSOAPHeaders(Object portObject) {
                    // Place headers here
                    return super.getSOAPHeaders(portObject);
                }

                @Override
                public HandlerResolver getHandlerResolver() {
                    return null;  //To change body of implemented methods use File | Settings | File Templates.
                }
            }, cmisCache);
        } else {
            return getSoapCmisConnection(cache);
        }
    }


    // ---------------------- LOGGERS --------------------------------------


    // DEBUG
    public void debug(final String... values) {
        StringBuilder sb = new StringBuilder();
        for (String value : values) {
            sb.append(value).append(" ");
        }

        log().info(sb.toString());
    }

    /*
     * this class has access to connector's specific methods
     * pass it to operation for content producing
    */
    public class BinaryContentProducer implements BinaryContentProducerInterface {

        /**
         * Creates content stream using JCR node.
         *
         * @param document JCR node representation
         * @return CMIS content stream object
         */
        public ContentStream jcrBinaryContent(Document document, String fileName) {

            try {
                DocumentReader reader = readDocument(document);
                Document props = document.getDocument("properties").getDocument(JcrLexicon.Namespace.URI);

//                String fileName = props.getString("name");
                String mimeType = props.getString("mimeType");
                if (debug) {
                    log().info("binary: String fileName = props.getString(\"fileName'\" : " + fileName);
                    log().info("binary: String fileName = props.getString(\"mimeType'\" : " + mimeType);
                    log().info("binary: document:: " + document);
                }

                org.modeshape.jcr.value.Property content = reader.getProperty(Constants.JCR_DATA);
                BinaryValue binary = factories().getBinaryFactory().create(content.getFirstValue());
                // create content stream
                return new ContentStreamImpl(fileName, BigInteger.valueOf(binary.getSize()), mimeType, binary.getStream());
            } catch (RepositoryException re) {
                debug("get content RepositoryException ", re.getMessage());
            }

            return null;
        }

    }

    // document producer lazily cached
    ConnectorDocumentProducer getDocumentProducer() {
        return runtimeSnapshot.getDocumentProducer();
    }

    //
    public final class ConnectorDocumentProducer implements DocumentProducer {

        public DocumentWriter getNewDocument(String id) {
            return newDocument(id);
        }

    }


    /**
     * Import given CMIS type to the JCR repository.
     *
     * @param cmisType    cmis object type
     * @param typeManager JCR type manager/
     * @param registry
     * @throws RepositoryException if there is a problem importing the types
     */
    @SuppressWarnings("unchecked")
    public void importType(ObjectType cmisType,
                           NodeTypeManager typeManager,
                           NamespaceRegistry registry) throws RepositoryException {
        // TODO: get namespace information and register
        // registry.registerNamespace(cmisType.getLocalNamespace(), cmisType.getLocalNamespace());

        // create node type template
        NodeTypeTemplate type = typeManager.createNodeTypeTemplate();

        // convert CMIS type's attributes to node type template we have just created
        type.setName(cmisType.getId());
        type.setAbstract(false);
        type.setMixin(false);
        type.setOrderableChildNodes(true);
        type.setQueryable(true);
        if (!cmisType.isBaseType()) {
            type.setDeclaredSuperTypeNames(superTypes(cmisType));
        }

        Map<String, PropertyDefinition<?>> props = cmisType.getPropertyDefinitions();
        Set<String> names = props.keySet();
        // properties
        for (String name : names) {
            PropertyDefinition<?> pd = props.get(name);
            PropertyDefinitionTemplate pt = typeManager.createPropertyDefinitionTemplate();
            pt.setRequiredType(properties.getJcrType(pd.getPropertyType()));
            pt.setAutoCreated(false);
            pt.setAvailableQueryOperators(new String[]{});
            pt.setName(name);
            pt.setMandatory(pd.isRequired());
            type.getPropertyDefinitionTemplates().add(pt);
        }

        // register type
        NodeTypeDefinition[] nodeDefs = new NodeTypeDefinition[]{type};
        typeManager.registerNodeTypes(nodeDefs, true);
    }

    /**
     * Determines supertypes for the given CMIS type in terms of JCR.
     *
     * @param cmisType given CMIS type
     * @return supertypes in JCR lexicon.
     */
    private String[] superTypes(ObjectType cmisType) {
        if (cmisType.getBaseTypeId() == BaseTypeId.CMIS_FOLDER) {
            return new String[]{JcrConstants.NT_FOLDER};
        }

        if (cmisType.getBaseTypeId() == BaseTypeId.CMIS_DOCUMENT) {
            return new String[]{JcrConstants.NT_FILE};
        }

        return new String[]{cmisType.getParentType().getId()};
    }

    public Document getChildReference(String parentKey,
                                      String childKey) {
        debug("Looking for the reference within parent : <" + parentKey + "> and child = <" + childKey + " > ...");
        CmisSingleVersionOperations singleVersionOps = getCmisSingleVersionOperations(); // todo optimize mr

        SingleVersionDocumentsCache singleVersionCache = runtimeSnapshot.getSingleVersionCache();

        if (singleVersionCache.containsKey(childKey) && (singleVersionCache.containsReferences(parentKey) || ObjectId.isUnfiledStorage(parentKey))) {
            TempDocument tempDocument = singleVersionCache.get(childKey);
            return newChildReference(childKey, tempDocument.getName().getLocalName());
        }

        if (ObjectId.isUnfiledStorage(childKey) && (StringUtils.equals(parentKey, getUnfiledParentId()))) {
            return newChildReference(childKey, ObjectId.Type.UNFILED_STORAGE.getValue());
        }
        CmisObject object = runtimeSnapshot.getCmisObjectFinderUtil().find(childKey);
        if (parentKey == null) {
            log().info("you got problem :: getChildReference -> parentKey == null");
        }
        if (object == null && ObjectId.isUnfiledStorage(childKey) && (StringUtils.equals(parentKey, getUnfiledParentId()))) {
            return newChildReference(childKey, ObjectId.Type.UNFILED_STORAGE.getValue());
        }
        String mappedId = runtimeSnapshot.getCmisObjectFinderUtil().getObjectMappingId(object);
        if (!childKey.equals(mappedId))
            log().info("getting reference childKey [" + childKey + "] is not equal to actual mapped id [" + mappedId + "]!!");
        return newChildReference(childKey, object.getName());
    }

    private static Set<String> PROP_MIN_SET = new TreeSet<String>(Collections.singletonList("cmis:objectId"));

    private OperationContext getChildrenQueryOperationContext() {
        return runtimeSnapshot.getSession().createOperationContext(PROP_MIN_SET, false, false, false, IncludeRelationships.NONE, null, false, "cmis:creationDate ASC" /* ?? */, true, Integer.MAX_VALUE);
    }


    private String getUnfiledQueryTemplate() {
        if (unfiledQueryTemplate == null) {
            unfiledQueryTemplate = (StringUtils.isNotEmpty(remoteUnfiledNodeId))
                    ? "select * from cmis:document where " +
                    "IN_FOLDER('" + remoteUnfiledNodeId + "') AND cmis:name='%s'"
                    : "select doc.cmis:objectId, doc.cmis:versionSeriesId, doc.cmis:objectTypeId from cmis:document doc " +
                    "LEFT JOIN ReferentialContainmentRelationship rcr ON document.This=rcr.Head " +
                    "WHERE rcr.Head is NULL AND doc.cmis:name='%s'";
        }

        return unfiledQueryTemplate;
    }


    private Set<String> mappedIdsQueryFilter = null;

    private Set<String> getIDsFilter() {
        if (mappedIdsQueryFilter == null) {
            mappedIdsQueryFilter = new TreeSet<String>();

            mappedIdsQueryFilter.add(PropertyIds.OBJECT_ID);
            mappedIdsQueryFilter.add(PropertyIds.VERSION_SERIES_ID);
            mappedIdsQueryFilter.add(PropertyIds.OBJECT_TYPE_ID);
            if (StringUtils.isNotEmpty(singleVersionOptions.getCommonIdPropertyName()))
                mappedIdsQueryFilter.add(singleVersionOptions.getCommonIdPropertyName());
        }

        return mappedIdsQueryFilter;
    }

    public Document getChildReference(String parentKey, Name childName, int snsIndex) {
        String query = (ObjectId.isUnfiledStorage(parentKey))
                ? String.format(getUnfiledQueryTemplate(), childName.getLocalName())
                : String.format("select * from cmis:document where IN_FOLDER('%s') AND cmis:name='%s'",
                parentKey,
                childName.getLocalName());

        log().info(query + "<<<< ");

        OperationContext ctx = getChildrenQueryOperationContext();
        ctx.setFilter(getIDsFilter());
        ctx.setMaxItemsPerPage(snsIndex);
        ItemIterable<QueryResult> result = runtimeSnapshot.getSession().query(query, false, ctx);

        if (snsIndex > 1)
            result.skipTo(snsIndex - 1);
        log().info(String.format("And I've found <%s> sns Items", result.getTotalNumItems()));

        QueryResult next = result.iterator().next();
        if (next == null)
            return null;

        String mappedId = runtimeSnapshot.getCmisObjectFinderUtil().getObjectMappingId(next);
        return newChildReference(mappedId, childName.getLocalName());
    }

    @Override
    public int getChildCount(String parentKey, Name name) {
        if (snsCommonIndex > -1)
            return snsCommonIndex;

        String query = (ObjectId.isUnfiledStorage(parentKey))
                ? String.format(getUnfiledQueryTemplate(), name.getLocalName())
                : String.format("select cmis:objectId from cmis:document where IN_FOLDER('%s') AND cmis:name='%s'",
                parentKey,
                name.getLocalName());

        //String query = String.format("select cmis:objectId from cmis:document where cmis:name='%s'", name.getLocalName());
        log().info(query + "<<<<    I've been called!!!!!! ");
        OperationContext ctx = getChildrenQueryOperationContext();

        ItemIterable<QueryResult> query1 = runtimeSnapshot.getSession().query(query, false, ctx);
        long totalNumItems = query1.getTotalNumItems();
        log().info(String.format("And I've found <%s> sns Items", totalNumItems));
        return (int) totalNumItems;
    }

    @Override
    public Document getChildren(PageKey pageKey) {
        CmisGetChildrenOperation cmisGetChildrenOperation =
                getCmisGetChildrenOperation();
        DocumentWriter writer = cmisGetChildrenOperation.getChildren(pageKey, newDocument(pageKey.getParentId()));
        return writer.document();
    }

    private String getUnfiledParentId() {
        if (runtimeSnapshot.getCaughtProjectedId() == null) {
            // property might be set after initialize.. -> reset it then
            runtimeSnapshot.setPreconfiguredProjections(preconfiguredProjections);

            if (runtimeSnapshot.getPreconfiguredProjections() == null)
                throw new RuntimeException("Projections are null!!");

            log().info("preconfiguredProjections  ");
            log().info(runtimeSnapshot.getPreconfiguredProjections().toString());

            List<RepositoryConfiguration.ProjectionConfiguration> projectionConfigurations =
                    runtimeSnapshot.getPreconfiguredProjections().values().iterator().next();
            log().info("projectionConfigurations  ");
            log().info(projectionConfigurations.toString());

            RepositoryConfiguration.ProjectionConfiguration projectionConfiguration = projectionConfigurations.get(0);


            String externalPath = projectionConfiguration.getExternalPath();
            String documentId = getDocumentId(externalPath);

            runtimeSnapshot.setCaughtProjectedId(documentId);
        }

        return runtimeSnapshot.getCaughtProjectedId();
    }

    /**
     * Validate the types registered in the system and current types in a remote storage
     *
     * @return {@link org.modeshape.common.collection.Problems} The problems encountered in type checking
     */
    public Problems getSelfCheckStatus() {

        log().info("Started self check in CmisConnector!");
        Problems problems;

        Map<String, ObjectType> cachedTypes = getCachedTypeDefinitions();

        List<Tree<ObjectType>> storageTypeDescendants;
        try {
            storageTypeDescendants = runtimeSnapshot.getSession().getTypeDescendants(null, Integer.MAX_VALUE, true);
            Map<String, ObjectType> storageTypes = new HashMap<String, ObjectType>();
            for (Tree<ObjectType> tree : storageTypeDescendants) {
                putObjectTypesToMap(tree, storageTypes);
            }
            problems = compareTypeDefinitions(cachedTypes, storageTypes);
        } catch (CmisRuntimeException cre) {
            if (cre.getCode().intValue() == 0) {
                problems = new SimpleProblems();
                problems.addError(CompareTypesI18n.repositoryException, cre.getMessage());
            } else {
                throw cre;
            }
        }
        setConnectorProblemsIfErrors(problems);
        return problems;
    }

    /**
     * Set to {@link #connectorProblems} messages from funded problems for {@link #getSelfCheckStatus()}
     * without connection problem
     *
     * @param problems current problems
     */
    private void setConnectorProblemsIfErrors(Problems problems) {
        if (problems.hasErrors()) {
            StringBuilder message = new StringBuilder();
            for (Problem problem : problems) {
                boolean isError = problem.getStatus() == Problem.Status.ERROR;

                if (isError) {
                    boolean isRepoException = problem.getMessage() == CompareTypesI18n.repositoryException;
                    if (!isRepoException) {
                        message.append(problem.getMessageString()).append("\n");
                    }
                }
            }

            String errors = message.toString();
            if (!errors.isEmpty()) {
                connectorProblems = errors;
                log().info("Found problems in connector: " + connectorProblems);
            }
        } else {
            connectorProblems = null;
        }
    }

    /**
     * Convert {@link org.apache.chemistry.opencmis.client.api.Tree} of {@link org.apache.chemistry.opencmis.client.api.ObjectType}
     * to {@link java.util.Map} witch has typeId as key and {@link org.apache.chemistry.opencmis.client.api.ObjectType} as value
     *
     * @param typeTree types to convert
     * @param types    result types map
     */
    private void putObjectTypesToMap(Tree<ObjectType> typeTree, Map<String, ObjectType> types) {
        ObjectType type = typeTree.getItem();
        List<Tree<ObjectType>> childrenTree = typeTree.getChildren();
        types.put(type.getId(), type);
        if (childrenTree != null && childrenTree.size() > 0) {
            for (Tree<ObjectType> tree : childrenTree) {
                putObjectTypesToMap(tree, types);
            }
        }
    }

    public LanguageDialect getLanguageDialect() {
        return runtimeSnapshot.getLanguageDialect();
    }

}
