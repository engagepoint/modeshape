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

import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.client.bindings.spi.StandardAuthenticationProvider;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.data.RepositoryInfo;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.ContentStreamImpl;
import org.apache.commons.lang3.StringUtils;
import org.infinispan.schematic.document.Document;
import org.modeshape.connector.cmis.config.TypeCustomMappingList;
import org.modeshape.connector.cmis.features.SingleVersionDocumentsCache;
import org.modeshape.connector.cmis.features.SingleVersionOptions;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.modeshape.connector.cmis.operations.BinaryContentProducerInterface;
import org.modeshape.connector.cmis.operations.CmisObjectFinderUtil;
import org.modeshape.connector.cmis.operations.DocumentProducer;
import org.modeshape.connector.cmis.operations.impl.*;
import org.modeshape.connector.cmis.util.CryptoUtils;
import org.modeshape.jcr.JcrLexicon;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.federation.spi.*;
import org.modeshape.jcr.value.BinaryValue;
import org.modeshape.jcr.value.Name;
import org.w3c.dom.Element;

import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;
import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.util.*;

import static org.modeshape.connector.cmis.operations.impl.CmisOperationCommons.asDocument;
import static org.modeshape.connector.cmis.operations.impl.CmisOperationCommons.isDocument;

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
public class CmisConnector extends Connector implements Pageable, UnfiledSupportConnector {

    private CmisObjectFinderUtil cmisObjectFinderUtil;
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
    private TypeCustomMappingList customMapping = new TypeCustomMappingList();
    // to not reset required properties on document create
    private boolean ignoreEmptyPropertiesOnCreate = false;
    // add required properties to a document if not present
    private boolean addRequiredPropertiesOnRead = false;
    // unfiled types binding
    private String[] applicableUnfiledTypes;
    // id of remote unfiled storage if any
    // for FileNet normally = NULL
    private String remoteUnfiledNodeId;
    // debug
    private boolean debug = false;
    // single version && commonId logic
    private SingleVersionOptions singleVersionOptions = new SingleVersionOptions();

    // -----  runtime variables -------------
    // id of the first projected folder
    private Session session;
    private String caughtProjectedId = null; // todo review
    private LocalTypeManager localTypeManager;
    private SingleVersionDocumentsCache singleVersionCache = new SingleVersionDocumentsCache();
    // local document producer instance
    private ConnectorDocumentProducer documentProducer = new ConnectorDocumentProducer();

    public CmisConnector() {
        super();
    }

    @Override
    public Collection<Name> getApplicableUnfiledTypes() {
        return localTypeManager.getApplicableTypesInstance();
    }

    // required by the custom query processor
    public Session getSession() {
        return session;
    }

    @Override
    public boolean isReadonly() {
        return false;
    }

    @Override
    public void initialize(NamespaceRegistry registry,
                           NodeTypeManager nodeTypeManager) throws RepositoryException, IOException {
        super.initialize(registry, nodeTypeManager);
        // setup CMIS connection
        this.session = getCmisConnection();
        // create types container
        this.localTypeManager = new LocalTypeManager(
                getContext().getValueFactories(),
                registry, nodeTypeManager,
                customMapping);
        // register external types into JCR
        localTypeManager.initialize(session, applicableUnfiledTypes);
        // Single version saving feature
        singleVersionOptions.initialize(factories(), nodeTypeManager);
        // extended getObject logic
        cmisObjectFinderUtil = new CmisObjectFinderUtil(session, singleVersionOptions);
    }


    @Override
    public Document getDocumentById(String id) {
        // object id is a composite key which holds information about
        // unique object identifier and about its type
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
                return cmisGetObjectOperation.jcrUnfiled(id, caughtProjectedId);
            case CONTENT:
                // in the jcr domain content is represented by child node of
                // the nt:file node while in cmis domain it is a property of
                // the cmis:document object. This action searches original
                // cmis:document and converts its content property into jcr node
                // result object should have same id as requested
                return cmisGetObjectOperation.cmisContent(objectId.getIdentifier());
            case OBJECT:
                // converts cmis folders and documents into jcr folders and files
                return cmisObject(objectId.getIdentifier());

            default:
                return null;
        }
    }

    /*
    * look up local temp storage for proposed id
    */
    public boolean isCachedTempDocument(String id) {
        return singleVersionCache.containsKey(id);
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
                return cmisObjectFinderUtil.find(objectId.getIdentifier()) != null;
            default:
                // here we checking cmis:folder and cmis:document
                return cmisObjectFinderUtil.find(id) != null;
        }
    }


    // --------------- id/path resolutions ----------------

    @Override
    public String getDocumentId(String path) {
        // establish relation between path and object identifier
        String id = session.getObjectByPath(path).getId();
        // try to catch and save first projection's folderId to stick unfiled to it..
        if (caughtProjectedId == null) caughtProjectedId = id;
        // what if 1st projection is not up but second is ok ?? will we get there to get Id of the first one ?

        return id;
    }

    @Override
    public Collection<String> getDocumentPathsById(String id) {
        CmisObject obj = session.getObject(id);

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
        return new CmisDeleteOperation(session, localTypeManager, cmisObjectFinderUtil).removeDocument(id);
    }

    /**
     * Converts CMIS object to JCR node.
     *
     * @param id the identifier of the CMIS object
     * @return JCR node document.
     */
    private Document cmisObject(String id) {
        CmisObject cmisObject = cmisObjectFinderUtil.find(id);

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
        RepositoryInfo info = session.getRepositoryInfo();
        DocumentWriter writer = newDocument(ObjectId.toString(ObjectId.Type.REPOSITORY_INFO, ""));

        writer.setPrimaryType(CmisLexicon.REPOSITORY);
        writer.setId(Constants.REPOSITORY_INFO_ID);

        // product name/vendor/version
        writer.addProperty(CmisLexicon.VENDOR_NAME, info.getVendorName());
        writer.addProperty(CmisLexicon.PRODUCT_NAME, info.getProductName());
        writer.addProperty(CmisLexicon.PRODUCT_VERSION, info.getProductVersion());

        return writer.document();
    }

    public Document getChildReference(String parentKey,
                                      String childKey) {
        debug("Looking for the reference within parent : <" + parentKey + "> and child = <" + childKey + " > ...");
        CmisObject object = cmisObjectFinderUtil.find(childKey);
        return newChildReference(object.getId(), object.getName());
    }

    @Override
    public Document getChildren(PageKey pageKey) {
        CmisGetChildrenOperation cmisGetChildrenOperation =
                getCmisGetChildrenOperation();
        DocumentWriter writer = cmisGetChildrenOperation.getChildren(pageKey, newDocument(pageKey.getParentId()));
        return writer.document();
    }

    // -------------------------------- AUXILIARIES -------------------------

    /* regular store operation */
    private CmisStoreOperation getCmisStoreOperation() {
        return new CmisStoreOperation(session, localTypeManager, ignoreEmptyPropertiesOnCreate, cmisObjectFinderUtil);
    }

    /* regular update operation */
    private CmisUpdateOperation getCmisUpdateOperation() {
        return new CmisUpdateOperation(session, localTypeManager, ignoreEmptyPropertiesOnCreate, cmisObjectFinderUtil);
    }

    /* universal getChildren op */
    private CmisGetChildrenOperation getCmisGetChildrenOperation() {
        return new CmisGetChildrenOperation(session, localTypeManager, remoteUnfiledNodeId, singleVersionOptions, cmisObjectFinderUtil);
    }

    /* newObject/store ops combined in a single call - used by singleVersion feature */
    private CmisNewObjectCombinedOperation getCmisNewObjectCombinedOperation() {
        return new CmisNewObjectCombinedOperation(session, localTypeManager,
                singleVersionOptions,
                ignoreEmptyPropertiesOnCreate, cmisObjectFinderUtil);
    }

    /* a set of modified actions and utils/checks to support singleVersion feature */
    private CmisSingleVersionOperations getCmisSingleVersionOperations() {
        return new CmisSingleVersionOperations(session, localTypeManager,
                cmisObjectFinderUtil, singleVersionOptions, singleVersionCache, getDocumentProducer());
    }

    /* regular newObjectId op */
    private CmisNewObjectOperation getCmisNewObjectOperation() {
        return new CmisNewObjectOperation(session, localTypeManager, cmisObjectFinderUtil);
    }

    /*
    * new instance of cmis getObjectOperation
    */
    private CmisGetObjectOperation getCmisGetOperation() {
        return new CmisGetObjectOperation(
                session, localTypeManager,
                addRequiredPropertiesOnRead,
                hideRootFolderReference,
                caughtProjectedId,
                remoteUnfiledNodeId,
                singleVersionOptions,
                getDocumentProducer(), cmisObjectFinderUtil
        );
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


    private Session getCmisConnection() throws IOException, RepositoryException {
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

        return factory.createSession(parameter, null, new StandardAuthenticationProvider() {
            private static final long serialVersionUID = 1L;

            @Override
            public Element getSOAPHeaders(Object portObject) {
                // Place headers here
                return super.getSOAPHeaders(portObject);
            }
        }, null);
    }


    // ---------------------- LOGGERS --------------------------------------


    // DEBUG
    public void debug(String... values) {
        if (debug) {
            StringBuilder sb = new StringBuilder();
            for (String value : values) {
                sb.append(value).append(" ");
            }
            if (getLogger() != null) {
                getLogger().debug(sb.toString());
            } else if (log() != null /* simple logger */) {
                log().debug(sb.toString());
            } else {
                System.out.println(sb.toString());
            }
        }
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
        public ContentStream jcrBinaryContent(Document document) {

            try {
                DocumentReader reader = readDocument(document);
                Document props = document.getDocument("properties").getDocument(JcrLexicon.Namespace.URI);

                String fileName = props.getString("fileName");
                String mimeType = props.getString("mimeType");

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
        return documentProducer;
    }

    //
    public final class ConnectorDocumentProducer implements DocumentProducer {

        public DocumentWriter getNewDocument(String id) {
            return newDocument(id);
        }

    }
}
