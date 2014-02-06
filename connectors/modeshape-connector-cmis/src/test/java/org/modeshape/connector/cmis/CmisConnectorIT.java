/*
 * ModeShape (http://www.modeshape.org)
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * See the AUTHORS.txt file in the distribution for a full listing of
 * individual contributors.
 *
 * Unless otherwise indicated, all code in ModeShape is licensed
 * to you under the terms of the GNU Lesser General Public License as
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

import junit.framework.Assert;
import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.modeshape.common.util.FileUtil;
import org.modeshape.jcr.MultiUseAbstractTest;
import org.modeshape.jcr.RepositoryConfiguration;
import org.modeshape.jcr.api.Workspace;

import javax.jcr.*;
import javax.jcr.Property;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.PropertyDefinition;
import java.io.*;
import java.util.*;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Provide integration testing of the CMIS connector with OpenCMIS InMemory Repository.
 * 
 * @author Alexander Voloshyn
 * @author Nick Knysh
 * @version 1.0 2/20/2013
 */
@Ignore
public class CmisConnectorIT extends MultiUseAbstractTest {
    /**
     * Test OpenCMIS InMemory Server URL.
     * <p/>
     * This OpenCMIS InMemory server instance should be started by maven cargo plugin at pre integration stage.
     */
    private static final String CMIS_URL = "http://localhost:8090/";
    public static final String DEFAULT_BINARY_CONTENT = "Hello World";
    private static Logger logger = Logger.getLogger(CmisConnectorIT.class);
    private static Session cmisDirectSession;

    @BeforeClass
    public static void beforeAll() throws Exception {
        FileUtil.delete("target/federation_persistent_repository");
        RepositoryConfiguration config = RepositoryConfiguration.read("config/repository-1.json");
        startRepository(config);

        // waiting when CMIS repository will be ready
        boolean isReady = false;

        // max time for waiting in milliseconds
        long maxTime = 30000L;

        // actually waiting time in milliseconds
        long waitingTime = 0L;

        // time quant in milliseconds
        long timeQuant = 500L;

        logger.info("Waiting for CMIS repository...");
        do {
            try {
                cmisDirectSession = testDirectChemistryConnect();
                isReady = true;
            } catch (Exception e) {
                Thread.sleep(timeQuant);
                waitingTime += timeQuant;
            }
        } while (!isReady && waitingTime < maxTime);

        // checking status
        if (!isReady) {
            throw new IllegalStateException("CMIS repository did not respond withing " + maxTime + " milliseconds");
        }
        logger.info("CMIS repository has been started successfully");
    }

    @AfterClass
    public static void afterAll() throws Exception {
        MultiUseAbstractTest.afterAll();
    }

    public static Session testDirectChemistryConnect() {
        // default factory implementation
        SessionFactory factory = SessionFactoryImpl.newInstance();
        Map<String, String> parameter = new HashMap<String, String>();

        // connection settings
        parameter.put(SessionParameter.BINDING_TYPE, BindingType.WEBSERVICES.value());
        parameter.put(SessionParameter.WEBSERVICES_ACL_SERVICE, CMIS_URL + "services/ACLService?wsdl");
        parameter.put(SessionParameter.WEBSERVICES_DISCOVERY_SERVICE, CMIS_URL + "services/DiscoveryService?wsdl");
        parameter.put(SessionParameter.WEBSERVICES_MULTIFILING_SERVICE, CMIS_URL + "services/MultiFilingService?wsdl");
        parameter.put(SessionParameter.WEBSERVICES_NAVIGATION_SERVICE, CMIS_URL + "services/NavigationService?wsdl");
        parameter.put(SessionParameter.WEBSERVICES_OBJECT_SERVICE, CMIS_URL + "services/ObjectService10?wsdl");
        parameter.put(SessionParameter.WEBSERVICES_POLICY_SERVICE, CMIS_URL + "services/PolicyService?wsdl");
        parameter.put(SessionParameter.WEBSERVICES_RELATIONSHIP_SERVICE, CMIS_URL + "services/RelationshipService?wsdl");
        parameter.put(SessionParameter.WEBSERVICES_REPOSITORY_SERVICE, CMIS_URL + "services/RepositoryService10?wsdl");
        parameter.put(SessionParameter.WEBSERVICES_VERSIONING_SERVICE, CMIS_URL + "services/VersioningService?wsdl");
        // Default repository id for in memory server is A1
        parameter.put(SessionParameter.REPOSITORY_ID, "A1");

        // create session
        final Session session = factory.createSession(parameter);
        assertTrue("Chemistry session should exists.", session != null);
        return session;
    }

    @Test 
    public void shouldSeeCmisTypesAsJcrTypes() throws Exception {
        NodeTypeManager manager = getSession().getWorkspace().getNodeTypeManager();

        NodeTypeIterator it = manager.getNodeType("nt:file").getDeclaredSubtypes();
        while (it.hasNext()) {
            NodeType nodeType = it.nextNodeType();
            assertTrue(nodeType != null);
        }
    }

    @Test 
    public void shouldAccessRootFolder() throws Exception {
        Node root = getSession().getNode("/cmis");
        assertTrue(root != null);
    }

    @Test 
    public void testRootFolderName() throws Exception {
        Node root = getSession().getNode("/cmis");
        assertEquals("cmis", root.getName());
    }

    @Test 
    public void shouldAccessRepositoryInfo() throws Exception {
        Node repoInfo = getSession().getNode("/cmis/repositoryInfo");
        // Different Chemistry versions return different things ...
        assertTrue(repoInfo.getProperty("cmis:productName").getString().contains("OpenCMIS"));
        assertTrue(repoInfo.getProperty("cmis:productName").getString().contains("InMemory"));
        assertEquals("Apache Chemistry", repoInfo.getProperty("cmis:vendorName").getString());
        assertTrue(repoInfo.getProperty("cmis:productVersion").getString() != null);
    }

    @Test 
    public void shouldAccessFolderByPath() throws Exception {
        Node root = getSession().getNode("/cmis");
        assertTrue(root != null);

        Node node1 = getSession().getNode("/cmis/My_Folder-0-0");
        assertTrue(node1 != null);

        Node node2 = getSession().getNode("/cmis/My_Folder-0-0/My_Folder-1-0");
        assertTrue(node2 != null);

        Node node3 = getSession().getNode("/cmis/My_Folder-0-0/My_Folder-1-0/My_Folder-2-0");
        assertTrue(node3 != null);
    }

    @Test 
    public void shouldAccessDocumentPath() throws Exception {
        Node file = getSession().getNode("/cmis/My_Folder-0-0/My_Document-1-0");
        assertTrue(file != null);
    }

    @Test 
    public void shouldAccessBinaryContent() throws Exception {
        Node file = getSession().getNode("/cmis/My_Folder-0-0/My_Document-1-0");
        Node cnt = file.getNode("jcr:content");

        Property value = cnt.getProperty("jcr:data");

        Binary bv = value.getValue().getBinary();
        InputStream is = bv.getStream();

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        int b = 0;
        while (b != -1) {
            b = is.read();
            if (b != -1) {
                bout.write(b);
            }
        }

        byte[] content = bout.toByteArray();
        String s = new String(content, 0, content.length);

        assertFalse("Content shouldn't be empty.", s.trim().isEmpty());
    }

    // -----------------------------------------------------------------------/
    // Folder cmis build-in properties
    // -----------------------------------------------------------------------/
    @Test 
    public void shouldAccessObjectIdPropertyForFolder() throws Exception {
        Node node = getSession().getNode("/cmis/My_Folder-0-0");
        System.out.println(node);
        String objectId = node.getProperty("jcr:uuid").getString();
        assertTrue(objectId != null);
    }

    @Test 
    public void shouldAccessNamePropertyForFolder() throws Exception {
        Node node = getSession().getNode("/cmis/My_Folder-0-0");
        String name = node.getName();
        assertEquals("My_Folder-0-0", name);
    }

    @Test 
    public void shouldAccessCreatedByPropertyForFolder() throws Exception {
        Node node = getSession().getNode("/cmis/My_Folder-0-0");
        String name = node.getProperty("jcr:createdBy").getString();
        assertEquals("unknown", name);
    }

    @Test 
    public void shouldAccessCreationDatePropertyForFolder() throws Exception {
        Node node = getSession().getNode("/cmis/My_Folder-0-0");
        Calendar date = node.getProperty("jcr:created").getDate();
        assertTrue(date != null);
    }

     //@Test
    public void shouldAccessModificationDatePropertyForFolder() throws Exception {
        Node node = getSession().getNode("/cmis/My_Folder-0-0");
        Calendar date = node.getProperty("jcr:lastModified").getDate();
        assertTrue(date != null);
    }

    // -----------------------------------------------------------------------/
    // Document cmis build-in properties
    // -----------------------------------------------------------------------/
    @Test 
    public void shouldAccessObjectIdPropertyForDocument() throws Exception {
        Node node = getSession().getNode("/cmis/My_Folder-0-0/My_Document-1-0");
        String objectId = node.getProperty("jcr:uuid").getString();
        assertTrue(objectId != null);
    }

    @Test 
    public void shouldAccessCreatedByPropertyForDocument() throws Exception {
        Node node = getSession().getNode("/cmis/My_Folder-0-0/My_Document-1-0");
        String name = node.getProperty("jcr:createdBy").getString();
        assertEquals("unknown", name);
    }

    @Test 
    public void shouldAccessCreationDatePropertyForDocument() throws Exception {
        Node node = getSession().getNode("/cmis/My_Folder-0-0/My_Document-1-0");
        Calendar date = node.getProperty("jcr:created").getDate();
        assertTrue(date != null);
    }

    @Test 
    public void shouldCreateFolderAndDocument() throws Exception {
        Node root = getSession().getNode("/cmis");

        String name = "test" + System.currentTimeMillis();
        Node node = root.addNode(name, "nt:folder");
        getSession().save();
        assertTrue(name.equals(node.getName()));
        // node.setProperty("name", "test-name");
        root = getSession().getNode("/cmis/" + name);
        Node node1 = root.addNode("test-1", "nt:file");
        // System.out.println("Test: creating binary content");
        byte[] content = "Hello World".getBytes();
        ByteArrayInputStream bin = new ByteArrayInputStream(content);
        bin.reset();

        // System.out.println("Test: creating content node");
        Node contentNode = node1.addNode("jcr:content", "nt:resource");
        Binary binary = session.getValueFactory().createBinary(bin);
        contentNode.setProperty("jcr:data", binary);
        contentNode.setProperty("jcr:lastModified", Calendar.getInstance());

        // System.out.println("Test: trying to save");
        getSession().save();
        // System.out.println("Test: checking result");

    }

//    @Test  //cmis:contentStreamMimeType - ?
    public void shouldModifyDocument() throws Exception {
        Node file = getSession().getNode("/cmis/My_Folder-0-0/My_Document-1-0");
        PropertyIterator it = file.getProperties();
        while (it.hasNext()) {
            Object val = it.nextProperty();
            printMessage("property=>" + val);
        }
        file.setProperty("StringProp", "modeshape");
        getSession().save();
    }


    @Test 
    public void shouldCreatePrefixedType() throws Exception {
        Node node = getSession().getRootNode().addNode("prefixedTypeFolder", "testing:prefixedFolderType");
        getSession().save();
        Node persistedNode = getSession().getNode(node.getPath());
        Property property = persistedNode.getProperty("jcr:primaryType");
        assertEquals("testing:prefixedFolderType", property.getValue().getString());
    }

    @Test 
    public void shouldSavePrefixedProperties() throws Exception {
        String propertyId = "testing:artist";
        Node node = getSession().getRootNode().addNode("prefixedTypeFolderWithProperties", "testing:prefixedFolderType");
        node.setProperty(propertyId, "The Artist");
        getSession().save();
        // saved property
        Node persistedNode = getSession().getNode(node.getPath());
        Property property = persistedNode.getProperty(propertyId);
        assertEquals("The Artist", property.getValue().getString());
        //update
        node.setProperty(propertyId, "An artist");
        getSession().save();
        // updated property
        Property propertyUpdated = getSession().getNode(node.getPath()).getProperty(propertyId);
        assertEquals("An artist", propertyUpdated.getValue().getString());
    }

    @Test 
    public void shouldRegisterNamespace() throws Exception {
        String testingNsUri = getSession().getWorkspace().getNamespaceRegistry().getURI("testing");
        assertEquals("http://org.modeshape/testing", testingNsUri);
    }

    @Test 
    public void shouldAddVersioning() throws Exception {
        NodeType nodeType = getSession().getWorkspace().getNodeTypeManager().getNodeType("testing:prefixedType");
        assertTrue(!nodeType.isNodeType(NodeType.MIX_VERSIONABLE) && !nodeType.isNodeType(NodeType.MIX_SIMPLE_VERSIONABLE));
    }

    @Test 
    public void shouldRegisterPrimaryTypes() throws Exception {
        org.modeshape.jcr.api.nodetype.NodeTypeManager nodeTypeManager = getSession().getWorkspace().getNodeTypeManager();
        assertFalse(nodeTypeManager.getNodeType("testing:prefixedType").isMixin());
        assertFalse(nodeTypeManager.getNodeType("audioFile").isMixin());
    }

    @Test 
    public void shouldCreateUnVersionedDocument() throws Exception {
        Node root = getSession().getRootNode();
        Node node1 = root.addNode("test-versioned-doc-1", "testing:prefixedType");
        byte[] content = "Hello World".getBytes();
        ByteArrayInputStream bin = new ByteArrayInputStream(content);
        bin.reset();

        Node contentNode = node1.addNode("jcr:content", "nt:resource");
        Binary binary = session.getValueFactory().createBinary(bin);
        contentNode.setProperty("jcr:data", binary);
        contentNode.setProperty("jcr:lastModified", Calendar.getInstance());

        getSession().save();

        NodeType primaryNodeType = getSession().getNode(node1.getPath()).getPrimaryNodeType();
        assertTrue("Node has no versionable mixin but should.",
                (!primaryNodeType.isNodeType(NodeType.MIX_SIMPLE_VERSIONABLE)
                        && !primaryNodeType.isNodeType(NodeType.MIX_VERSIONABLE)));
    }

//    @Test 
    public void shouldCreateRemappedType() throws Exception {
        Node root = getSession().getNode("/cmis");

        String propTitle = "Title_";
        String propTrack = "TrAcK_";
        // mapping MyDocType2.8
        String fileName = "testFile_" + Long.toString((new Date()).getTime());
        String mappedNodeType = "MyDocType2.8_remapped";
        NodeType nodeType = getSession().getWorkspace().getNodeTypeManager().getNodeType(mappedNodeType);
        System.out.println("Type definition: " + mappedNodeType);

        for (PropertyDefinition propertyDefinition : nodeType.getPropertyDefinitions()) {
            System.out.println("Property: " + propertyDefinition.getName());
        }
        System.out.println(" - - - - - - Type definition end  - - - - - -");

        Node node1 = root.addNode(fileName, mappedNodeType);

        byte[] content = "Hello World".getBytes();
        ByteArrayInputStream bin = new ByteArrayInputStream(content);
        bin.reset();

        Node contentNode = node1.addNode("jcr:content", "nt:resource");
        Binary binary = session.getValueFactory().createBinary(bin);
        contentNode.setProperty("jcr:data", binary);
        contentNode.setProperty("jcr:lastModified", Calendar.getInstance());
        getSession().save();

        Node node = getSession().getNode(node1.getPath());
        assertTrue(node.isNodeType(mappedNodeType));
    }

//    @Test 
    public void shouldCreateRemappedTypeWithPrefix() throws Exception {
        Node root = getSession().getNode("/cmis");

        String propTitle = "testing:Title_";
        String propTrack = "TrAcK_";
        String propTags = "multiTags";
        // mapping testing:ViDeoFiLe
        String fileName = "testFile_" + Long.toString((new Date()).getTime());
        String mappedNodeType = "testing:ViDeoFiLe";
        NodeType nodeType = getSession().getWorkspace().getNodeTypeManager().getNodeType(mappedNodeType);
        System.out.println("Type definition: " + mappedNodeType);

        for (PropertyDefinition propertyDefinition : nodeType.getPropertyDefinitions()) {
            System.out.print("Property: [" + propertyDefinition.getName());
            System.out.print("] : <" + PropertyType.nameFromValue(propertyDefinition.getRequiredType()) + ">");
            System.out.print("; isMultiple: " + propertyDefinition.isMultiple());
            System.out.println();
        }
        System.out.println(" - - - - - - Type definition end  - - - - - -");

        Node node1 = root.addNode(fileName, mappedNodeType);
        node1.setProperty(propTitle, "title title");
        node1.setProperty(propTrack, (Integer) 4);
        node1.setProperty(propTags, new String[]{"1", "3"});

        byte[] content = "Hello World".getBytes();
        ByteArrayInputStream bin = new ByteArrayInputStream(content);
        bin.reset();

        Node contentNode = node1.addNode("jcr:content", "nt:resource");
        Binary binary = session.getValueFactory().createBinary(bin);
        contentNode.setProperty("jcr:data", binary);
        contentNode.setProperty("jcr:lastModified", Calendar.getInstance());
        getSession().save();

        Node node;
//         node = getSession().getNode(node1.getPath());
//        node.setProperty(propTags, new String[]{"1", "2"});

//        getSession().save();

        node = getSession().getNode(node1.getPath());
        assertTrue(node.isNodeType(mappedNodeType));
        assertEquals("title title", node.getProperty(propTitle).getString());
        assertTrue("property " + propTrack + " should present", node.hasProperty(propTrack));
        assertEquals(4, node.getProperty(propTrack).getLong());
        assertTrue(node.hasProperty(propTags));
        System.out.println(node.getProperty(propTags).getString());
    }

    @Test 
    public void shouldCreateMultiTypeWithPrefix() throws Exception {
        Node root = getSession().getNode("/cmis");

        String pFrom = "from";
        String pTo = "to";
        String fileName = "testFile_ed_" + Long.toString((new Date()).getTime());
        String mappedNodeType = "emailDocument";
        NodeType nodeType = getSession().getWorkspace().getNodeTypeManager().getNodeType(mappedNodeType);
        System.out.println("Type definition: " + mappedNodeType);

        for (PropertyDefinition propertyDefinition : nodeType.getPropertyDefinitions()) {
            System.out.print("Property: [" + propertyDefinition.getName());
            System.out.print("] : <" + PropertyType.nameFromValue(propertyDefinition.getRequiredType()) + ">");
            System.out.print("; isMultiple: " + propertyDefinition.isMultiple());
            System.out.println();
        }
        System.out.println(" - - - - - - Type definition end  - - - - - -");

        Node node1 = root.addNode(fileName, mappedNodeType);
        node1.setProperty(pFrom, "title title");
        node1.setProperty(pTo, new String[]{"1sefsefs", "fsf3"});
        node1.setProperty("cc", new String[]{"cc1"});
        node1.setProperty("bcc", "bcc1");

        byte[] content = "Hello World".getBytes();
        ByteArrayInputStream bin = new ByteArrayInputStream(content);
        bin.reset();

        Node contentNode = node1.addNode("jcr:content", "nt:resource");
        Binary binary = session.getValueFactory().createBinary(bin);
        contentNode.setProperty("jcr:data", binary);
        contentNode.setProperty("jcr:lastModified", Calendar.getInstance());
        getSession().save();

        Node node;
//         node = getSession().getNode(node1.getPath());
//        node.setProperty(propTags, new String[]{"1", "2"});

//        getSession().save();

        node = getSession().getNode(node1.getPath());
        assertTrue(node.isNodeType(mappedNodeType));
        assertEquals("title title", node.getProperty(pFrom).getString());
        assertTrue("property " + pTo + " should present", node.hasProperty(pTo));
        assertTrue("property cc should present", node.hasProperty("cc"));
        assertTrue("property bcc should present", node.hasProperty("bcc"));
        assertEquals(2, node.getProperty(pTo).getValues().length);
        assertEquals(1, node.getProperty("cc").getValues().length);
        System.out.println("assertEquals(1, node.getProperty(\"to\").().length); " + node.getProperty("to").toString());
        System.out.println("assertEquals(1, node.getProperty(\"cc\").().length); " + node.getProperty("cc").toString());
        System.out.println("assertEquals(1, node.getProperty(\"bcc\").().length); " + node.getProperty("bcc").toString());
//        assertEquals(1, node.getProperty("bcc").().length);
    }

    @Test 
    public void testRename() throws Exception{
        Node root = getSession().getNode("/cmis");

        // Create node
        String fileName       = "rename_testFile_" + Long.toString((new Date()).getTime());
        String mappedNodeType = "emailDocument";

        Node node1 = root.addNode(fileName, mappedNodeType);
        node1.addMixin("mix:referenceable");

        byte[] content = "Hello World".getBytes();
        ByteArrayInputStream bin = new ByteArrayInputStream(content);
        bin.reset();

        Node contentNode = node1.addNode("jcr:content", "nt:resource");
        Binary binary = getSession().getValueFactory().createBinary(bin);

        contentNode.setProperty("jcr:data", binary);
        contentNode.setProperty("jcr:lastModified", Calendar.getInstance());

        getSession().save();

        // Rename node
        Node node;

        node = getSession().getNodeByIdentifier(node1.getIdentifier());

        String nameUpdated = "updated_" + fileName;
        String newPath = node.getParent().getPath() + "/" + nameUpdated;

        getSession().move(node.getPath(), newPath);
        getSession().save();

        node = getSession().getNodeByIdentifier(node.getIdentifier());

        assertEquals(nameUpdated, node.getName());
    }

    void pt(String... values){
        for (String value : values) {
            System.out.print(value);
            System.out.print(" ");
        }
        System.out.println();
    }

    void pnt(Node node) throws RepositoryException {
        pt("Node stree ....");
        printNodeTree(node);
        pt("....Node stree ||");
    }

    void printNodeTree(Node node) throws RepositoryException {
        pt(node.toString());
        NodeIterator nodes = node.getNodes();
        while (nodes.hasNext()) {
            printNodeTree(nodes.nextNode());
        }
    }

//    @Test   // fails due to bug in modeshape deepCopy method of Node
    public void testCopy() throws Exception {
        Node root = getSession().getNode("/cmis");

        String fileName = "testFile_cp_" + Long.toString((new Date()).getTime());
        String mappedNodeType = "emailDocument";

        Node node1 = root.addNode(fileName, mappedNodeType);
        node1.addMixin("mix:referenceable");
        byte[] content = "Hello World".getBytes();
        ByteArrayInputStream bin = new ByteArrayInputStream(content);
        bin.reset();

        Node contentNode = node1.addNode("jcr:content", "nt:resource");
        contentNode.addMixin("mix:referenceable");
        Binary binary = session.getValueFactory().createBinary(bin);
        contentNode.setProperty("jcr:data", binary);
        contentNode.setProperty("jcr:lastModified", Calendar.getInstance());
        getSession().save();

        Node node = getSession().getNodeByIdentifier(node1.getIdentifier());
//        pnt(getSession().getNode("/cmis"));
        getSession().getWorkspace().copy(node.getPath(), node.getPath() + "_copy");
        getSession().save();
    }

    /*@Test 
    public void testFSCopy() throws Exception {
        Node root = getSession().getNode("/cmis");

        String fileName = "testFile_cp_" + Long.toString((new Date()).getTime());
        String mappedNodeType = "nt:file";

        Node node1 = root.addNode(fileName, mappedNodeType);
        node1.addMixin("mix:referenceable");
        byte[] content = "Hello World".getBytes();
        ByteArrayInputStream bin = new ByteArrayInputStream(content);
        bin.reset();

        Node contentNode = node1.addNode("jcr:content", "nt:resource");
        contentNode.addMixin("mix:referenceable");
        Binary binary = session.getValueFactory().createBinary(bin);
        contentNode.setProperty("jcr:data", binary);
        contentNode.setProperty("jcr:lastModified", Calendar.getInstance());
        getSession().save();

        Node node = getSession().getNodeByIdentifier(node1.getIdentifier());
//        pnt(getSession().getNode("/cmis"));
        getSession().getWorkspace().copy(node.getPath(), node.getPath() + "_copy");
        getSession().save();
    }*/

    public Node addFile(Node targetNode, String fileName, String fileType) throws RepositoryException {
        Node node1 = targetNode.addNode(fileName, fileType);
        node1.addMixin("mix:referenceable");
        byte[] content = "Hello World".getBytes();
        ByteArrayInputStream bin = new ByteArrayInputStream(content);
        bin.reset();

        Node contentNode = node1.addNode("jcr:content", "nt:resource");
        contentNode.addMixin("mix:referenceable");
        Binary binary = session.getValueFactory().createBinary(bin);
        contentNode.setProperty("jcr:data", binary);
        contentNode.setProperty("jcr:lastModified", Calendar.getInstance());
        return node1;
    }

    public String generateFileName(String prefix) {
        return prefix + Long.toString((new Date()).getTime());
    }


    @Test
    public void testShouldDeleteTree() throws Exception {
        Node root = getSession().getNode("/cmis");

        Node folderL1 = root.addNode("folderL1", "nt:folder");
        addFile(folderL1, generateFileName("testFile_cp_"), "nt:file");

        Node folderL2 = folderL1.addNode("folderL1", "nt:folder");
        addFile(folderL2, generateFileName("testFile_cp_"), "nt:file");

        Node folderL3 = folderL2.addNode("folderL1", "nt:folder");
        addFile(folderL3, generateFileName("testFile_cp_"), "nt:file");

        getSession().save();
        
        folderL1.remove();
        getSession().save();

        NodeIterator nodes = getSession().getNode("/cmis").getNodes();
        while (nodes.hasNext()) {
            pt("child:", nodes.nextNode().getName());
        }
    }

//    @Test
    public void testShouldResetMultiValued() throws Exception {
        Node root = getSession().getNode("/cmis");

        Node node1 = addFile(root, generateFileName("testMultivf_"), "emailDocument");
        node1.setProperty("to", new String[]{"me", "he"});
        getSession().save();

        Node node = getSession().getNode(node1.getPath());
        node.setProperty("to", new String[]{});
        getSession().save();

        node = getSession().getNode(node1.getPath());
        assertFalse(node.hasProperty("to"));
//        System.out.println(node.getProperty("to").getString());
    }


//    @Test
//    @Test
    public void testShouldUploadLargeFile() throws Exception {
        Node root = getSession().getNode("/cmis");

        Node node1 = root.addNode(generateFileName("testMultivf_"), "emailDocument");
        node1.addMixin("mix:referenceable");
//        byte[] content = "Hello World".getBytes();
//        ByteArrayInputStream bin = new ByteArrayInputStream(content);
//        bin.reset();

        Node contentNode = node1.addNode("jcr:content", "nt:resource");
        contentNode.addMixin("mix:referenceable");
        File file = new File("c:\\content.exe");
        System.out.println("File size: " + file.length());
        FileInputStream fileInputStream = new FileInputStream(file);
        BufferedInputStream stream = new BufferedInputStream(fileInputStream);
        Binary binary = session.getValueFactory().createBinary(stream);
        contentNode.setProperty("jcr:data", binary);
        contentNode.setProperty("jcr:lastModified", Calendar.getInstance());


//        node1.setProperty("to", new String[]{"me", "he"});
        getSession().save();

        Node node = getSession().getNode(contentNode.getPath());
//        node.setProperty("to", new String[]{});
//        getSession().save();

//        node = getSession().getNode(node1.getPath());
//        assertFalse(node.hasProperty("to"));

        System.out.println(node.toString());

        System.out.println("length: " + node.getProperty("jcr:data").getBinary().getSize());
    }
    
    @Test
    public void shouldBeAbleToMoveExternalNodes() throws Exception {
        assertNotNull(session.getNode("/cmis/My_Folder-0-0/My_Document-1-0"));

        ((Workspace) session.getWorkspace()).move("/cmis/My_Folder-0-0/My_Document-1-0", "/cmis/My_Folder-0-1/My_Document-1-X");

        Node file = session.getNode("/cmis/My_Folder-0-1/My_Document-1-X");
        assertNotNull(file);
        assertNotNull(session.getNode("/cmis/My_Folder-0-0"));
        ((Workspace) session.getWorkspace()).move("/cmis/My_Folder-0-0", "/cmis/My_Folder-0-X");
        Node folder = session.getNode("/cmis/My_Folder-0-X");
        assertNotNull(folder);
        assertEquals("nt:folder", folder.getPrimaryNodeType().getName());
        ((Workspace) session.getWorkspace()).move("/cmis/My_Folder-0-1/My_Document-1-X", "/cmis/My_Folder-0-X/My_Document-1-0");
        ((Workspace) session.getWorkspace()).move("/cmis/My_Folder-0-X", "/cmis/My_Folder-0-0");

    }

//	@Test
//	public void test() throws Exception
//	{
//		Node base = session.getNode("/cmis");
//
//		Node folder1 = base.addNode("Folder1", "nt:folder");
//		Node folder2 = base.addNode("Folder2", "nt:folder");
//
//		Node document = folder1.addNode("Document", "nt:file");
//		Node content = document.addNode("jcr:content", "nt:resource");
//		content.setProperty("jcr:data", "hello");
//
//		session.save();
//
//		((Workspace) session.getWorkspace()).move("/cmis/Folder1/Document", "/cmis/Folder2/Document");
//	}

    @Test
    public void shouldCreateEmptyDocument() throws Exception {
        Node root = getSession().getNode("/cmis");
        String fileName = "testFile_cp_emptyFile";
        System.out.println("creating " + fileName);
        Node targetNode = root.addNode("folderWithEmptyFile", "nt:folder");
        Node node1 = targetNode.addNode(fileName, "nt:file");
        node1.addMixin("mix:referenceable");

//        byte[] content = "Hello World".getBytes();
//        ByteArrayInputStream bin = new ByteArrayInputStream(content);
//        bin.reset();

        Node contentNode = node1.addNode("jcr:content", "nt:resource");
//        contentNode.addMixin("mix:referenceable");
//        Binary binary = session.getValueFactory().createBinary(bin);
//        contentNode.setProperty("jcr:data", binary);
//        contentNode.setProperty("jcr:lastModified", Calendar.getInstance());
        System.out.println("empty created");
    }

}
