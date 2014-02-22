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

import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.client.api.SessionFactory;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.modeshape.jcr.MultiUseAbstractTest;
import org.modeshape.jcr.RepositoryConfiguration;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import java.io.ByteArrayInputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Provide integration testing of the CMIS connector with OpenCMIS InMemory Repository.
 *
 * @author Alexander Voloshyn
 * @author Nick Knysh
 * @version 1.0 2/20/2013
 */
@Ignore
public class LoadCreateUnfiledIT extends MultiUseAbstractTest {
    /**
     * Test OpenCMIS InMemory Server URL.
     * <p/>
     * This OpenCMIS InMemory server instance should be started by maven cargo plugin at pre integration stage.
     */
    private static final String CMIS_URL = "http://localhost:8090/";
    public static final String DEFAULT_BINARY_CONTENT = "Hello World";
    private static Logger logger = Logger.getLogger(LoadCreateUnfiledIT.class);
    private static Session cmisDirectSession;

    @BeforeClass
    public static void beforeAll() throws Exception {
        RepositoryConfiguration config = RepositoryConfiguration.read("config/repository-1-paged.json");
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
        parameter.put(SessionParameter.WEBSERVICES_OBJECT_SERVICE, CMIS_URL + "services/ObjectService?wsdl");
        parameter.put(SessionParameter.WEBSERVICES_POLICY_SERVICE, CMIS_URL + "services/PolicyService?wsdl");
        parameter.put(SessionParameter.WEBSERVICES_RELATIONSHIP_SERVICE, CMIS_URL + "services/RelationshipService?wsdl");
        parameter.put(SessionParameter.WEBSERVICES_REPOSITORY_SERVICE, CMIS_URL + "services/RepositoryService?wsdl");
        parameter.put(SessionParameter.WEBSERVICES_VERSIONING_SERVICE, CMIS_URL + "services/VersioningService?wsdl");
        // Default repository id for in memory server is A1
        parameter.put(SessionParameter.REPOSITORY_ID, "A1");

        // create session
        final Session session = factory.createSession(parameter);
        assertTrue("Chemistry session should exists.", session != null);
        return session;
    }


    @Test
    public void shouldCreateFolderAndDocument() throws Exception {
        Node root = getSession().getNode("/cmis");
        NodeIterator nodes = root.getNodes();
        while (nodes.hasNext()) {
            Node node = nodes.nextNode();

            System.out.println(node.getIdentifier() + " / " + node.getName() + " / " + node.getPrimaryNodeType().toString());
        }
        String name = "loadTstCreate-paged" + System.currentTimeMillis();
        Node node = root.addNode(name, "nt:folder");
        getSession().save();
        assertTrue(name.equals(node.getName()));
        // node.setProperty("name", "test-name");
        root = getSession().getNodeByIdentifier("jcr:unfiled");


        for (int i = 0; i < 500; i++) {
            long start = new Date().getTime();
            Node node1 = root.addNode("test-1_" + new Date().getTime(), "MyDocType2.8_remapped");
            // System.out.println("Test: creating binary content");
            byte[] content = "Hello World".getBytes();
            ByteArrayInputStream bin = new ByteArrayInputStream(content);
            bin.reset();

            // System.out.println("Test: creating content node");
            Node contentNode = node1.addNode("jcr:content", "nt:resource");
            Binary binary = session.getValueFactory().createBinary(bin);
            contentNode.setProperty("jcr:data", binary);
            contentNode.setProperty("jcr:lastModified", Calendar.getInstance());

            getSession().save();
//            System.out.println(node1.getIdentifier() + " << node identity");
            Node nodeByIdentifier = getSession().getNodeByIdentifier("9fe71cdeaa9871" + Integer.toString(i + 137));
            System.out.println(nodeByIdentifier.getName() + " << found prevNode");
            System.out.println(" <|| load paged: creation time: " + ((new Date().getTime()) - start) + " |" + (i + 1) + "|> ");
        }
//        System.out.println("Test: checking result");
        root.remove();

    }


    //    @Test
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
