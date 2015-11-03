/*
 * ModeShape (http://www.modeshape.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.modeshape.jcr;

import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.client.api.SessionFactory;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.modeshape.common.util.FileUtil;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author evgeniy.shevchenko
 * @version 1.0 11/3/2015
 */

public class IndexSyncWithExtRepoIT extends MultiUseAbstractTest {
    /**
     * Test OpenCMIS InMemory Server URL.
     * <p/>
     * This OpenCMIS InMemory server instance should be started by maven cargo plugin at pre integration stage.
     */
    private static final String CMIS_URL = "http://localhost:8090/";
    private static Logger logger = Logger.getLogger(IndexSyncWithExtRepoIT.class);
    private static final String PROJECTION = "/cmis";
    @BeforeClass
    public static void beforeAll() throws Exception {
        FileUtil.delete("target/federation_persistent_repository");
        RepositoryConfiguration config = RepositoryConfiguration.read("config/repository-test-index-sync.json");
        startRepository(config);

        // waiting when CMIS repository will be ready
        boolean isReady = false;

        // max time for waiting in milliseconds
        long maxTime = 30000L;

        // actially waiting time in milliseconds
        long waitingTime = 0L;

        // time quant in milliseconds
        long timeQuant = 500L;

        logger.info("Waiting for CMIS repository...");
        do {
            try {
                Session directSession = getDirectChemistrySession();
                assertTrue("Chemistry session should exists.", directSession != null);
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
        logger.info("CMIS repository has been started successfuly");
    }

    @AfterClass
    public static void afterAll() throws Exception {
        MultiUseAbstractTest.afterAll();
    }

    public static Session getDirectChemistrySession() {
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
        return factory.createSession(parameter);

    }

    private static String extRepoPath(String folderName){
        return "/" + folderName;
    }

    private static String jcrRepoPath(String folderName){
        return PROJECTION + "/" + folderName;
    }
    private Node createFolderInJcr(String folderName) throws RepositoryException {
        Node root = getSession().getNode(PROJECTION);
        Node node = root.addNode(folderName, "nt:folder");
        assertTrue(folderName.equals(node.getName()));
        getSession().save();
        return node;
    }

    private void updateNodeDirectlyInCmis(final String nodePath, final String newName) {
        Session directSession = getDirectChemistrySession();
        CmisObject node = directSession.getObjectByPath(nodePath);
        Map<String, String> props = new HashMap<>();
        props.put("cmis:name", newName);
        node.updateProperties(props);
    }

    @Test
    public void shouldFindNodesUpdatedDirectlyInExtRepo() throws Exception {
        final String queryTemplate = "select * from [nt:folder] where [nt:folder].[jcr:name] = '%s'";
        String folderName = "test" + System.currentTimeMillis();
        Node jcrFolder = createFolderInJcr(folderName);
        assertNotNull(jcrFolder);
        assertNodesAreFound(String.format(queryTemplate, folderName), Query.JCR_SQL2,jcrRepoPath(folderName));
        final String newFolderName = folderName + System.currentTimeMillis();
        updateNodeDirectlyInCmis(extRepoPath(folderName), newFolderName);

        assertNodesAreFound(String.format(queryTemplate, newFolderName),Query.JCR_SQL2,jcrRepoPath(newFolderName));
    }




}
