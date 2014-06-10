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

import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.Folder;
import org.apache.chemistry.opencmis.client.api.ItemIterable;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.client.api.SessionFactory;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.modeshape.common.util.FileUtil;
import org.modeshape.jcr.JcrRepository;
import org.modeshape.jcr.MultiUseAbstractTest;
import org.modeshape.jcr.TestingUtil;
import org.modeshape.jcr.api.Workspace;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static junit.framework.Assert.assertNotNull;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Provide integration testing of the CMIS connector with OpenCMIS InMemory Repository.
 * 
 * @author Evgeniy Shevchenko
 * @version 1.0 6/10/2014
 */

public class ClusteredCmisConnectorIT {
    private static final String CMIS_URL = "http://localhost:8090/";
    private static final String QUERYABLE = "/projection-queryable";
    private static final String NON_QUERYABLE = "/projection-non-queryable";

    private JcrRepository repository1;
    private JcrRepository repository2;

    @Before
    public void before() throws Exception {
        FileUtil.delete("target/clustered");
        // Start the first process completely ...
        repository1 = TestingUtil.startRepositoryWithConfig("config/repo-config-clustered-persistent-1.json");
        // Start the second process completely ...
        repository2 = TestingUtil.startRepositoryWithConfig("config/repo-config-clustered-persistent-2.json");
    }

    @After
    public void after() throws Exception {
        TestingUtil.killRepositories(repository1, repository2);
    }

    public static Session getExternalSourceSession() {
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

    public static void initFolderForProjection(Session session, String name) {
        Folder folder = null;
        try{
            folder = (Folder)session.getObjectByPath("/" + name);
            ItemIterable<CmisObject> children = folder.getChildren();
            Iterator<CmisObject> iterator = children.iterator();
            while (iterator.hasNext()) {
                CmisObject item = iterator.next();
                if (item instanceof Folder) {
                    ((Folder)item).deleteTree(true, null, false);
                } else {
                    item.delete();
                }
            }

        } catch (CmisObjectNotFoundException e) {
            Map<String, Object> properties= new HashMap<String, Object>();
            properties.put(PropertyIds.NAME, name);
            properties.put(PropertyIds.OBJECT_TYPE_ID, "cmis:folder");
            folder = session.getRootFolder().createFolder(properties);
        }
        Assert.assertFalse(folder.getChildren().getHasMoreItems());
    }
    @BeforeClass
    public static void beforeAll() throws Exception {
        Session cmisSession = getExternalSourceSession();
        initFolderForProjection(cmisSession, QUERYABLE.substring(1));
        initFolderForProjection(cmisSession, NON_QUERYABLE.substring(1));
    }



    @Test
    public void shouldNotIndexNotQueryableConnector()
            throws Exception {

        javax.jcr.Session session1 = null;
        javax.jcr.Session session2 = null;
        try {
            session1 = repository1.login();
            assertThat(session1.getRootNode(), is(notNullValue()));
            session2 = repository2.login();
            assertThat(session2.getRootNode(), is(notNullValue()));

            createFolder(session1, QUERYABLE, "samplefolder");
            createFolder(session1, NON_QUERYABLE, "samplefolder");
            createFile(session1, QUERYABLE, "samplefile");
            createFile(session1, NON_QUERYABLE, "samplefile");
            session1.save();
            Workspace workspace1 = (Workspace) session1.getWorkspace();
            Workspace workspace2 = (Workspace) session2.getWorkspace();

            workspace1.reindex();
            workspace2.reindex();

            assertNotNull(session1.getNode(QUERYABLE + "/samplefolder"));
            assertNotNull(session2.getNode(QUERYABLE + "/samplefolder"));
            assertNotNull(session1.getNode(QUERYABLE + "/samplefile"));
            assertNotNull(session2.getNode(QUERYABLE + "/samplefile"));

            assertNotNull(session1.getNode(NON_QUERYABLE + "/samplefolder"));
            assertNotNull(session2.getNode(NON_QUERYABLE + "/samplefolder"));
            assertNotNull(session1.getNode(NON_QUERYABLE + "/samplefile"));
            assertNotNull(session2.getNode(NON_QUERYABLE + "/samplefile"));

            String query = "select * FROM [nt:base] where [jcr:path] like '%s'";
            //expected result is equal to 3 because we created 3 nodes: file, content and folder
            queryAndExpectResults(session1, String.format(query,QUERYABLE +"/%") , 3);
            queryAndExpectResults(session2, String.format(query,QUERYABLE+"/%") , 3);

            queryAndExpectResults(session1, String.format(query,NON_QUERYABLE+"/%") , 0);
            queryAndExpectResults(session2, String.format(query,NON_QUERYABLE+"/%") , 0);


        } finally {
            if (session1 != null) {
                session1.logout();
            }
            if (session2 != null) {
                session2.logout();
            }
        }
    }

    private void queryAndExpectResults(javax.jcr.Session session, String queryString, int howMany) throws RepositoryException{
        javax.jcr.query.QueryManager queryManager = session.getWorkspace().getQueryManager();
        Query query = queryManager.createQuery(queryString, Query.JCR_SQL2);
        NodeIterator nodes = query.execute().getNodes();
        assertEquals(howMany, nodes.getSize());
    }

    private void createFolder(javax.jcr.Session session, String parentPath, String folderName)
            throws RepositoryException {
        session.getNode(parentPath).addNode(folderName,"nt:folder");
    }

    private void createFile(javax.jcr.Session session, String parentPath, String nodeName)
            throws RepositoryException, FileNotFoundException {
        Node parent = session.getNode(parentPath);
        Node file = parent.addNode(nodeName, "nt:file");
        Node resource1 = file.addNode("jcr:content", "nt:resource");
        resource1.setProperty(
                "jcr:data",
                session.getValueFactory()
                        .createBinary(new FileInputStream("target/test-classes/types_extended.xml")));

    }
}
