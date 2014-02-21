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

/**
 * Provide integration testing of the CMIS connector with OpenCMIS InMemory Repository.
 * 
 * @author Alexander Voloshyn
 * @author Nick Knysh
 * @version 1.0 2/20/2013
 */
@Ignore
public class CmisConnectorSingleVesionIT extends MultiUseAbstractTest {
    /**
     * Test OpenCMIS InMemory Server URL.
     * <p/>
     * This OpenCMIS InMemory server instance should be started by maven cargo plugin at pre integration stage.
     */
    private static final String CMIS_URL = "http://localhost:8090/";
    public static final String DEFAULT_BINARY_CONTENT = "Hello World";
    public static final String DEFAULT_BINARY_CONTENT_2 = "Hello World LOL";
    private static Logger logger = Logger.getLogger(CmisConnectorSingleVesionIT.class);
    private static Session cmisDirectSession;

    private static final String commonIdPropertyName = "custom:commonId";
    private static final String commonIdTypeName = "custom:singleVersionBaseType";
    private static final String commonIdQueryTemplate = "SELECT * FROM %1$s where '%3$s' =  ANY %2$s";

    @BeforeClass
    public static void beforeAll() throws Exception {
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


    CmisObject getLatestVersion(CmisObject object) {
        List<Document> allVersions = ((Document) object).getAllVersions();
        Document document = allVersions.get(0);
        return cmisDirectSession.getObject(document.getId());
    }


    @Test
    public void shouldCreateCompleteDocumentAsSingleVersion() throws Exception {
        Node root = getSession().getNode("/cmis");

        Node fileNode = root.addNode("test-sv-1" + new Date().getTime(), "custom:singleVersionTypeOne");
        fileNode.setProperty("custom:artistOne", "artist");

        byte[] content = DEFAULT_BINARY_CONTENT.getBytes();
        ByteArrayInputStream bin = new ByteArrayInputStream(content);
        bin.reset();

        Node contentNode = fileNode.addNode("jcr:content", "nt:resource");
        Binary binary = session.getValueFactory().createBinary(bin);
        contentNode.setProperty("jcr:data", binary);
        contentNode.setProperty("jcr:lastModified", Calendar.getInstance());

        getSession().save();

        Node fileNodeAfter = session().getNodeByIdentifier(fileNode.getIdentifier());
        Assert.assertNotNull(fileNodeAfter);

        String identifier = fileNode.getIdentifier();
        identifier = getExternalId(identifier); // remove connector's key prefix
        String query = String.format(commonIdQueryTemplate, commonIdTypeName, commonIdPropertyName, identifier);
        System.out.println("executing query from test: " + query);
        ItemIterable<QueryResult> queryResults = cmisDirectSession.query(query, false);
        Assert.assertEquals(1, queryResults.getTotalNumItems());

        QueryResult first = queryResults.iterator().next();
        CmisObject directObject = cmisDirectSession.getObject(first.getPropertyById(PropertyIds.OBJECT_ID).getFirstValue().toString());
        Assert.assertNotNull(directObject);

        System.out.println("directObject: " + directObject);
        Assert.assertEquals("artist", directObject.getPropertyValue("custom:artistOne").toString());

        Assert.assertEquals("artist", directObject.getPropertyValue("custom:artistOne").toString());
        Assert.assertEquals(directObject.getPropertyValue("custom:artistOne").toString()
                , fileNode.getProperty("custom:artistOne").getValue().toString());
        // content length
        Assert.assertEquals(11, ((Document) directObject).getContentStreamLength());
        Assert.assertEquals(11, fileNode.getNode("jcr:content").getProperty("jcr:data").getBinary().getSize());

        List<Document> allVersions = ((Document) directObject).getAllVersions();
        Assert.assertEquals(1, allVersions.size());
    }

    private String getExternalId(String identifier) {
        return  identifier.indexOf("FAKE") > 0
                ? identifier.substring(identifier.indexOf("FAKE"))
                : identifier.substring(14); //drop connector key - 9fe71cdeaa9871
    }

    @Test
    public void shouldCreateCompleteMappedDocumentAsSingleVersion() throws Exception {
        Node root = getSession().getNode("/cmis");

        Node fileNode = root.addNode("test-sv-2" + new Date().getTime(), "custom:singleVersionTypeTwoMapped");
        fileNode.setProperty("custom:artistTwoMapped", "artist");
        byte[] content = DEFAULT_BINARY_CONTENT.getBytes();
        ByteArrayInputStream bin = new ByteArrayInputStream(content);
        bin.reset();

        Node contentNode = fileNode.addNode("jcr:content", "nt:resource");
        Binary binary = session.getValueFactory().createBinary(bin);
        contentNode.setProperty("jcr:data", binary);
        contentNode.setProperty("jcr:lastModified", Calendar.getInstance());

        getSession().save();

        Node fileNodeAfter = session().getNodeByIdentifier(fileNode.getIdentifier());
        Assert.assertNotNull(fileNodeAfter);

        String identifier = fileNode.getIdentifier();
        identifier = getExternalId(identifier);
        String query = String.format(commonIdQueryTemplate, commonIdTypeName, commonIdPropertyName, identifier);
        System.out.println("executing query from test: " + query);
        ItemIterable<QueryResult> queryResults = cmisDirectSession.query(query, false);
        Assert.assertEquals(1, queryResults.getTotalNumItems());

        QueryResult first = queryResults.iterator().next();
        CmisObject directObject = cmisDirectSession.getObject(first.getPropertyById(PropertyIds.OBJECT_ID).getFirstValue().toString());
        Assert.assertNotNull(directObject);
         // meta-data
        System.out.println("directObject: " + directObject.getId() + " / " + directObject.getProperty(commonIdPropertyName));
        Assert.assertEquals("artist", directObject.getPropertyValue("custom:artistTwo").toString());
        Assert.assertEquals(directObject.getPropertyValue("custom:artistTwo").toString()
                , fileNode.getProperty("custom:artistTwoMapped").getValue().toString());
        // content length
        Assert.assertEquals(11, ((Document) directObject).getContentStreamLength());
        Assert.assertEquals(11, fileNode.getNode("jcr:content").getProperty("jcr:data").getBinary().getSize());
        // versions
        List<Document> allVersions = ((Document) directObject).getAllVersions();
        Assert.assertEquals(1, allVersions.size());
    }


//    @Test // has problems with chemcistry-inmemory , coz of it's logic for get by VersionSeries
    public void shouldUpdateMappedDocumentAsSingleVersion() throws Exception {
        Node root = getSession().getNode("/cmis");

        Node fileNode = root.addNode("test-sv-2" + new Date().getTime(), "custom:singleVersionTypeTwoMapped");
        fileNode.setProperty("custom:artistTwoMapped", "artist");
        byte[] content = DEFAULT_BINARY_CONTENT.getBytes();
        ByteArrayInputStream bin = new ByteArrayInputStream(content);
        bin.reset();

        Node contentNode = fileNode.addNode("jcr:content", "nt:resource");
        Binary binary = session.getValueFactory().createBinary(bin);
        contentNode.setProperty("jcr:data", binary);
        contentNode.setProperty("jcr:lastModified", Calendar.getInstance());

        getSession().save();

        Node fileNodeAfter = session().getNodeByIdentifier(fileNode.getIdentifier());
        Assert.assertNotNull(fileNodeAfter);

        String identifier = fileNode.getIdentifier();
        identifier = getExternalId(identifier);
        String query = String.format(commonIdQueryTemplate, commonIdTypeName, commonIdPropertyName, identifier);
        System.out.println("executing query from test: " + query);
        ItemIterable<QueryResult> queryResults = cmisDirectSession.query(query, false);
        Assert.assertEquals(1, queryResults.getTotalNumItems());

        QueryResult first = queryResults.iterator().next();
        CmisObject directObject = cmisDirectSession.getObject(first.getPropertyById(PropertyIds.OBJECT_ID).getFirstValue().toString());
        String versionSeries = directObject.getProperty(PropertyIds.VERSION_SERIES_ID).getValueAsString();
        Assert.assertNotNull(directObject);
        // meta-data
        System.out.println("directObject: " + directObject.getId() + " / " + directObject.getProperty(commonIdPropertyName));
        Assert.assertEquals("artist", directObject.getPropertyValue("custom:artistTwo").toString());
        Assert.assertEquals(directObject.getPropertyValue("custom:artistTwo").toString()
                , fileNode.getProperty("custom:artistTwoMapped").getValue().toString());
        // content length
        Assert.assertEquals(11, ((Document) directObject).getContentStreamLength());
        Assert.assertEquals(11, fileNode.getNode("jcr:content").getProperty("jcr:data").getBinary().getSize());
        // versions
        List<Document> allVersions = ((Document) directObject).getAllVersions();
        Assert.assertEquals(1, allVersions.size());

        // update meta-data
        fileNode.setProperty("custom:artistTwoMapped", "artistNew");
        session().save();

        CmisObject objectV2 = getLatestVersion(directObject);
        Assert.assertEquals("artistNew", objectV2.getPropertyValue("custom:artistTwo").toString());
        Assert.assertEquals(objectV2.getPropertyValue("custom:artistTwo").toString()
                , ((Node) session().getNodeByIdentifier(fileNode.getIdentifier())).getProperty("custom:artistTwoMapped").getValue().toString());

        // update content
        byte[] content2 = DEFAULT_BINARY_CONTENT_2.getBytes();
        ByteArrayInputStream bin2 = new ByteArrayInputStream(content2);
        bin2.reset();

        Binary binary2 = session.getValueFactory().createBinary(bin2);
        contentNode.setProperty("jcr:data", binary2);
        contentNode.setProperty("jcr:lastModified", Calendar.getInstance());

        session().save();

        // content length
        CmisObject version3 = getLatestVersion(directObject);
        Assert.assertEquals(15, ((Document) version3).getContentStreamLength());
        Assert.assertEquals(15, fileNode.getNode("jcr:content").getProperty("jcr:data").getBinary().getSize());

        // versions
        List<Document> allVersionsEnd = ((Document) cmisDirectSession.getObject(versionSeries)).getAllVersions();
        Assert.assertEquals(1, allVersionsEnd.size());
    }


    @Test
    public void shouldDeleteCreatedAsSingleVersionDcoument() throws Exception {
        Node root = getSession().getNode("/cmis");

        Node fileNode = root.addNode("test-sv-1" + new Date().getTime(), "custom:singleVersionTypeOne");
        fileNode.setProperty("custom:artistOne", "artist");

        byte[] content = DEFAULT_BINARY_CONTENT.getBytes();
        ByteArrayInputStream bin = new ByteArrayInputStream(content);
        bin.reset();

        Node contentNode = fileNode.addNode("jcr:content", "nt:resource");
        Binary binary = session.getValueFactory().createBinary(bin);
        contentNode.setProperty("jcr:data", binary);
        contentNode.setProperty("jcr:lastModified", Calendar.getInstance());

        getSession().save();

        Node fileNodeAfter = session().getNodeByIdentifier(fileNode.getIdentifier());
        Assert.assertNotNull(fileNodeAfter);

        String identifier = fileNode.getIdentifier();
        identifier = getExternalId(identifier); // remove connector's key prefix
        String query = String.format(commonIdQueryTemplate, commonIdTypeName, commonIdPropertyName, identifier);
        System.out.println("executing query from test: " + query);
        ItemIterable<QueryResult> queryResults = cmisDirectSession.query(query, false);
        Assert.assertEquals(1, queryResults.getTotalNumItems());

        QueryResult first = queryResults.iterator().next();
        CmisObject directObject = cmisDirectSession.getObject(first.getPropertyById(PropertyIds.OBJECT_ID).getFirstValue().toString());
        Assert.assertNotNull(directObject);

        System.out.println("directObject: " + directObject);
        Assert.assertEquals("artist", directObject.getPropertyValue("custom:artistOne").toString());

        Assert.assertEquals("artist", directObject.getPropertyValue("custom:artistOne").toString());
        Assert.assertEquals(directObject.getPropertyValue("custom:artistOne").toString()
                , fileNode.getProperty("custom:artistOne").getValue().toString());
        // content length
        Assert.assertEquals(11, ((Document) directObject).getContentStreamLength());
        Assert.assertEquals(11, fileNode.getNode("jcr:content").getProperty("jcr:data").getBinary().getSize());

        List<Document> allVersions = ((Document) directObject).getAllVersions();
        Assert.assertEquals(1, allVersions.size());

        fileNode.remove();
        session().save();

        ItemIterable<QueryResult> queryResults2 = cmisDirectSession.query(query, false);
        Assert.assertEquals(0, queryResults2.getTotalNumItems());
    }
}
