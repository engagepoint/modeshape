package org.modeshape.jcr;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.modeshape.common.util.FileUtil;
import org.modeshape.connector.mock.MockClusteredConnectorWithCounters;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryResult;

import static org.junit.Assert.assertEquals;

/**
 * Created by vyacheslav.polulyakh on 10/20/2014.
 */
public class ClusteredFederationConnectorTest {
    public static final String IPV4 = "java.net.preferIPv4Stack";
    public static final String XPATH_QUERY = "//element(*,nt:file)";;

    @BeforeClass
    public static void beforeClass() throws Exception {
        //System.setProperty(IPV4, "true");
        ClusteringHelper.bindJGroupsToLocalAddress();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        //System.clearProperty(IPV4);
        ClusteringHelper.removeJGroupsBindings();
    }

    @Test
    public void shouldGetDocumentInBothNodes() throws Exception {
        JcrRepository repository1 = null;
        JcrRepository repository2 = null;
        try {
            repository1 = TestingUtil.startRepositoryWithConfig("config/repo-config-cluster-mock-federation-persistent-1.json");
            JcrSession session1 = repository1.login();

            repository2 = TestingUtil.startRepositoryWithConfig("config/repo-config-cluster-mock-federation-persistent-2.json");
            JcrSession session2 = repository2.login();

            AbstractJcrNode connectorParent = session1.getNode("/projection1");
            String id = createDocument(connectorParent, session1, "doc1_1");

            AbstractJcrNode parent = session1.getRootNode().addNode("folder1", "nt:folder");
            createDocument(parent, session1, "doc1_2");

            session1.save();

            //one node when creating a document takes it 5 times from connector
            Thread.sleep(1000);
            assertEquals(10, getCountById(id));

            //wait until the TTL in cache expires
            Thread.sleep(2000);
            session2.getNodeByIdentifier(id);
            assertEquals(11, getCountById(id));

            assertEquals(2, executeXpath(session1, XPATH_QUERY).getNodes().getSize());
            assertEquals(2, executeXpath(session2, XPATH_QUERY).getNodes().getSize());

        } finally {
            TestingUtil.killRepositories(repository1, repository2);
            FileUtil.delete("target/clustered");
        }
    }

    @Test
    public void shouldGetDocumentOnlyInOneNode() throws Exception {
        JcrRepository repository1 = null;
        JcrRepository repository2 = null;
        try {
            repository1 = TestingUtil.startRepositoryWithConfig("config/repo-config-cluster-mock-federation-non-query-persistent-1.json");
            JcrSession session1 = repository1.login();

            repository2 = TestingUtil.startRepositoryWithConfig("config/repo-config-cluster-mock-federation-non-query-persistent-2.json");
            JcrSession session2 = repository2.login();

            AbstractJcrNode connectorParent = session1.getNode("/projection1");
            String id = createDocument(connectorParent, session1, "doc1_1");

            AbstractJcrNode parent = session1.getRootNode().addNode("folder1", "nt:folder");
            createDocument(parent, session1, "doc1_2");

            session1.save();

            //one node when creating a document takes it 5 times from connector
            Thread.sleep(1000);
            assertEquals(9, getCountById(id));

            //wait until the TTL in cache expires
            Thread.sleep(2000);
            session2.getNodeByIdentifier(id);
            assertEquals(10, getCountById(id));

            assertEquals(2, executeXpath(session1, XPATH_QUERY).getNodes().getSize());
            assertEquals(1, executeXpath(session2, XPATH_QUERY).getNodes().getSize());
        } finally {
            TestingUtil.killRepositories(repository1, repository2);
            FileUtil.delete("target/clustered");
        }
    }

    private String createDocument(Node parent, JcrSession session, String name) throws RepositoryException {
        Node node = parent.addNode(name, "nt:file");
        Node content = node.addNode("jcr:content", "nt:resource");
        content.setProperty("jcr:data", session.getValueFactory().createBinary("Binary".getBytes()));
        return node.getIdentifier();
    }
    private QueryResult executeXpath(JcrSession session, String xpath) throws RepositoryException {
        org.modeshape.jcr.api.query.Query query = (org.modeshape.jcr.api.query.Query) session.getWorkspace()
                .getQueryManager()
                .createQuery(xpath, Query.XPATH);
        return query.execute();
    }

    private int getCountById(String id) {
        return MockClusteredConnectorWithCounters.getCounters().get(id.substring(14)).getDocumentByIdCounter();
    }
}
