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
package org.modeshape.jcr;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Session;
import javax.jcr.nodetype.NodeType;
import javax.jcr.query.Query;
import javax.jcr.query.QueryResult;
import org.junit.Test;
import org.modeshape.common.FixFor;
import org.modeshape.common.util.FileUtil;
import org.modeshape.common.util.IoUtil;
import org.modeshape.jcr.api.JcrTools;
import org.modeshape.jcr.api.NamespaceRegistry;

/**
 * A test the verifies that a repository will persist content (including binaries).
 */
public class RepositoryPersistenceTest extends MultiPassAbstractTest {

    @Test
    public void shouldPersistBinariesAcrossRestart() throws Exception {
        String repositoryConfigFile = "config/repo-config-persistent-cache.json";
        File persistentFolder = new File("target/persistent_repository");
        // remove all persisted content ...
        FileUtil.delete(persistentFolder);
        assertDataPersistenceAcrossRestarts(repositoryConfigFile);
    }

    @FixFor( "MODE-2212" )
    @Test
    public void shouldPersistGeneratedNamespacesAcrossRestart() throws Exception {
        String repositoryConfigFile = "config/repo-config-persistent-cache.json";
        File persistentFolder = new File("target/persistent_repository");
        // remove all persisted content ...
        FileUtil.delete(persistentFolder);

        startRunStop(new RepositoryOperation() {

            @Override
            public Void call() throws Exception {
                Session session = repository.login();

                final NamespaceRegistry namespaceRegistry = (NamespaceRegistry)session.getWorkspace().getNamespaceRegistry();

                namespaceRegistry.registerNamespace("info:a#");
                namespaceRegistry.registerNamespace("info:b#");
                namespaceRegistry.registerNamespace("info:c#");
                assertEquals("ns001", namespaceRegistry.getPrefix("info:a#"));
                assertEquals("ns002", namespaceRegistry.getPrefix("info:b#"));
                assertEquals("ns003", namespaceRegistry.getPrefix("info:c#"));

                final Node node = session.getRootNode().addNode("ns001:xyz", NodeType.NT_UNSTRUCTURED);
                node.setProperty("ns002:abc", "abc");
                node.setProperty("ns003:def", "def");

                session.save();
                session.logout();
                return null;
            }
        }, repositoryConfigFile);

        startRunStop(new RepositoryOperation() {

            @Override
            public Void call() throws Exception {
                Session session = repository.login();

                final NamespaceRegistry namespaceRegistry = (NamespaceRegistry)session.getWorkspace().getNamespaceRegistry();

                assertEquals("ns001", namespaceRegistry.getPrefix("info:a#"));
                assertEquals("ns002", namespaceRegistry.getPrefix("info:b#"));
                assertEquals("ns003", namespaceRegistry.getPrefix("info:c#"));
                session.save();
                session.logout();
                return null;
            }
        }, repositoryConfigFile);

    }

    private void assertDataPersistenceAcrossRestarts( String repositoryConfigFile ) throws Exception {
        final List<File> testFiles = new ArrayList<File>();
        final Map<String, Long> testFileSizesInBytes = new HashMap<String, Long>();
        testFiles.add(getFile("mimetype/test.xml"));
        testFiles.add(getFile("mimetype/modeshape.doc"));
        testFiles.add(getFile("mimetype/log4j.properties"));
        for (File testFile : testFiles) {
            assertThat(testFile.getPath() + " should exist", testFile.exists(), is(true));
            assertThat(testFile.getPath() + " should be a file", testFile.isFile(), is(true));
            assertThat(testFile.getPath() + " should be readable", testFile.canRead(), is(true));
            testFileSizesInBytes.put(testFile.getName(), testFile.length());
        }

        final JcrTools tools = new JcrTools();

        startRunStop(new RepositoryOperation() {
            @Override
            public Void call() throws Exception {
                Session session = repository.login();

                // Add some content ...
                session.getRootNode().addNode("testNode");
                for (File testFile : testFiles) {
                    String name = testFile.getName();
                    Node fileNode = tools.uploadFile(session, "/testNode/" + name, testFile);
                    Binary binary = fileNode.getNode("jcr:content").getProperty("jcr:data").getBinary();
                    assertThat(binary.getSize(), is(testFileSizesInBytes.get(name)));
                }

                session.save();

                Node testNode = session.getNode("/testNode");
                for (File testFile : testFiles) {
                    String name = testFile.getName();
                    Node fileNode = testNode.getNode(name);
                    assertThat(fileNode, is(notNullValue()));
                    Binary binary = fileNode.getNode("jcr:content").getProperty("jcr:data").getBinary();
                    byte[] expectedBytes = IoUtil.readBytes(testFile);
                    byte[] actualBytes = IoUtil.readBytes(binary.getStream());
                    assertArrayEquals(expectedBytes, actualBytes);
                }

                Query query = session.getWorkspace().getQueryManager().createQuery("SELECT * FROM [nt:file]", Query.JCR_SQL2);
                QueryResult results = query.execute();
                NodeIterator iter = results.getNodes();
                while (iter.hasNext()) {
                    Node fileNode = iter.nextNode();
                    assertThat(fileNode, is(notNullValue()));
                    String name = fileNode.getName();
                    Binary binary = fileNode.getNode("jcr:content").getProperty("jcr:data").getBinary();
                    assertThat(binary.getSize(), is(testFileSizesInBytes.get(name)));
                }

                session.logout();
                return null;
            }
        }, repositoryConfigFile);

        startRunStop(new RepositoryOperation() {
            @Override
            public Void call() throws Exception {

                Session session = repository.login();
                assertNotNull(session.getNode("/testNode"));

                for (File testFile : testFiles) {
                    String name = testFile.getName();
                    Node fileNode = session.getNode("/testNode/" + name);
                    assertNotNull(fileNode);
                    Binary binary = fileNode.getNode("jcr:content").getProperty("jcr:data").getBinary();
                    assertThat(binary.getSize(), is(testFileSizesInBytes.get(name)));
                }

                Query query = session.getWorkspace().getQueryManager().createQuery("SELECT * FROM [nt:file]", Query.JCR_SQL2);
                QueryResult results = query.execute();
                NodeIterator iter = results.getNodes();
                while (iter.hasNext()) {
                    Node fileNode = iter.nextNode();
                    String name = fileNode.getName();
                    Binary binary = fileNode.getNode("jcr:content").getProperty("jcr:data").getBinary();
                    assertThat(binary.getSize(), is(testFileSizesInBytes.get(name)));
                }

                session.logout();

                return null;
            }
        }, repositoryConfigFile);
    }

    @Test
    public void shouldPersistDataUsingDB() throws Exception {
        // make sure the DB is clean (empty) when running this test; there is no effective teardown
        assertDataPersistenceAcrossRestarts("config/db/repo-config-jdbc.json");
    }

    protected File getFile( String resourcePath ) throws URISyntaxException {
        URL resource = getClass().getClassLoader().getResource(resourcePath);
        assertNotNull(resourcePath + " not found", resource);
        return new File(resource.toURI());
    }
}
