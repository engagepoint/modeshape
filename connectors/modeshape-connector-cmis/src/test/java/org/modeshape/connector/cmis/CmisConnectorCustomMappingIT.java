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
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.modeshape.connector.cmis.config.TypeCustomMappingList;
import org.modeshape.jcr.Connectors;
import org.modeshape.jcr.MultiUseAbstractTest;
import org.modeshape.jcr.RepositoryConfiguration;
import org.modeshape.jcr.federation.FederatedDocumentStore;

import javax.jcr.Repository;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.Assert.assertTrue;

/**
 * Provide integration testing of the custom mapping for the CMIS connector with OpenCMIS InMemory Repository.
 *
 * 
 * @author Alexander Voloshyn
 * @author Nick Knysh
 * @version 1.0 2/20/2013
 */

@Ignore
public class CmisConnectorCustomMappingIT extends MultiUseAbstractTest {
    /**
     * Test OpenCMIS InMemory Server URL.
     * <p/>
     * This OpenCMIS InMemory server instance should be started by maven cargo plugin at pre integration stage.
     */
    private static final String CMIS_URL = "http://localhost:8090/";
    public static final String DEFAULT_BINARY_CONTENT = "Hello World";
    private static Logger logger = Logger.getLogger(CmisConnectorCustomMappingIT.class);
    private static Session cmisDirectSession;

    @BeforeClass
    public static void beforeAll() throws Exception {
        RepositoryConfiguration config = RepositoryConfiguration.read("config/repository-1.json");
        startRepository(config);

    }

    @AfterClass
    public static void afterAll() throws Exception {
        MultiUseAbstractTest.afterAll();
    }

       protected FederatedDocumentStore getFederation(Repository repository) {
        if (repository instanceof org.modeshape.jcr.JcrRepository) {

            try {
                Method methodDocumentStore = org.modeshape.jcr.JcrRepository.class.getDeclaredMethod("documentStore", new Class[0]);
                methodDocumentStore.setAccessible(true);
                Object invokeResult = methodDocumentStore.invoke((org.modeshape.jcr.JcrRepository) repository);
                if (invokeResult instanceof FederatedDocumentStore) {
                    return (FederatedDocumentStore) invokeResult;
                }
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException(e.getMessage(), e);
            } catch (InvocationTargetException e) {
                throw new IllegalStateException(e.getMessage(), e);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
        return null;
    }

    protected Connectors getConnectors(FederatedDocumentStore fdStore) {
        try {
            Field fldConnectors = FederatedDocumentStore.class.getDeclaredField("connectors");
            fldConnectors.setAccessible(true);
            Object oConn = fldConnectors.get(fdStore);

            return (Connectors) oConn;
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Test
    public void shouldLoadGlobalIgnoredExtProperies() throws Exception {
        FederatedDocumentStore store = getFederation(repository());
        CmisConnector cmisConnector = (CmisConnector)getConnectors(store).getConnectorForSourceName("cmis");
        TypeCustomMappingList customMapping = cmisConnector.getCustomMapping();

        Assert.assertNotNull(customMapping.getGlobalIgnoredExtProperties());

        Assert.assertTrue("Ignored properties contain title",
                customMapping.getGlobalIgnoredExtProperties().contains("title"));

        Assert.assertFalse("Registered properties does not contain title",
                cmisConnector.getRegisteredProperties().values().contains("title"));
        Assert.assertTrue("Registered properties does contain track",
                cmisConnector.getRegisteredProperties().values().contains("track"));
    }
}
