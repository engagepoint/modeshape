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
package org.modeshape.jcr.brokenfolder;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.modeshape.jcr.AbstractTransactionalTest;
import org.modeshape.jcr.JcrRepository;
import org.modeshape.jcr.JcrSession;
import org.modeshape.jcr.ModeShapeEngine;
import org.modeshape.jcr.ModeShapeEngine.State;
import org.modeshape.jcr.RepositoryConfiguration;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

public class ReadBrokenFolderTest extends AbstractTransactionalTest {

    private RepositoryConfiguration config;
    private ModeShapeEngine engine;
    private JcrRepository repository;

    @Before
    public void beforeEach() throws Exception {
        config = RepositoryConfiguration.read("broken_folder/concurrent-load-repo-config.json");
        engine = new ModeShapeEngine();
        startEngineAndDeployRepositoryAndLogIn();
    }


    private void stopRepo() throws ExecutionException, InterruptedException {
        if (engine.getState() == State.RUNNING) {
            engine.shutdown(true).get();
        }
    }

    @After
    public void afterEach() throws Exception {
        try {
            stopRepo();
        } finally {
            repository = null;
            engine = null;
            config = null;
        }

    }


    @Test
    public void shouldReadBrokenFolder() throws Exception {

        Session session = null;
        try {
            session = repository.login();
            final Node baseFolder = session.getNode("/baseFolder");
            final NodeIterator nodeIterator = baseFolder.getNodes();


            int nodesWithBodyCount = 0;
            while (nodeIterator.hasNext()) {
                Node node = nodeIterator.nextNode();
                if (node.hasNode("jcr:content")) {
                    nodesWithBodyCount++;
                }
            }



            assertThat(
                    "Folder has appropriate child nodes count",
                    nodeIterator.getSize(),
                    is((long) Phase.TASKS_COUNT));
            assertThat(
                    "Folder has appropriate child nodes count with body",
                    nodesWithBodyCount,
                    is(Phase.TASKS_COUNT));
        } finally {
            if (session != null) {
                session.logout();
            }
        }


    }


    protected void startEngineAndDeployRepositoryAndLogIn() throws Exception {

        System.out.println("starting engine");
        engine.start();
        assertThat(engine.getState(), is(State.RUNNING));

        System.out.println("deploying repository");
        repository = engine.deploy(config);
        JcrSession session = repository.login();
        assertThat(session.getRootNode(), is(notNullValue()));
        session.logout();
    }


}
