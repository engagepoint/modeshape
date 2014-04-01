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
import org.modeshape.common.util.FileUtil;
import org.modeshape.jcr.AbstractTransactionalTest;
import org.modeshape.jcr.JcrRepository;
import org.modeshape.jcr.JcrSession;
import org.modeshape.jcr.ModeShapeEngine;
import org.modeshape.jcr.ModeShapeEngine.State;
import org.modeshape.jcr.RepositoryConfiguration;

import javax.jcr.Node;
import javax.jcr.Session;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

public class ConcurrentNodesCreationTest extends AbstractTransactionalTest {


    private RepositoryConfiguration config;
    private ModeShapeEngine engine;
    private JcrRepository repository;

    @Before
    public void beforeEach() throws Exception {
        FileUtil.delete("target/concurrent_load_non_clustered");
        config = RepositoryConfiguration.read("broken_folder/concurrent-load-repo-config.json");
        engine = new ModeShapeEngine();
        startEngineAndDeployRepositoryAndLogIn();
    }


    @After
    public void afterEach() throws Exception {
        try {
            engine.shutdown(true).get();
        } finally {
            repository = null;
            engine = null;
            config = null;
        }

    }

    @Test
    public void shouldCreateFilesWithContent() throws Exception {
        final String baseFolderId = getBaseFolder("baseFolder").getIdentifier();
        long start = System.currentTimeMillis();
        System.out.println("Started ... ");
        final Phase firstPhase = new CreateEmptyFilesPhase(repository, baseFolderId);
        firstPhase.execute();
        long duration = TimeUnit.MILLISECONDS.toSeconds(
                System.currentTimeMillis() - start);
        System.out.println(
                String.format("Finished 1 phase: %d seconds",
                        duration));
        start = System.currentTimeMillis();

        final Phase secondPhase = new UploadContentPhase(repository, firstPhase.getTaskResults());
        secondPhase.execute();
        System.out.println(
                String.format("Finished 2 phase: %d seconds",
                        TimeUnit.MILLISECONDS.toSeconds(
                                System.currentTimeMillis() - start)));
        duration += TimeUnit.MILLISECONDS.toSeconds(
                System.currentTimeMillis() - start);
        System.out.println(
                String.format("Finished total: %d seconds",
                        duration));

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

    protected Node getBaseFolder(String name) throws Exception {
        System.out.println("Creating base folder");
        Session session = repository.login();
        Node baseFolder = session.getRootNode().addNode(name,  "acme:Domain");
        session.save();
        session.logout();
        return baseFolder;
    }


}
